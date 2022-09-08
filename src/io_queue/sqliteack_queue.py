from hashlib import new
import os
import json
import math
import time
import pickle
import sqlite3
import random
from loguru import logger
import cachetools.func

# Modeled after persist-queue
# https://github.com/peter-wangxu/persist-queue


class AckStatus(object):
    inited = "0"
    ready = "1"
    unack = "2"  # Message is popped off by receiever has not ack'd
    acked = "5"  # Message is popped and receiver has ack'd; assumed done
    ack_failed = "9" # Reciever has marked message as failed
    ack_done = "17" # Reciever has marked explicitly message as done


class DummySerializer:
    def loads(self, x):
        return x

    def dumps(self, x):
        return x


dummy_serializer = DummySerializer()


class SQLiteAckQueue:
    columns = []
    _TABLE_NAME = "ack_unique_queue_default"
    _KEY_COLUMN = "_id"
    _SQL_CREATE_UNIQUE = (
        "CREATE TABLE IF NOT EXISTS {table_name} ("
        "{key_column} INTEGER PRIMARY KEY AUTOINCREMENT, "
        "timestamp FLOAT, status INTEGER, {unique_column} TEXT, UNIQUE ({unique_column}))"
    )
    _SQL_CREATE = (
        "CREATE TABLE IF NOT EXISTS {table_name} ("
        "{key_column} INTEGER PRIMARY KEY AUTOINCREMENT, "
        "timestamp FLOAT, status INTEGER)"
    )
    _SQL_SELECT = (
        "SELECT {key_column}, timestamp, status {table_columns} FROM {table_name} "
        "WHERE status < %s "
        "ORDER BY {key_column} ASC LIMIT {limit} OFFSET {offset}" % AckStatus.unack
    )
    _SQL_SELECT_ALL = (
        "SELECT {key_column}, timestamp, status {table_columns} FROM {table_name} "
        "ORDER BY {key_column} ASC LIMIT {limit} OFFSET {offset}"
    )
    _SQL_MARK_ACK_SELECT = """
        SELECT _id, data FROM {table_name}
        WHERE {key_column} IN ({indices})
        """
    _SQL_MARK_ACK_UPDATE = """
        UPDATE {table_name} SET status = {status} 
        WHERE {key_column} IN ({indices})
        RETURNING *
    """
    _SQL_UPDATE_SINGLE_ROW = """
        UPDATE {table_name} SET {column_name} = {column_value}
        WHERE {row_id_col} = {row_id_val}
        RETURNING *
    """
    _SQL_DELETE = """
        DELETE FROM {table_name}
        WHERE {key_column} IN ({indices})
    """
    _SQL_INSERT = (
        "INSERT OR IGNORE INTO {table_name} (timestamp, status, {table_columns})"
        " VALUES (?, %s, {table_values}) " % AckStatus.inited
    )
    _SQL_COUNT = "SELECT COUNT(*) FROM {table_name}"
    _SQL_FREE = "SELECT COUNT(*) FROM {table_name} WHERE status < %s" % AckStatus.unack
    _SQL_DONE = "SELECT COUNT(*) FROM {table_name} WHERE status > %s" % AckStatus.unack
    _SQL_TIMEOUT = """
        UPDATE {table_name}
        SET status = %s
        WHERE status = %s
        AND timestamp < {timeout}
    """ % (
        AckStatus.ready,
        AckStatus.unack,
    )
    _SQL_CREATE_COLUMN = "ALTER TABLE {table_name} ADD {column_name} {column_type}"
    _SQL_READ_COLUMNS = "PRAGMA table_info({table_name})"

    _con = None
    _last_count_update = -1
    last_timeout_application = 0
    serializer = json

    def __init__(
        self,
        path,
        unique_column=None,
        timeout=300,
        max_size=None,
        delete_on_ack=False,
        serializer=json,
        table_name=None,
    ):
        self.timeout = timeout
        self.path = path
        self.max_size = max_size
        self.delete_on_ack = delete_on_ack
        self.serializer = serializer
        if table_name:
            self._TABLE_NAME = table_name
        self.sql = self._SQL_CREATE_UNIQUE if unique_column else self._SQL_CREATE
        self.con.execute(
            self.sql.format(table_name=self._TABLE_NAME, key_column=self._KEY_COLUMN,
                            unique_column=unique_column)
        )
        self.columns = self.read_columns()
        if unique_column and unique_column not in self.columns:
            self.columns.append(unique_column)
        self.con.commit()

    @property
    def con(self):
        self.apply_timeout()
        if self._con is None:
            self._con = sqlite3.connect(self.path)
        return self._con

    def get(self):
        return self.gets(1)

    def gets(self, n, random_offset=False, ack=True, return_keys=False,
             read_all=False):
        offset = 0
        if random_offset:
            offset = random.randint(0, n * 100)
        # Select rows to update
        rows = self.select(n, offset, read_all=read_all)
        # Skip the id & timestamp  & status fields by only
        # reading from the 3rd field onward
        items = self._process_rows(rows)
        keys = [row[0] for row in rows]
        # Mark them as checked out
        if ack:
            self.updates(keys, AckStatus.unack)
        self.con.commit()
        if return_keys:
            return keys, items
        return items

    def _process_rows(self, rows):
        items = [{k: v for (k, v) in zip(self.columns, row[3:])} for row in rows]
        return items

    def select(self, n, offset=0, read_all=False):

        qwhere = self._SQL_SELECT_ALL if read_all else self._SQL_SELECT
        qwhere = qwhere.format(
            table_name=self._TABLE_NAME,
            key_column=self._KEY_COLUMN,
            table_columns = "," + ", ".join(self.columns) if len(self.columns) > 0 else "",
            limit=n,
            offset=offset,
        )
        cursor = self.con.execute(qwhere)
        rows = list(cursor.fetchall())
        return rows

    def put(self, item):
        return self.puts([item])

    def puts(self, items):
        assert len(items) > 0, "Must have more than a single item"
        if not all(isinstance(i, dict) for i in items):
            raise ValueError("Items must be dicts")
        if not all(len(i) > 0 for i in items):
            raise ValueError("Dicts cannot be empty")
        self.max_size_block()
        self.update_table_schema(items[0])
        items = self.reorder_to_match_table_schema(items)
        cols_str = ", ".join(self.columns)
        vals_str = ", ".join("?" for _ in self.columns)
        insert = self._SQL_INSERT.format(table_name=self._TABLE_NAME,
                                         table_columns=cols_str,
                                         table_values=vals_str )
        self.con.executemany(insert, items)
        self.con.commit()
    
    def update_table_schema(self, row):
        """ Update table schema """
        for k, v in row.items():
            if k not in self.columns:
                self.create_column(k, v)
    
    def create_column(self, name, value):
        if isinstance(value, str):
            v_type = "TEXT"
        elif isinstance(value, float):
            v_type = "REAL"
        elif isinstance(value, int):
            v_type = "INTEGER"
        elif isinstance(value, dict):
            raise ValueError("Cannot have nested dictionaries")
        else:
            v_type = "TEXT"
        query = self._SQL_CREATE_COLUMN.format(table_name=self._TABLE_NAME, column_name=name, column_type=v_type) 
        self.con.execute(query)
        self.columns.append(name)

    def reorder_to_match_table_schema(self, rows):
        new_rows = []
        for i, row in enumerate(rows):
            new_row = [time.time()]
            for column in self.columns:
                new_row.append(row.pop(column, None))
            assert len(row) == 0, f"Extra columns not present in table found in {i}th row"
            new_rows.append(new_row)
        return new_rows

    def read_columns(self):
        cursor = self.con.execute(self._SQL_READ_COLUMNS.format(table_name=self._TABLE_NAME))
        rows = cursor.fetchall()
        column_names = [row[1] for row in rows]
        column_names = [n for n in column_names if n not in ("_id", "timestamp", "status")]
        return column_names

    def max_size_block(self):
        """ Block the main thread until the count in the table
        decreases.
        """
        if self.max_size:
            i = 0
            while self.approx_count() > self.max_size:
                i += 1
                time.sleep(1)
                if int(math.log2(i)) == math.log2(i):
                    logger.info(f"Waited {i} sec so far for queue to deplete")
            if i > 1:
                logger.info(f"Finished waiting after {i} sec")

    def updates(self, keys, status=AckStatus.unack):
        indices = ",".join((str(r) for r in keys))
        qupdat = self._SQL_MARK_ACK_UPDATE.format(
            table_name=self._TABLE_NAME,
            key_column=self._KEY_COLUMN,
            status=status,
            indices=indices,
        )
        cursor = self.con.execute(qupdat)
        rows = cursor.fetchall()
        if len(rows) != len(keys):
            raise KeyError("Could not update all keys")
        self.con.commit()

    def set(self, row_key_dict, **field_dict):
        return self.sets([row_key_dict], [field_dict])
 
    def sets(self, row_key_dicts, field_dicts):
        for row_key_dict, field_dict in zip(row_key_dicts, field_dicts):
            (row_id_col, row_id_val), = list(row_key_dict.items())
            for column_name, column_value in field_dict.items():
                if column_name not in self.columns:
                    self.create_column(column_name, column_value)
                qry = self._SQL_UPDATE_SINGLE_ROW.format(table_name=self._TABLE_NAME,
                                                         row_id_col=row_id_col,
                                                         row_id_val=row_id_val,
                                                         column_name=column_name,
                                                         column_value=column_value)
                cursor = self.con.execute(qry)
                rows = cursor.fetchall()
                assert len(rows) == 1, f"Did not find row for {row_id_col}={row_id_val}"

    def delete(self, keys):
        indices = ",".join((str(r) for r in keys))
        qdel = self._SQL_DELETE.format(
            table_name=self._TABLE_NAME,
            key_column=self._KEY_COLUMN,
            indices=indices,
        )
        self.con.execute(qdel)
        self.con.commit()

    def acks(self, keys, status=AckStatus.acked):
        self.updates(keys, status)
        if self.delete_on_ack:
            self.delete(keys)

    def apply_timeout(self):
        # Chane unack to ready
        # Don't apply time out if connection isnt open yet
        if self._con is None:
            return
        # Make sure we do not apply the timeout logic too frequently
        dt = time.time() - self.last_timeout_application
        if dt < self.timeout:
            return
        logger.debug(f"Applying timeout on old unack messages on {self.path}")
        logger.debug(f"Last applied timeout {dt:1.1f} sec ago")
        time_cutoff = time.time() - self.timeout
        qtimeout = self._SQL_TIMEOUT.format(
            table_name=self._TABLE_NAME, timeout=time_cutoff
        )
        self._con.execute(qtimeout)
        self._con.commit()
        self.last_timeout_application = time.time()
        logger.debug(f"Finished recycling messages at {self.last_timeout_application}")

    def free(self):
        cursor = self.con.execute(self._SQL_FREE.format(table_name=self._TABLE_NAME))
        (n,) = cursor.fetchone()
        self.con.commit()
        return n

    def done(self):
        cursor = self.con.execute(self._SQL_DONE.format(table_name=self._TABLE_NAME))
        (n,) = cursor.fetchone()
        self.con.commit()
        return n

    @cachetools.func.ttl_cache(maxsize=1, ttl=10)
    def approx_count(self):
        return self._count()

    def _count(self):
        cursor = self.con.execute(self._SQL_COUNT.format(table_name=self._TABLE_NAME))
        (n,) = cursor.fetchone()
        return n
    
    def count(self):
        return self._count()

    def clear_acked_data(self):
        pass

    def shrink_disk_usage(self):
        pass


def test():
    # Initialized queue should be zero sized
    q = SQLiteAckQueue("temp.db", unique_column="id")
    assert q.count() == 0

    # Raise an error -- we have zero items
    items = q.gets(1)
    # Does not raise an error -- key does not exist
    try:
        q.acks([7])
        raise RuntimeError("Expected to raise KeyError")
    except KeyError:
        pass

    # Cannot put in dicts
    try:
        q.puts([{} for _ in range(10)])
        raise RuntimeError("Expected to raise ValueError")
    except ValueError:
        pass

    # Initialize list
    q.puts([{'id': i} for i in range(10)])
    assert q.count() == 10

    # Won't duplicate items
    q.puts([{'id': i} for i in range(10)])
    assert q.count() == 10
    assert q.free() == 10
    assert q.done() == 0

    # Will add a new column
    q.puts([{'id': i, 'color': str(i + 100)} for i in range(10, 21)])
    assert q.count() == 21
    assert q.free() == 21
    assert q.done() == 0

    # Get items
    keys, items = q.gets(7, return_keys=True)
    assert len(keys) == len(items) == 7
    assert q.count() == 21
    assert q.free() == 14
    assert len(keys) == len(items) == 7

    all_items = [i for i in items]

    # We have finished processing keys; now ack them
    q.acks(keys)
    assert q.count() == 21
    assert q.free() == 14

    # Ack them again -- should be idempotent
    q.acks(keys)
    assert q.count() == 21
    assert q.free() == 14

    # This should get the remainder of the items
    keys, items = q.gets(50, return_keys=True)
    assert len(keys) == len(items) == 14
    assert q.count() == 21
    assert q.free() == 0

    # Test that we got back our original items
    all_items.extend(items)
    items_1 = [i for i in all_items if i['color'] is None]
    items_2 = [i for i in all_items if i['color'] is not None]
    assert len(all_items) == 21
    assert len(items_1) == 10
    assert len(items_2) == 11

    # Will update item fields in place
    assert q.count() == 21
    q.sets([{'id': i} for i in range(8)],
          [{"id2": i + 500} for i in range(8)])
    assert q.count() == 21
    items = q.gets(50, read_all=True)
    assert len([i for i in items if i['id2'] is not None]) == 8


    os.remove('temp.db')


if __name__ == "__main__":
    test()
