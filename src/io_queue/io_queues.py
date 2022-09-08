"""
IOQueues is an object that tracks the progress of items in input and output
queue and offers a simple way to track jobs finished, failed or complete
as they get places into the output queue.

"""
import os
from uuid import uuid4
from loguru import logger
from sqliteack_queue import AckStatus

from sqliteack_queue import SQLiteAckQueue

class IOQueues:
    _SQL_SIZE_DELTA = """
    SELECT COUNT(*) 
    FROM {input_q_name} 
    LEFT JOIN {output_q_name} 
    ON {input_q_name}.{input_q_id_column}={output_q_name}.{output_q_id_column}
    WHERE {output_q_name}.{output_q_id_column} IS NULL
     AND {input_q_name}.status < %s
    """ % (AckStatus.unack)

    _SQL_GETS = """
    SELECT {input_q_name}.*
    FROM {input_q_name} 
    LEFT JOIN {output_q_name} 
    ON {input_q_name}.{input_q_id_column}={output_q_name}.{output_q_id_column}
    WHERE {output_q_name}.{output_q_id_column} IS NULL
     AND {input_q_name}.status < %s
    """ % (AckStatus.unack)

    def __init__(self, filename, input_q_name=None, output_q_name=None, 
                 input_q_id_column=None,
                 output_q_id_column=None,
                 batch_size=None, name=None,
                 queue_kwargs={}):
        self.filename = filename
        self.input_q_name = input_q_name
        self.output_q_name = output_q_name
        self.input_q = SQLiteAckQueue(filename, table_name=input_q_name, **queue_kwargs) if input_q_name else None
        self.output_q = SQLiteAckQueue(filename, table_name=output_q_name, **queue_kwargs) if output_q_name else None
        self.batch_size = batch_size 
        self.input_q_id_column = input_q_id_column or "_id"
        self.output_q_id_column = output_q_id_column or "_id"
        self.name = name
    
    def acks(self, keys):
        self.input_q.acks(keys)

    def puts(self, rows):
        """ Place data rows in the output queue.
        """
        self.output_q.puts(rows)

    def gets(self, batch_size=None):
        """ Get an interator over batches from the input queue
        that are not in the output q.
        """
        if batch_size is None:
            batch_size = self.batch_size
        if self.input_q is None:
            return None
        query = self._SQL_GETS.format(
            input_q_name=self.input_q_name,
            output_q_name=self.output_q_name,
            input_q_id_column=self.input_q_id_column,
            output_q_id_column=self.output_q_id_column,
            batch_size=self.batch_size
        )
        cursor = self.input_q.con.execute(query)
        rows = cursor.fetchall()
        keys = [row[0] for row in rows]
        items = self.input_q._process_rows(rows)
        self.input_q.updates(keys, AckStatus.unack)
        return keys, items

    def size_delta(self):
        """ Estimate how many rows are in input q that are not 
        in the output q and also not in submitted and ongoing jobs.
        """
        query = self._SQL_SIZE_DELTA.format(
            input_q_name=self.input_q_name,
            output_q_name=self.output_q_name,
            input_q_id_column=self.input_q_id_column,
            output_q_id_column=self.output_q_id_column
            )
        cursor = self.input_q.con.execute(query)
        (n,) = cursor.fetchone()
        return n


def test_ioq_puts(n=25):
    fn = 'test_cache'
    if os.path.exists(fn):
        os.remove(fn)
    ioq = IOQueues("./test_cache", output_q_name="test_outputq")

    rows = [dict(idx=idx) for idx in range(n)]
    ioq.puts(rows)
    assert ioq.output_q.count() == n
    os.remove('./test_cache')


def test_ioq_end_to_end(n=25):
    import time
    fn = 'test_cache'
    if os.path.exists(fn):
        os.remove(fn)

    rows = [dict(idx=idx) for idx in range(n)]
    ioq0 = IOQueues("./test_cache", output_q_name="test_inputq")
    ioq0.puts(rows)

    ioq = IOQueues("./test_cache", 
                  input_q_name="test_inputq", 
                  output_q_name="test_outputq",
                  queue_kwargs=dict(timeout=0.2))
    assert ioq.size_delta() == n

    _, batch = ioq.gets(n * 2)
    assert len(batch) == n
    assert ioq.size_delta() == 0

    # Items recently got are unavailable
    _, batch2 = ioq.gets(n * 2)
    assert len(batch2) == 0
    assert ioq.size_delta() == 0

    # Now we timeout and return unack messages to the queue
    time.sleep(0.3)

    # Unack messages return to the queue
    assert ioq.size_delta() == n
    keys, batch3 = ioq.gets(n * 2)
    assert len(batch3) == n

    # Ack the messages (now messages default to staying on the queue)
    ioq.acks(keys)
    assert ioq.size_delta() == 0
    os.remove(fn)


def test_ioq_e2e_join(n=25):
    fn = 'test_cache'
    if os.path.exists(fn):
        os.remove(fn)

    import time
    rows = [dict(idx=idx) for idx in range(n)]
    ioq0 = IOQueues("./test_cache", output_q_name="test_inputq")
    ioq0.puts(rows)

    # Set timeout to 0, which will effectively turn unack into ready
    ioq = IOQueues("./test_cache", 
                  input_q_name="test_inputq", 
                  output_q_name="test_outputq",
                  queue_kwargs=dict(timeout=0.0001))

    # Get messages, but with 0 timeout messages become reavailable
    _, batch = ioq.gets(n * 2)
    time.sleep(0.1)
    delta = ioq.size_delta()
    assert  delta == n

    # Now we process the messages from outputq -- now messages are unavailable
    transformed = [{**row, **{'processed': True}} for row in batch]
    ioq.puts(transformed)
    assert ioq.size_delta() == 0

    os.remove('./test_cache')


if __name__ == '__main__':
    test_ioq_puts()
    test_ioq_end_to_end()
    test_ioq_e2e_join()