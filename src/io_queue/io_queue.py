"""
- IOQueue.link submits row ids to the job, the job then acks those job ids and marks them as complete
- ensure that queue size is easily readable without running a table scan so that way the job runner knows when to fire off jobs
- keep backing sqlite store on a shared modal volume when running remotely
- use file lock on queuestore to prevent concurrent writes
- Mark tasks  (submitted, started, ended) to keep track of them
-   and (how many inputs each task is claiming)
-   and the timestamp (to keep tasks fresh and cycle out stale ones)
"""
import os
import math
from sqlitedict import SqliteDict
from uuid import uuid4
from loguru import logger

from io_queue.sqliteack_queue import SQLiteAckQueue


def create_table(table_name, row):
    query = "CREATE TABLE {0} ({1})".format(table, ", ".join(fieldset))

        c.execute(query)


def switch_modal_function(stub=None, use_modal=True, **kwargs):
    def wrapper(inner_func):
        if use_modal:
            assert stub is not None, "If using modal, please provide `stub`."
            return stub.function(**kwargs)(inner_func)
        else:
            return inner_func
    return wrapper


class IOQueue:
    links = {}

    def __init__(self, stub):
        self.stub = stub

    def link(self, input_q=None, output_q=None, skip_if_present=None,
             batch_size=1, modal_function_kwargs={}):
        def wrapper(inner_func):
            name = inner_func.__name__
            q = Store(self.root, input_q=input_q, output_q=output_q,
                      skip_if_present=skip_if_present, batch_size=batch_size,
                      name=name)

            @switch_modal_function(self.stub, use_modal=self.use_modal, 
                                   **modal_function_kwargs)
            def func(task_id, **kwargs):
                q.set_task(task_id, status="started")

                for in_rows in q.get(batch_size):
                    out_rows = inner_func(in_rows, **kwargs)
                    if output_q:
                        q.puts(out_rows)
                q.set_task(task_id, status="done")

            self.links[name] = (q, func)
            return func
        return wrapper

    def run(self, use_modal=False):
        self.use_modal = use_modal
        for (q, func) in self.links.values():
            if delta := q.size_delta() > q.batch_size:
                n_tasks = int(math.floor(delta / q.batch_size))
                for _ in range(n_tasks):
                    task_id = q.create_task()
                    q.set_task(task_id, status="submitted")
                    if use_modal:
                        call_id = func.submit(task_id)
                        q.set_task(task_id, call_id=call_id)
                    else:
                        func(task_id)


class Store:
    def __init__(self, filename, input_q=None, output_q=None, skip_if_present=None,
                 batch_size=None, name=None):
        self.filename = filename
        self.input_q = SQLiteAckQueue(filename, table_name=input_q)
        self.output_q = SQLiteAckQueue(filename, table_name=output_q)
        self.tasks = SqliteDict(filename, tablename="tasks", autocommit=True)
        self.skip_if_present = skip_if_present
        self.batch_size = batch_size 
        self.name = name

    def put(self, rows):
        """ Place data rows in the output queue.
        """
        pass

    def get(self, batch_size=1):
        """ Get an interator over batches from the input queue
        that are not in the output q.
        """
        logger.debug(f"Set {task_id} to {fields}")
        yield rows

    def size_delta(self):
        """ Estimate how many rows are in input q that are not 
        in the output q and also not in submitted and ongoing jobs.
        """
        pass

    def create_task(self, status="created"):
        """ Create a new task entry
        """
        task_id = uuid4()
        self.tasks[task_id] = dict(status=status)
        logger.debug(f"Created task {task_id}")
        return task_id
    
    def set_task(self, task_id, **fields):
        task = self.tasks[task_id]
        task.update(fields)
        self.tasks[task_id] = task
        logger.debug(f"Set {task_id} to {fields}")

    def read_outputq(self):
        pass


def test_ioq_outputq():
    mq = IOQueue(root="./cache")

    @mq.link(output_q="outq")
    def qload():
        idxs = list(range(25))
        return idxs
    
    mq.run(use_modal=False)
    keys, _ = mq.output_q.gets(50, ack=False)
    assert all(k in keys for k in range(25))
    os.remove("./cache")


if __name__ == '__main__':
    test_ioq()