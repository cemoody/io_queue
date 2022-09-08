import math
import os.path
from dataclasses import dataclass
from typing import Callable

from loguru import logger

from io_queues import IOQueues
from sqliteack_queue import AckStatus
from sqliteack_queue import SQLiteAckQueue


def switch_modal_function(stub=None, use_modal=True, **kwargs):
    def wrapper(inner_func):
        if use_modal:
            assert stub is not None, "If using modal, please provide `stub`."
            return stub.function(**kwargs)(inner_func)
        else:
            return inner_func
    return wrapper


def submit_func_default(func, task_id, **kwargs):
    return func(task_id, **kwargs)


@dataclass
class Link:
    ioqueues: IOQueues
    function: Callable
    tasks: SQLiteAckQueue

    def set_inputs(self, rows):
        return self.ioqueues.input_q.puts(rows)
    
    def get_outputs(self, n):
        return self.ioqueues.output_q.gets(n)


class Linker:
    links = {}
    _task_count = {}

    def __init__(self, fn_q="queues.db", fn_tasks="tasks.db", submit_func=submit_func_default):
        self.fn_q = fn_q
        self.fn_tasks = fn_tasks
        self.submit_func = submit_func

    def link(self, taskq_kwargs={}, **kwargs):
        def wrapper(inner_func):
            name = inner_func.__name__
            q = IOQueues(self.fn_q, name=name, **kwargs)
            tasks = SQLiteAckQueue(self.fn_tasks, table_name=f"tasks_{name}", **taskq_kwargs)

            def func(task_id, **kwargs):
                tasks.acks([task_id])
                # If there's not input queue, just run the function
                # with no arguments
                args = [q.gets()] if q.input_q else []
                out_rows = inner_func(*args, **kwargs)
                if q.output_q_name:
                    q.puts(out_rows)
                # Mark this task as complete
                tasks.acks([task_id], status=AckStatus.ack_done)

            self.links[name] = Link(q, func, tasks)
            return func
        return wrapper

    def run_once(self):
        for name, link in self.links.items():
            delta = link.ioqueues.size_ready()
            n_tasks_required = int(math.ceil(delta / link.ioqueues.batch_size))
            n_tasks_active = link.tasks.active()
            while n_tasks_required >= n_tasks_active:
                self.create_task(name, link)
                n_tasks_active += 1
    
    def run_until_complete(self, **kwargs):
        while not self._check_complete():
            self.run_once(**kwargs)

    def _check_complete(self):
        completes = {}
        for name, link in self.links.items():
            completes[name] = link.ioqueues.size_ready() == 0
        return all(completes.values())

    def create_task(self, name, link):
        task_id = self._task_count.get(name, 0)
        task_cfg = {'task_index': task_id}
        logger.info(f"Creating task {task_id} for {name}")
        key = link.tasks.put(task_cfg)
        self.submit_func(link.function, key, **task_cfg)
        self._task_count[name] = task_id + 1


def test_ioq_simple(n=25):
    for batch_size in [1, 7]:
        fn = 'tasks.db'
        if os.path.exists(fn):
            os.remove(fn)
        fn = 'queues.db'
        if os.path.exists(fn):
            os.remove(fn)

        l = Linker(fn)

        @l.link(input_q_name="inq", output_q_name="outq", batch_size=batch_size)
        def transform(items, **cfg):
            logger.info(f"Processing {len(items)} items {cfg}")
            idxs = [{'out': item['idx'] + 50, **item} for item in items]
            return idxs

        # Load up the DAG with initial data
        inputs = [dict(idx=idx) for idx in range(n)]
        l.links['transform'].set_inputs(inputs)
        
        # Run until all links report there's no more data
        # left to process
        l.run_until_complete()

        # Check outputs are what we expected
        items = l.links['transform'].get_outputs(100)
        flat = [item['out'] for item in items]
        assert all(k in flat for k in range(50, 75))
        os.remove("tasks.db")
        os.remove("queues.db")


def test_ioq_complex(n=5, batch_size=10):
    fn = 'tasks.db'
    if os.path.exists(fn):
        os.remove(fn)
    fn = 'queues.db'
    if os.path.exists(fn):
        os.remove(fn)

    l = Linker(fn)

    @l.link(input_q_name="urls", output_q_name="links", batch_size=batch_size)
    def crawler(items, **cfg):
        logger.info(f"Processing {len(urls)} items {cfg} ")
        links = []
        for item in items:
            url = item['url']
            links.append({'link': f"{url}/a.html", **item})
            links.append({'link': f"{url}/b.html", **item})
        return links

    @l.link(input_q_name="links", output_q_name="vecs", batch_size=batch_size)
    def transform(items, **cfg):
        logger.info(f"Processing {len(items)} links {cfg}")
        vectors = []
        for item in items:
            vectors.append({'vector': [1, 2, 3], **item})
        return vectors

    @l.link(input_q_name="vecs", output_q_name="mean_vec", batch_size=10000)
    def sum_vector(items, **cfg):
        import numpy as np
        vecs = [item['vector'] for item in items]
        logger.info(f"Processing {len(vecs)} vecs {cfg}")
        if len(vecs) == 0:
            return []
        sum = float(np.concatenate(vecs).sum())
        rows = [dict(sum_vector=sum)]
        return rows

    # Load up the DAG with initial data
    urls = [dict(url=f"{idx}.com") for idx in range(n)]
    l.links['crawler'].set_inputs(urls)
    
    # Run until all links report there's no more data
    # left to process
    l.run_until_complete()

    # Check outputs are what we expected
    row, = l.links['sum_vector'].get_outputs(100)
    assert row['sum_vector'] == 60.0
    os.remove("tasks.db")
    os.remove("queues.db")

if __name__ == '__main__':
    test_ioq_simple()
    test_ioq_complex()