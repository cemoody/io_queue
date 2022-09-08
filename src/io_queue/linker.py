import math
import os.path
from io_queue.sqliteack_queue import AckStatus

from io_queues import IOQueues
from sqliteack_queue import SQLiteAckQueue


def switch_modal_function(stub=None, use_modal=True, **kwargs):
    def wrapper(inner_func):
        if use_modal:
            assert stub is not None, "If using modal, please provide `stub`."
            return stub.function(**kwargs)(inner_func)
        else:
            return inner_func
    return wrapper


class Linker:
    links = {}
    _task_count = {}

    def __init__(self, stub=None, fn_q="queues.db", fn_tasks="tasks.db"):
        self.fn_q = fn_q
        self.fn_tasks = fn_tasks
        self.stub = stub

    def link(self, modal_function_kwargs={}, taskq_kwargs={}, **kwargs):
        def wrapper(inner_func):
            name = inner_func.__name__
            q = IOQueues(self.root, name=name, **kwargs)
            tasks = SQLiteAckQueue(self.fn_tasks, table_name=f"tasks_{name}", **taskq_kwargs)

            @switch_modal_function(self.stub, use_modal=self.use_modal, 
                                   **modal_function_kwargs)
            def func(task_id, **kwargs):
                tasks.ack(task_id)
                # If there's not input queue, just run the function
                # with no arguments
                args = [q.gets()]  if q.input_q else []
                out_rows = inner_func(*args, **kwargs)
                if q.output_q_name:
                    q.puts(out_rows)
                # Mark this task as complete
                tasks.acks(task_id, status=AckStatus.ack_done)

            self.links[name] = (q, func, tasks)
            return func
        return wrapper

    def run_once(self, use_modal=False):
        self.use_modal = use_modal
        for name, (q, func, tasks) in self.links.items():
            delta = q.size_delta()
            n_tasks_required = int(math.ceil(delta / q.batch_size))
            while n_tasks_required >= 0:
                self.create_task(name, q, func, tasks)
                n_tasks_required -= 1
    
    def run_until_complete(self, **kwargs):
        while not self._check_complete():
            self.run_once(**kwargs)

    def _check_complete(self):
        completes = {}
        for name, (q, func, tasks) in self.links.items():
            completes[name] = q.size_delta() == 0
        return all(completes.values())

    def create_task(self, name, q, func, tasks):
        task_count = self._task_count.get(name, 0)
        task_id = q.init_task()
        task_cfg = {'task_index': task_count}
        tasks.put(task_cfg)
        if self.use_modal:
            call_id = func.submit(task_id)
            q.tasks.set({'task_id': task_id}, dict(call_id=call_id))
        else:
            func(task_id)
        self._task_count[name] = task_count + 1



def test_ioq_load(n=25):
    fn = 'test_cache'
    if os.path.exists(fn):
        os.remove(fn)

    l = Linker(root=fn)
    @l.link(output_q_name="outq")
    def qload():
        idxs = [dict(idx=idx) for idx in range(n)]
        return idxs
    
    # This should place 25 items into the outq
    l.run_until_complete(use_modal=False)
    items = l.links['qload'].output_q.gets(50)
    flat = [item['idx'] for item in items]
    assert all(k in flat for k in range(25))
    os.remove(fn)


def test_ioq_load(n=25):
    fn = 'test_cache'
    if os.path.exists(fn):
        os.remove(fn)

    l = Linker(root=fn)
    @l.link(output_q_name="inq")
    def qload():
        idxs = [dict(idx=idx) for idx in range(n)]
        return idxs

    @l.link(input_q_name="inq", output_q_name="outq")
    def transform(items):
        idxs = [{**item, 'out': item['idx'] + 50 } for item in items]
        return idxs
    
    # This should place 25 items into the out queue from transform
    l.run_until_complete(use_modal=False)
    items = l.links['transform'].output_q.gets(50)
    flat = [item['idx'] for item in items]
    assert all(k in flat for k in range(50, 75))
    os.remove(fn)

if __name__ == '__main__':
    test_ioq_load()