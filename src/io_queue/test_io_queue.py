from io_queue import MultiQueue
from itertools import chain


def test_mq():
    mq = MultiQueue(root="./cache")

    @mq.link(output_q="qproduct", 
              skip_if_present=["qproduct-image", "url"],
              output_q_maxsize=1024,
              modal_function_kwargs=dict())
    def qload():
        idxs = list(range(25))
        return idxs
    
    mq.run(use_modal=False)
    flat = sorted(chain(*list(mq.output_q.get())))
    assert all(f in range(25) for f in flat)

