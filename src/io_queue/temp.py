import pandas as pd
from os.path import basename

from multi_queue import MultiQueue
from utils import products_from_pg
from utils import products_from_bq 


job = MultiQueue(root="/cache")


@job.link(output_q="qproduct", skip_if_present=["qproduct-image", "url"], output_q_maxsize=1024,
          modal_function_kwargs=dict())
def load_product():
    products = products_from_pg()
    present = products_from_bq()
    products = [p.update(dict(local_fn=basename(local_fn))) for p in products]
    todo = [p for p in products if p not in present]
    return todo


@job.link(inputq="qproduct", outputq="qproduct-image", batch_size=32,
          skip_if_key_present=["local_fn"], maxq_size=1024,
          modal_function_kwargs=dict())
def downloader(products):
    preprocess = initialize_clip()
    imgs = products.imgs
    products_out = []
    for product in zip(products):
        img = product.img
        im = download(img)
        array = preprocess(im)
        fn = write_to_disk(array)
        product['image_local_fn'] = fn
        products_out.append(product)
    # job.link automatically transform a list of JSON objects correctly
    return products_out


@job.link(input_q="qproduct-image", output_q="qproduct-image-embed", 
          skip_if_key_present=["image_local_fn"],
          # output_gcs="crawler_job_10", 
          batch_size=2048, 
          modal_function_kwargs=dict(gpu=True))
def embedder(products, mini_batch_size=32):
    model = load_clip()
    product_lookup = {p['image_local_fn']: p for p in products}
    images = list(product_lookup.keys())
    dl = DataLoader(images, batch_size=mini_batch_size)
    products = []
    for fns, batch in dl:
        vector_batch = model.encode_image(batch)
        for fn, vector in zip(fns, vector_batch):
            product = product_lookup[fn]
            # job.link will automatically transform a vector into a bunch of columns
            product['vector_image_0000'] = vector
            products.append(product)
    return products


@job.link(input_q="qproduct-image-embed", output_q="qproduct-image-embed-concat", 
          skip_if_key_present=["id"],
          batch_size=8192, modal_function_kwargs=dict())
def to_bq(products):
    temp_fn = to_parquet(products)
    upload_to_bq(temp_fn)
    os.remove(temp_fn)
    product_ids = [p['id'] for p in products]
    return product_ids


if __name__ == '__main__':
    if debug:
        job.run(use_modal=False)
    else:
        job.run()
