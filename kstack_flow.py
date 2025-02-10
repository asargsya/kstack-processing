from __future__ import annotations
from prefect import flow, task
from myutils import optimal_param, embed_func, UnionFind
from dq import is_valid
import gc
import logging
import os
import random
import time
from collections import defaultdict
from pathlib import Path
import datasets
import numpy as np
from datasets import load_dataset
from tqdm import tqdm

dataset = "JetBrains/KStack"
config = "default"
split = "train"
# data_dir: str = None
revision = "main"
column = "content"
cache_dir = ".cache"
ngram_size: int = 5
num_perm: int = 256
threshold: float = 0.7
output_dir = "output"
SEED = 42
# NON_ALPHA = re.compile("[^A-Za-z_0-9]")
RNG = np.random.RandomState(SEED)
# MAX_HASH = np.uint64((1 << 32) - 1)
MERSENNE_PRIME = np.uint64((1 << 61) - 1)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
datasets.logging.set_verbosity_error()
uf = UnionFind()

@flow
def kstack_near_deduplication():
    global uf
    OUTPUT_BASE = Path(output_dir)
    OUTPUT_BASE.mkdir(exist_ok=True, parents=True)
    output = OUTPUT_BASE / "deduplicated"

    logging.basicConfig(level=logging.INFO)

    time_measures = {}
    start_time = time.time()

    B, R = optimal_param(threshold, num_perm)
    HASH_RANGES = [(i * R, (i + 1) * R) for i in range(B)]
    HASH_TABLES = [defaultdict(set) for _ in range(B)]

    time_measures["load_dataset"] = time.time()
    ds = get_data()
    time_measures["load_dataset"] = time.time() - time_measures["load_dataset"]
    DATA_SIZE = len(ds)


    time_measures["minhash"] = time.time()
    embedded = minhash(ds, HASH_RANGES)
    time_measures["minhash"] = time.time() - time_measures["minhash"]

    time_measures["clustering"] = time.time()
    uf = clustering(embedded, HASH_TABLES)
    time_measures["clustering"] = time.time() - time_measures["clustering"]

    time_measures["filtering"] = time.time()
    final_data = filtering(ds, uf)
    time_measures["filtering"] = time.time() - time_measures["filtering"]

    time_measures["save"] = time.time()
    save_output(final_data, output)
    time_measures["save"] = time.time() - time_measures["save"]

    check_data_quality()

    FINAL_DATA_SIZE = len(final_data)
    DUP_SIZE = DATA_SIZE - FINAL_DATA_SIZE
    PAD = 32

    for key, value in time_measures.items():
        logger.info(f"{key:<{PAD}}: {value:.2f} seconds")
    logger.info(f"{'Data Number (before)':<{PAD}}: {DATA_SIZE}")
    logger.info(
        f"{'Data Number (after)':<{PAD}}: {FINAL_DATA_SIZE} ({FINAL_DATA_SIZE / DATA_SIZE:.2%})"  # noqa: E501
    )
    logger.info(f"{'Duplicate Number':<{PAD}}: {DUP_SIZE} ({DUP_SIZE / DATA_SIZE:.2%})")  # noqa: E501
    logger.info(f"{'Total Time':<{PAD}}: {time.time() - start_time:.2f} seconds")
    logger.info(f"{'Deduplicated Dataset':<{PAD}}: {output}")
    logger.info("🤗 Happy Deduplicating 🤗")


@task
def get_data():
    """Flow: Get the data!"""
    ds = load_dataset(
        path=dataset,
        name=config,
        split="train",
        use_auth_token=True,
        cache_dir=cache_dir,
        revision=revision,
    ).select([i for i in range(1000)])
    return ds


@task
def minhash(ds, HASH_RANGES):
    PERMUTATIONS = np.array(
        [
            (
                RNG.randint(1, MERSENNE_PRIME, dtype=np.uint64),
                RNG.randint(0, MERSENNE_PRIME, dtype=np.uint64),
            )
            for _ in range(num_perm)
        ],
        dtype=np.uint64,
    ).T
    embedded = ds.map(
        function=embed_func,
        fn_kwargs={
            "num_perm": num_perm,
            "hashranges": HASH_RANGES,
            "ngram_size": ngram_size,
            "permutations": PERMUTATIONS,
        },
        input_columns=[column],
        remove_columns=ds.column_names,
        num_proc=os.cpu_count(),
        with_indices=True,
        desc="Fingerprinting...",
    )
    return embedded

@task
def clustering(embedded, HASH_TABLES):
    batch_size: int = 10000
    for i in tqdm(
        range(0, len(embedded), batch_size), dynamic_ncols=True, desc="Iterating MinHashes..."  # noqa: E501
    ):
        batch = embedded[i: i + batch_size]
        for key, Hs in zip(batch["__id__"], batch["__signatures__"]):
            for H, hashtable in zip(Hs, HASH_TABLES):
                hashtable[H].add(key)
    for table in tqdm(HASH_TABLES, dynamic_ncols=True, desc="Clustering..."):
        for cluster in table.values():
            if len(cluster) <= 1:
                continue
            idx = min(cluster)
            for x in cluster:
                uf.union(x, idx)
    return uf

@task
def filtering(ds, uf):
    gc.freeze()
    gc.disable()
    ds = ds.map(
        function=lambda _, idx: {"__cluster__": uf.find(idx)},
        with_indices=True,
        num_proc=1,#os.cpu_count(), filtering is getting stuck with num_proc > 1
        new_fingerprint=str(random.getrandbits(128)),
        desc="Finding clusters...",
    )
    gc.enable()
    gc.collect()
    # This is where the deduplication happens
    # Since there is no easy groupby in datasets
    # I will use this simple filter for now
    final_data = ds.filter(
        function=lambda record, idx: record["__cluster__"] == idx,
        with_indices=True,
        num_proc=1,#os.cpu_count(),
        desc="Filtering clusters...",
    )
    return final_data

@task
def save_output(final_data, output):
    final_data = final_data.remove_columns(["__cluster__"])
    final_data.save_to_disk(output)

@task
def check_data_quality():
    if not is_valid():
        raise Exception("Data quality check have not passed!")

# Run the flow
if __name__ == "__main__":
    kstack_near_deduplication.serve(name="kstack-deployment")