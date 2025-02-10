# Kstack processing

The Kstack dataset processing is managed using Prefect. The dataset, which comprises open-source Kotlin code, is sourced from the Hugging Face repository. We employ a near-deduplication algorithm to eliminate duplicate entries. Additionally, dataset quality is verified using Deedqu.


# Near deduplication
Code for running near-deduplication with MinHash and LSH indexing

### Setup

````
pip install -r requirements.txt
````

Login to be able to push the dataset to the hub after deduplication and clone your huggingface-hub repositories:

````
huggingface-cli login
````

And make sure you have git-lfs installed.

If you use datasets with different column names from the BigCode ones, you might need to change `PATH_COLUMN` and `CONTENT` variables in `minhash_deduplication.py`.

### Usage

Setup a local or remove Prefect env and add kstack_flow.py to you flows.

To run near deduplication use the following command and adapt the arguments for your case:

````
python near_deduplicate.py \
    --dataset_name JetBrains/KStack \
    --org bigcode-data \
    --repo_name huggingface_repo \
    --out_path ./data/KStack \
    --text_column content 
````

To make just a test run with a subset of the data, set `test_run` argument to True.

The first time you load the dataset might be slow if it is large, but the data is saved in the cache thanks to `datasets`, and the subsequent calls will be fast.

### Alternative Deduplication Script

`minhash_deduplication_alt.py` is an alternative you might find useful to use as well. It is best for a single multi-core machine environment and uses similar parameters to the original deduplication script.

```bash
pip install -r requirements_alt.txt
# Quick example
python minhash_deduplication_alt.py --dataset codeparrot/codeparrot-clean-valid \  
    --split train \
    --column content \
    --cache-dir .cache \
    --verbose
# For details on the arguments, see the help message
python minhash_deduplication_alt.py --help
```

#### Implementation Analysis

This is for the alternative script that is designed for single-machine setup.

More details can be found on 
Dataset: https://huggingface.co/datasets/JetBrains/KStack
Near Deduplication: https://github.com/bigcode-project/bigcode-analysis/tree/main/data_analysis
Deequ framework: https://github.com/awslabs/deequ
Prefect: https://docs-3.prefect.io/3.0/get-started/index