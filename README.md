# Issue with RAM when converting and uploading large datasets as TileDB experiment to S3

This repo contains files to reproduce an issue I encounterd while uploading a large dataset to S3.

The file [anndata_generation.py](anndata_generation.py) can be used to generate dummy data. Alternatively, it is also available via `git lfs`.

Running the [upload_tiledb.py](upload_tiledb.py) requires a connectio to S3 by specifying the keys and bucket. 

## Problem statement
After registering the remaining h5ad to upload, RAM increases as expected for each anndata object. However, when one pass finished, RAM does not return to its original but is roughly 2 GB bigger with each pass. This eventually results in RAM being full. 

```
        for ad in adjusted_data_paths[1:]:
            tiledbsoma.io.from_h5ad(
                experiment_uri = experiment_uri,
                input_path = ad,
                measurement_name = "RNA",
                registration_mapping=rd,                    
                context=context,
            )
            print(f"Successfully uploaded {ad}")
```