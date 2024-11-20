import os
import anndata
import tiledbsoma
import tiledbsoma.io

def split(input_path):
    name = "data"
    data = anndata.read_h5ad(input_path, backed='r')
    
    batch_size = 100000
    output_paths = []
    for start in range(0, data.shape[0], batch_size):
        end = min(start + batch_size, data.shape[0])

        # transform
        ann_data_batch = data[start:end].to_memory()
        output_path = f"{name}_{start}_{end}.h5ad"
        ann_data_batch.write_h5ad(output_path)
        output_paths.append(output_path)
    return output_paths

def upload_multiple_h5ads_to_tiledb(experiment_uri: str, adjusted_data_paths: list[str], context: tiledbsoma.SOMATileDBContext):
        """
        Converts the adjusted data to TileDB format from a list of h5ad paths and pushes them directly to AWS.

        Parameters
        ----------
        experiment_uri: str
            The URI to the experiment on AWS
        adjusted_data_paths: list[str]
            The paths to the adjusted h5ads
        context: tiledbsoma.SOMATileDBContext
            The context to be used for the TileDB operations
        """

        tiledbsoma.io.from_h5ad(
            experiment_uri = experiment_uri,
            input_path = adjusted_data_paths[0],
            measurement_name="RNA",
            context = context,
        )
        print("Successfully transformed and registered the first h5ad.")

        rd = tiledbsoma.io.register_h5ads(
            experiment_uri = experiment_uri,
            h5ad_file_names = adjusted_data_paths[1:],
            measurement_name="RNA",
            obs_field_name="obs_id",
            var_field_name="var_id",
            append_obsm_varm=True,
            context=context,
        )
        print(f"Successfully registered the remaining {len(adjusted_data_paths[1:])} h5ads.")
        print(f"Uploading remaining h5ads to TileDB...")

        # the issue happens in this for loop
        # RAM usage increases with each iteration
        # it falls back down after the loop is done but not to the original level
        # over time, this will cause the RAM to be filled up
        for ad in adjusted_data_paths[1:]:
            tiledbsoma.io.from_h5ad(
                experiment_uri = experiment_uri,
                input_path = ad,
                measurement_name = "RNA",
                registration_mapping=rd,                    
                context=context,
            )
            print(f"Successfully uploaded {ad}")

if __name__ == "__main__":

    config = {
        "vfs.s3.aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
        "vfs.s3.aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "vfs.s3.region": os.environ.get("S3_REGION"),
    }
    context = tiledbsoma.SOMATileDBContext(tiledb_config = config)
    splits = split("./large_dataset.h5ad")
    experiment_uri = "s3://test"
    upload_multiple_h5ads_to_tiledb(experiment_uri, splits, context)