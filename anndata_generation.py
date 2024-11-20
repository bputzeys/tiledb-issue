import numpy as np
import scipy.sparse as sp
import anndata as ad
import pandas as pd
from tqdm.auto import tqdm

def generate_large_anndata(
    n_cells=1_000_000,
    n_genes=20_000,
    sparsity=0.95,
    n_batch=50,
    seed=42,
    save_path=None
):
    """
    Generate a large AnnData object with specified dimensions and sparsity.
    
    Parameters:
    -----------
    n_cells : int
        Number of cells (default: 1,000,000)
    n_genes : int
        Number of genes (default: 20,000)
    sparsity : float
        Fraction of zero values (default: 0.95)
    n_batch : int
        Number of batches to split data generation (default: 50)
    seed : int
        Random seed for reproducibility (default: 42)
    save_path : str, optional
        Path to save the AnnData object
        
    Returns:
    --------
    AnnData object if save_path is None, else saves to file
    """
    np.random.seed(seed)
    print(f"Generating dataset with shape: ({n_cells:,}, {n_genes:,})")
    
    # Calculate expected size
    non_zero_elements = int(n_cells * n_genes * (1 - sparsity))
    expected_size_gb = (non_zero_elements * 4 + n_cells * 8 + n_genes * 8) / (1024**3)
    print(f"Expected size (approximate): {expected_size_gb:.2f} GB")
    
    # Generate gene names
    gene_names = [f"Gene_{i}" for i in range(n_genes)]
    
    # Generate cell metadata
    print("Generating cell metadata...")
    cell_metadata = pd.DataFrame(
        {
            'batch': np.random.randint(0, 20, n_cells),
            'n_counts': np.random.negative_binomial(100, 0.3, n_cells),
            'n_genes': np.random.negative_binomial(80, 0.3, n_cells)
        },
        index=[f"Cell_{i}" for i in range(n_cells)]
    )
    
    # Generate data in batches
    print("Generating expression matrix in batches...")
    batch_size = n_cells // n_batch
    data_list = []
    
    for i in tqdm(range(n_batch)):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, n_cells)
        n_cells_batch = end_idx - start_idx
        
        # Generate random sparse matrix for this batch
        n_nonzero = int(n_cells_batch * n_genes * (1 - sparsity))
        rows = np.random.randint(0, n_cells_batch, n_nonzero)
        cols = np.random.randint(0, n_genes, n_nonzero)
        data = np.random.negative_binomial(5, 0.3, n_nonzero)
        
        # Create sparse matrix for this batch
        batch_matrix = sp.csr_matrix(
            (data, (rows, cols)),
            shape=(n_cells_batch, n_genes)
        )
        data_list.append(batch_matrix)
    
    # Combine all batches
    print("Combining batches...")
    X = sp.vstack(data_list)
    del data_list, rows, cols, data
    
    # Create AnnData object
    print("Creating AnnData object...")
    adata = ad.AnnData(
        X=X,
        obs=cell_metadata,
        var=pd.DataFrame(index=gene_names)
    )
    
    # Add some gene metadata
    adata.var['highly_variable'] = np.random.choice([True, False], size=n_genes, p=[0.1, 0.9])
    adata.var['mean_expression'] = np.array(X.mean(axis=0)).flatten()
    
    # Save if path provided
    if save_path:
        print(f"Saving to {save_path}...")
        adata.write(save_path)
        return None
    
    return adata


# To generate and save:
generate_large_anndata(
    n_cells=1_000_000,
    n_genes=20_000,
    save_path='large_dataset.h5ad'
)

