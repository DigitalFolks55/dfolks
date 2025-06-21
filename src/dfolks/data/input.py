"""Load data from files.

Need to do
0) Simplify the function.
1) Integrate with parsers.
"""

import glob as gl
import pathlib as pth

import pandas as pd


def load_flat_file(path: str, load_all: bool = False) -> pd.DataFrame:
    """Load data from a file or files.

    Inputs
    ----------
    path: File path or directory path.
    load_all: Load all files in a directory.
    sep: Separator.
    """
    # Load a file.
    if pth.Path(path).is_file():
        if path.endswith(".csv"):
            dfs = pd.read_csv(path, low_memory=False)
        elif path.endswith(".xlsx"):
            dfs = pd.read_excel(path)
    # Load files in a directory.
    elif pth.Path(path).is_dir() and load_all:
        df_list = []
        files = gl.glob(f"{path}/*")
        for file in files:
            if file.endswith(".csv"):
                df = pd.read_csv(file, low_memory=False)
                df_list.append(df)
            elif file.endswith(".xlsx"):
                df = pd.read_excel(file)
                df_list.append(df)
        dfs = pd.concat(df_list)
        dfs.reset_index(drop=True, inplace=True)
    # if the path is a directory but load_all is False, raise an error.
    elif pth.Path(path).is_dir() and not load_all:
        raise ValueError("Path is a directory, thus 'load_all' should be True")
    else:
        raise NotImplementedError("Unknown file type and path. Check variables again.")

    return dfs
