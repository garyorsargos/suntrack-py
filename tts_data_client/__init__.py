import s3fs
import pandas as pd
from typing import Optional, List

class TTSClient:
    def __init__(self):
        self.fs = s3fs.S3FileSystem(anon=True)
        self.bucket = "oedi-data-lake"
        self.base_prefix = "tracking-the-sun"

    def query(self, year: int, state: Optional[str] = None, technology: Optional[str] = None, limit: int = 1, field_filters: Optional[dict] = None) -> pd.DataFrame:
        """
        Query the Tracking-the-Sun dataset for a given year, state, and technology.
        Optionally filter the resulting DataFrame by field_filters, e.g. {"system_size": (">", 10)}
        Returns a pandas DataFrame with the results.
        """
        prefix = f"{self.base_prefix}/{year}"
        if state:
            prefix += f"/state={state}"
        if technology:
            prefix += f"/technology={technology}"
        s3_path = f"{self.bucket}/{prefix}"
        files = self.fs.glob(f"{s3_path}/*.parquet")
        if not files:
            raise FileNotFoundError(f"No Parquet files found for query: {prefix}")
        dfs = []
        for file in files[:limit]:
            with self.fs.open(file, "rb") as f:
                dfs.append(pd.read_parquet(f, engine="pyarrow"))
        df = pd.concat(dfs, ignore_index=True)
        # Apply flexible field filters
        if field_filters:
            for field, (op, value) in field_filters.items():
                if op == "==":
                    df = df[df[field] == value]
                elif op == ">":
                    df = df[df[field] > value]
                elif op == ">=":
                    df = df[df[field] >= value]
                elif op == "<":
                    df = df[df[field] < value]
                elif op == "<=":
                    df = df[df[field] <= value]
                elif op == "!=" or op == "<>":
                    df = df[df[field] != value]
                else:
                    raise ValueError(f"Unsupported operator: {op}")
        return df

    def get_fields(self, year: int, state: Optional[str] = None, technology: Optional[str] = None) -> List[str]:
        """
        Return the list of fields (columns) in the dataset for the given query.
        """
        prefix = f"{self.base_prefix}/{year}"
        if state:
            prefix += f"/state={state}"
        if technology:
            prefix += f"/technology={technology}"
        s3_path = f"{self.bucket}/{prefix}"
        files = self.fs.glob(f"{s3_path}/*.parquet")
        if not files:
            raise FileNotFoundError(f"No Parquet files found for query: {prefix}")
        # Just read the first file to get columns
        with self.fs.open(files[0], "rb") as f:
            df = pd.read_parquet(f, engine="pyarrow")
        return list(df.columns)

    def count_rows(self, year: int, state: Optional[str] = None, technology: Optional[str] = None, max_files: Optional[int] = None) -> int:
        """
        Return the total number of rows for the given query. Optionally limit the number of files scanned.
        """
        prefix = f"{self.base_prefix}/{year}"
        if state:
            prefix += f"/state={state}"
        if technology:
            prefix += f"/technology={technology}"
        s3_path = f"{self.bucket}/{prefix}"
        files = self.fs.glob(f"{s3_path}/*.parquet")
        if not files:
            raise FileNotFoundError(f"No Parquet files found for query: {prefix}")
        if max_files:
            files = files[:max_files]
        total_rows = 0
        for file in files:
            with self.fs.open(file, "rb") as f:
                # Only read metadata for row count
                pf = pd.read_parquet(f, engine="pyarrow")
                total_rows += len(pf)
        return total_rows

    def print_summary(self, year: int, state: Optional[str] = None, technology: Optional[str] = None, max_files: Optional[int] = None):
        """
        Print a summary of the dataset for the given query: fields and row count.
        """
        try:
            fields = self.get_fields(year, state, technology)
            print(f"Fields: {fields}")
            nrows = self.count_rows(year, state, technology, max_files=max_files)
            print(f"Total rows: {nrows}")
        except Exception as e:
            print(f"Error: {e}") 