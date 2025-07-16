import s3fs
import pandas as pd
from typing import Optional, List, Union

try:
    from tqdm import tqdm
    _HAS_TQDM = True
except ImportError:
    _HAS_TQDM = False

class TTSClient:
    def __init__(self):
        self.fs = s3fs.S3FileSystem(anon=True)
        self.bucket = "oedi-data-lake"
        self.base_prefix = "tracking-the-sun"

    def _list_years(self):
        # List all available years in the S3 bucket
        prefix = f"{self.base_prefix}/"
        dirs = self.fs.ls(f"{self.bucket}/{prefix}", detail=True)
        years = [d['name'].split('/')[-1] for d in dirs if d['type'] == 'directory' and d['name'].split('/')[-1].isdigit()]
        return sorted(years)

    def _list_states(self, year):
        # List all available states for a given year
        prefix = f"{self.base_prefix}/{year}/"
        dirs = self.fs.ls(f"{self.bucket}/{prefix}", detail=True)
        states = [d['name'].split('=')[-1] for d in dirs if d['type'] == 'directory' and d['name'].split('/')[-1].startswith('state=')]
        return sorted(states)

    def _normalize_param(self, param, all_options):
        if param == 'all':
            return all_options
        if isinstance(param, (list, range)):
            return list(param)
        return [param]

    def query(self, year: Union[int, List[int], range, str], state: Union[str, List[str], None] = None, limit: int = 1, field_filters: Optional[dict] = None) -> pd.DataFrame:
        """
        Query the Tracking-the-Sun dataset for given year(s) and state(s).
        Accepts single values, lists, ranges, or 'all' for both year and state.
        """
        # Discover all years/states if needed
        all_years = self._list_years()
        years = self._normalize_param(year, all_years)
        dfs = []
        for y in years:
            all_states = self._list_states(y)
            states = self._normalize_param(state, all_states) if state is not None else [None]
            for s in states:
                prefix = f"{self.base_prefix}/{y}"
                if s:
                    prefix += f"/state={s}"
                s3_path = f"{self.bucket}/{prefix}"
                files = self.fs.glob(f"{s3_path}/*.parquet")
                if not files:
                    print(f"No Parquet files found for query: {prefix}")
                    continue
                print(f"Year {y}, State {s}: Found {len(files)} files. Loading up to {limit}...")
                iterator = tqdm(files[:limit], desc=f"Loading files {y}-{s}") if _HAS_TQDM else files[:limit]
                for file in iterator:
                    if not _HAS_TQDM:
                        print(f"Loading {file} ...")
                    with self.fs.open(file, "rb") as f:
                        dfs.append(pd.read_parquet(f, engine="pyarrow"))
        if not dfs:
            raise FileNotFoundError("No Parquet files found for the given query parameters.")
        print("Concatenating data frames...")
        df = pd.concat(dfs, ignore_index=True)
        if field_filters:
            print("Applying filters...")
            if _HAS_TQDM:
                for field, (op, value) in tqdm(field_filters.items(), desc="Filtering"):
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
            else:
                for field, (op, value) in field_filters.items():
                    print(f"Filtering {field} {op} {value}")
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
        print("Done.")
        return df

    def get_fields(self, year: Union[int, List[int], range, str], state: Union[str, List[str], None] = None) -> List[str]:
        """
        Return the list of fields (columns) in the dataset for the given query.
        For multiple years/states, returns the fields from the first available file.
        """
        all_years = self._list_years()
        years = self._normalize_param(year, all_years)
        for y in years:
            all_states = self._list_states(y)
            states = self._normalize_param(state, all_states) if state is not None else [None]
            for s in states:
                prefix = f"{self.base_prefix}/{y}"
                if s:
                    prefix += f"/state={s}"
                s3_path = f"{self.bucket}/{prefix}"
                files = self.fs.glob(f"{s3_path}/*.parquet")
                if files:
                    with self.fs.open(files[0], "rb") as f:
                        df = pd.read_parquet(f, engine="pyarrow")
                    return list(df.columns)
        raise FileNotFoundError("No Parquet files found for the given query parameters.")

    def count_rows(self, year: Union[int, List[int], range, str], state: Union[str, List[str], None] = None, max_files: Optional[int] = None) -> int:
        """
        Return the total number of rows for the given query. Optionally limit the number of files scanned per year/state.
        """
        all_years = self._list_years()
        years = self._normalize_param(year, all_years)
        total_rows = 0
        for y in years:
            all_states = self._list_states(y)
            states = self._normalize_param(state, all_states) if state is not None else [None]
            for s in states:
                prefix = f"{self.base_prefix}/{y}"
                if s:
                    prefix += f"/state={s}"
                s3_path = f"{self.bucket}/{prefix}"
                files = self.fs.glob(f"{s3_path}/*.parquet")
                if not files:
                    continue
                if max_files:
                    files = files[:max_files]
                for file in files:
                    with self.fs.open(file, "rb") as f:
                        pf = pd.read_parquet(f, engine="pyarrow")
                        total_rows += len(pf)
        return total_rows

    def print_summary(self, year: Union[int, List[int], range, str], state: Union[str, List[str], None] = None, max_files: Optional[int] = None):
        """
        Print a summary of the dataset for the given query: fields and row count.
        """
        try:
            fields = self.get_fields(year, state)
            print(f"Fields: {fields}")
            nrows = self.count_rows(year, state, max_files=max_files)
            print(f"Total rows: {nrows}")
        except Exception as e:
            print(f"Error: {e}") 