# ☀️ suntrack-py: Tracking-the-Sun Data Client

A lightweight Python library for querying the publicly available “Tracking‑the‑Sun” dataset stored in an S3 data lake of Parquet files. This importable module (`tts_data_client`) abstracts away all S3 key patterns and file I/O so you can focus on analysis by passing simple parameters.

---

## 🚀 Features

- 🔓 **Anonymous S3 access** (no AWS credentials needed) via `s3fs`
- 🗂️ **Flexible filtering** by partition fields: `year`, `state`, `technology`, etc.
- 🦥 **Lazy loading**—only downloads the Parquet files you request
- 📊 **pandas.DataFrame** output for immediate data manipulation
- 🔍 **Automatic discovery** of available partitions
- 🧮 **Flexible in-memory filtering** (e.g., `system_size > 5000`)

---

## 📦 Installation

First, create and activate a virtual environment (recommended):

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Then install dependencies:

```bash
pip install -r requirements.txt
```

---

## 🛠️ Basic Usage

### 1. Initialize the client
```python
from tts_data_client import TTSClient
client = TTSClient()
```

### 2. Query 2019 California data
```python
df_ca2019 = client.query(year=2019, state="CA")
print(df_ca2019.head())
```

### 3. Further filter by technology
```python
df_ca2019_csp = client.query(year=2019, state="CA", technology="CSP")
print(df_ca2019_csp.head())
```

### 4. Flexible field filtering (e.g., system size > 5000)
```python
df_large = client.query(year=2019, state="CA", field_filters={"system_size": (">", 5000)})
print(df_large.head())
```

---

## 🧰 Helper Methods

- **List available fields:**
    ```python
    fields = client.get_fields(year=2019, state="CA")
    print(fields)
    ```
- **Count data points:**
    ```python
    nrows = client.count_rows(year=2019, state="CA")
    print(nrows)
    ```
- **Print a summary:**
    ```python
    client.print_summary(year=2019, state="CA")
    ```

---

## 📂 Example Scripts

All example scripts are in the `examples/` folder. Run them from the project root, e.g.:

```bash
python examples/example_query_system_size.py
```

---

## 🌎 Data Source

Data is fetched directly from the [OEDI Tracking-the-Sun S3 data lake](https://oedi-data-lake.s3.amazonaws.com/tracking-the-sun/).

---

## 📝 License

MIT License. See [LICENSE](LICENSE) for details.

---

## ✨ Contributions

PRs and issues welcome! Let’s make solar data more accessible together. 