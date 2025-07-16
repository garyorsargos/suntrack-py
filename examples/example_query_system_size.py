import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# To run this script, use: python examples/example_query_system_size.py from the project root
from tts_data_client import TTSClient

if __name__ == "__main__":
    client = TTSClient()
    df = client.query(year=2019, state="CA", field_filters={"system_size": (">", 4000)})
    print(df.head())
    print(f"Total results: {len(df)}") 