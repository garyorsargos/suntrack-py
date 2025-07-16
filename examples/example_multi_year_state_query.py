import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# To run this script, use: python examples/example_multi_year_state_query.py from the project root or python example_multi_year_state_query.py from within examples/
from tts_data_client import TTSClient

if __name__ == "__main__":
    client = TTSClient()
    df = client.query(year=[2018, 2019], state=["CA", "AZ"])
    print(df.head())
    print(f"Total results: {len(df)}") 