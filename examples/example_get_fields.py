import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# To run this script, use: python examples/example_get_fields.py from the project root
from tts_data_client import TTSClient

if __name__ == "__main__":
    client = TTSClient()
    fields = client.get_fields(year=2019, state="CA")
    print("Available fields:")
    for field in fields:
        print(field) 