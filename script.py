import time
import io
import requests
import pandas as pd
from premav import clean_premav_data, save_to_mysql

api_url = "https://redcap.rbc.gov.rw/api/"
api_token = ""

def fetch_data_in_batches(batch_size=100):
    # Get IDs
    id_payload = {'token': api_token, 'content': 'record', 'format': 'csv', 'fields[0]': 'record_id'}
    r = requests.post(api_url, data=id_payload)
    all_ids = pd.read_csv(io.StringIO(r.text))['record_id'].unique().tolist()
    
    print(f"📦 REDCap Records Found: {len(all_ids)}")
    
    chunks = []
    for i in range(0, len(all_ids), batch_size):
        batch = all_ids[i:i + batch_size]
        print(f"⏳ Downloading batch {i//batch_size + 1}...")
        
        payload = {
            'token': api_token, 'content': 'record', 'format': 'csv',
            'type': 'flat', 'rawOrLabel': 'label', 'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'true'
        }
        for idx, r_id in enumerate(batch):
            payload[f'records[{idx}]'] = r_id
            
        res = requests.post(api_url, data=payload, timeout=120)
        chunks.append(pd.read_csv(io.StringIO(res.text)))
        
    return pd.concat(chunks, ignore_index=True)

if __name__ == "__main__":
    while True:
        print("\n🚀 Starting Sync Cycle...")
        try:
            df_raw = fetch_data_in_batches(batch_size=100)
            if not df_raw.empty:
                df_clean = clean_premav_data(df_raw)
                save_to_mysql(df_clean, "premav", "record_id")
        except Exception as e:
            print(f"❌ Error: {e}")
            
        print("⏰ Sleeping for 30 seconds...")
        time.sleep(30)