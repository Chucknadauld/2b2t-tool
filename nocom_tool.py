import pandas as pd
import pyarrow.feather as feather
import os
import re
import requests
import json
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Step 1: Optimized raw SQL dump parser
def parse_sql_dump(filepath, table_name):
    """
    Generator that yields batches of rows from a raw SQL dump.
    Optimized to read massive files line-by-line without filling RAM.
    """
    print(f"Reading {filepath}...")
    # Regex to capture the values inside the parentheses of an INSERT statement
    pattern = re.compile(r"\((.*?)\)") 
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if line.startswith(f"INSERT INTO `{table_name}`") or line.startswith(f"INSERT INTO {table_name}"):
                # Find all (x, y, z, id) blocks in this specific INSERT line
                matches = pattern.findall(line)
                for match in matches:
                    # Split by comma and strip any rogue quotes/spaces
                    yield [val.strip().strip("'") for val in match.split(',')]

# Step 2, 3 & 4: Chunk data and save to Feather format for ultra-fast I/O
def raw_to_feather(input_file, table_name, columns, output_dir, chunksize=500000):
    os.makedirs(output_dir, exist_ok=True)
    chunk_index = 0
    data = []
    
    print(f"Extracting '{table_name}' and saving to Feather chunks...")
    for row in parse_sql_dump(input_file, table_name):
        data.append(row)
        
        # When we hit chunksize, write to a feather file and clear RAM
        if len(data) >= chunksize:
            df = pd.DataFrame(data, columns=columns)
            feather_path = os.path.join(output_dir, f'{table_name}_chunk_{chunk_index}.feather')
            feather.write_feather(df, feather_path)
            print(f"Saved {feather_path}")
            data = []
            chunk_index += 1
            
    # Save the final remainder chunk
    if data:
        df = pd.DataFrame(data, columns=columns)
        feather_path = os.path.join(output_dir, f'{table_name}_chunk_{chunk_index}.feather')
        feather.write_feather(df, feather_path)
        print(f"Saved final chunk: {feather_path}")

# Step 5 & 6: Load Feather chunks directly into PostgreSQL
def feather_to_postgres(feather_dir, db_url, table_name):
    print(f"Connecting to PostgreSQL to load {table_name}...")
    engine = create_engine(db_url)
    
    for file in sorted(os.listdir(feather_dir)):
        if file.startswith(table_name) and file.endswith('.feather'):
            filepath = os.path.join(feather_dir, file)
            df = feather.read_feather(filepath)
            
            # Clean data types: Convert strings to numeric where applicable
            df = df.apply(pd.to_numeric, errors='ignore').dropna()
            
            # Append to database
            df.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"Loaded {file} into PostgreSQL.")

# Send Waypoints to Discord
def send_to_discord(webhook_url, file_path, stash_count, min_chests):
    message = f"🚨 **NoCom Scan Complete!** Found **{stash_count}** massive stashes ({min_chests}+ chests). Drop the attached file into both of your Prism Launcher `.minecraft/XaeroWaypoints/` folders!"
    
    with open(file_path, 'rb') as f:
        files = {'file': (os.path.basename(file_path), f, 'text/plain')}
        payload = {'content': message}
        requests.post(webhook_url, files=files, data={'payload_json': json.dumps(payload)})
    print("Sent Xaero waypoints to Discord Webhook.")

# Step 7, 8, 9 & 10: PostgreSQL Query -> CSV + Xaero Waypoints -> Discord
def search_and_export(db_url, min_chests, output_name, webhook_url):
    print(f"\nExecuting SQL Query for {min_chests}+ chests...")
    engine = create_engine(db_url)
    
    # Query groups chests by 16x16 chunk grid to find clustered stash bases
    query = f"""
    SELECT 
        FLOOR(b.x/16)*16 AS x, 
        FLOOR(b.z/16)*16 AS z, 
        COUNT(*) as chest_count
    FROM blocks b
    JOIN block_states bs ON b.blockstate_id = bs.id
    WHERE bs.block_name LIKE '%%chest%%' OR bs.block_name LIKE '%%shulker_box%%'
    GROUP BY FLOOR(b.x/16)*16, FLOOR(b.z/16)*16
    HAVING COUNT(*) >= {min_chests}
    ORDER BY chest_count DESC;
    """
    
    results_df = pd.read_sql(query, engine)
    stash_count = len(results_df)
    
    if stash_count == 0:
        print("No stashes found matching that criteria.")
        return

    # 1. Export CSV
    csv_path = f"{output_name}.csv"
    results_df.to_csv(csv_path, index=False)
    print(f"Exported {stash_count} results to {csv_path} for Excel.")
    
    # 2. Export Xaero Waypoints
    xaero_path = f"{output_name}_xaero.txt"
    with open(xaero_path, 'w') as f:
        for index, row in results_df.iterrows():
            f.write(f"waypoint:Stash_{int(row['chest_count'])}Chests:S:{int(row['x'])}:64:{int(row['z'])}:1:false:0:Internal-overworld-waypoints:false:0:0\n")
    print(f"Exported Xaero Waypoints to {xaero_path}.")

    # 3. Webhook
    if webhook_url:
        send_to_discord(webhook_url, xaero_path, stash_count, min_chests)


if __name__ == "__main__":
    DB_URL = os.getenv("DB_URL")
    WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
    
    if not DB_URL:
        print("Error: Missing DB_URL in .env file.")
        exit(1)
        
    # --- PIPELINE ---
    # Uncomment Step 1 and Step 2 ONCE when the download finishes. 
    # Once the data is in PostgreSQL, you can comment them back out.

    # Data Paths (Ensure these match where you extracted the files on your external drive)
    BLOCKS_SQL_PATH = '/Volumes/ExtDrive/NocomData/blocks.sql'
    STATES_SQL_PATH = '/Volumes/ExtDrive/NocomData/block_states.sql'

    # STEP 1: Parse Text Files -> Save as Feather
    # raw_to_feather(STATES_SQL_PATH, 'block_states', ['id', 'block_name'], 'feather_chunks')
    # raw_to_feather(BLOCKS_SQL_PATH, 'blocks', ['x', 'y', 'z', 'blockstate_id'], 'feather_chunks')
    
    # STEP 2: Load Feather -> Push to PostgreSQL
    # feather_to_postgres('feather_chunks', DB_URL, 'block_states')
    # feather_to_postgres('feather_chunks', DB_URL, 'blocks')
    
    # STEP 3: Search Database -> Export -> Discord
    search_and_export(DB_URL, min_chests=20, output_name='massive_stashes', webhook_url=WEBHOOK_URL)
