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
def search_and_export(db_url, output_name, webhook_url):
    # 1. Read the search parameters from the config file
    with open('search_config.json', 'r') as f:
        config = json.load(f)
        
    min_chests = config['min_chests']
    search_diameter = config.get('search_diameter', 4) # Defaults to 4 if missing
    c1 = config['corner1']
    c2 = config['corner2']

    # Math: (Chunks * 16 blocks per chunk) / 2 = Radius.
    radius = (search_diameter * 16) / 2
    squared_radius = int(radius ** 2)

    
    # Automatically figure out the true Min and Max, regardless of how you typed them
    min_x, max_x = min(c1['x'], c2['x']), max(c1['x'], c2['x'])
    min_z, max_z = min(c1['z'], c2['z']), max(c1['z'], c2['z'])

    print(f"\nSearching for {min_chests}+ chests between X:[{min_x} to {max_x}] and Z:[{min_z} to {max_z}]...")
    engine = create_engine(db_url)
    
        # 2. The targeted radius query
    query = f"""
    WITH target_blocks AS (
        -- Step 1: Filter out everything except chests/shulkers to speed up math
        SELECT b.x, b.z
        FROM blocks b
        JOIN block_states bs ON b.blockstate_id = bs.id
        WHERE bs.block_name LIKE '%%chest%%' OR bs.block_name LIKE '%%shulker_box%%'
        AND b.x BETWEEN {min_x} AND {max_x}
        AND b.z BETWEEN {min_z} AND {max_z}
    )
    -- Step 2: For every chest, count how many chests are within the radius
    SELECT 
        t1.x, 
        t1.z, 
        COUNT(t2.x) as chest_count
    FROM target_blocks t1
    JOIN target_blocks t2 
        -- Bounding box check (fast)
        ON t2.x BETWEEN t1.x - {radius} AND t1.x + {radius}
        AND t2.z BETWEEN t1.z - {radius} AND t1.z + {radius}
        -- True radius math check (accurate)
        AND POWER(t1.x - t2.x, 2) + POWER(t1.z - t2.z, 2) <= {squared_radius}
    GROUP BY t1.x, t1.z
    HAVING COUNT(t2.x) >= {min_chests}
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
    DB_URL = os.getenv('DB_URL')
    WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')

    if not DB_URL:
        print("Error: Missing DB_URL in .env file.")
        exit(1)

    # 1. Data Paths (Ensure these point to your external drive)
    BLOCKS_SQL_PATH = '/Volumes/ExtDrive/NocomData/nocom/blocks.sql'
    STATES_SQL_PATH = '/Volumes/ExtDrive/NocomData/nocom/block_states.sql'
    FEATHER_DIR = './feather_data'

    # 2. Extract and chunk the raw text files to Feather
    raw_to_feather(BLOCKS_SQL_PATH, 'blocks', ['x', 'y', 'z', 'blockstate_id'], FEATHER_DIR)
    raw_to_feather(STATES_SQL_PATH, 'block_states', ['id', 'block_name'], FEATHER_DIR)

    # 3. Load Feather chunks into PostgreSQL
    feather_to_postgres(FEATHER_DIR, DB_URL, 'blocks')
    feather_to_postgres(FEATHER_DIR, DB_URL, 'block_states')

    # 4. Run the targeted search!
    search_and_export(DB_URL, output_name='targeted_stashes', webhook_url=WEBHOOK_URL)
