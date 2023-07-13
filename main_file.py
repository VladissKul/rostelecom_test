import time
import random
import requests
import sqlite3

DB_NAME = 'data.db'

SOURCE_1 = 'https://random-data-api.com/api/v2/banks'
SOURCE_2 = 'https://random-data-api.com/api/v2/beers'
DATA_RANGE = (10, 20)

ETL_INTERVALS = [(5, 10), (15, 20)]
MERGE_WAIT_TIME = 5


def generate_data(source_url, num_records):
    response = requests.get(source_url, params={'size': num_records})
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data from {source_url}")


def create_table(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            uid INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT,
            load_time DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()


def create_metadata_table(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            uid INTEGER PRIMARY KEY AUTOINCREMENT,
            source1_id TEXT,
            source2_id TEXT,
            merge_id TEXT,
            load_time DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()


def create_merge_table(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            uid INTEGER PRIMARY KEY AUTOINCREMENT,
            data1 TEXT,
            data2 TEXT,
            load_time DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (uid, data1, data2, load_time)
        )
    """)
    conn.commit()


def insert_data(conn, table_name, data):
    cursor = conn.cursor()
    for record in data:
        values = (str(record),)
        cursor.execute(f"INSERT INTO {table_name} (data) VALUES (?)", values)
    conn.commit()


def merge_data(conn, table1, table2, merge_table):
    cursor = conn.cursor()

    cursor.execute(f"SELECT COUNT(*) FROM {merge_table}")
    merge_data_count = cursor.fetchone()[0]

    if merge_data_count == 0:
        cursor.execute(f"""
            INSERT INTO {merge_table} (data1, data2)
            SELECT {table1}.data, {table2}.data
            FROM {table1}
            INNER JOIN {table2} ON {table1}.uid = {table2}.uid
        """)
    else:
        cursor.execute(f"""
            INSERT OR IGNORE INTO {merge_table} (data1, data2)
            SELECT {table1}.data, {table2}.data
            FROM {table1}
            INNER JOIN {table2} ON {table1}.uid = {table2}.uid
            WHERE {table1}.uid > (SELECT MAX(uid) FROM {merge_table})
        """)

    conn.commit()


def create_etl_metadata(conn, source1_id, source2_id, merge_id):
    cursor = conn.cursor()
    cursor.execute(f"""
        INSERT INTO etl_metadata (source1_id, source2_id, merge_id)
        VALUES (?, ?, ?)
    """, (source1_id, source2_id, merge_id))
    conn.commit()


def main():
    conn = sqlite3.connect(DB_NAME)
    create_table(conn, 'source1')
    create_table(conn, 'source2')
    create_merge_table(conn, 'merge_data')
    create_metadata_table(conn, 'etl_metadata')

    while True:
        num_records = random.randint(*DATA_RANGE)

        source1_data = generate_data(SOURCE_1, num_records)
        source1_id = None

        source2_data = generate_data(SOURCE_2, num_records)
        source2_id = None

        insert_data(conn, 'source1', [{'data': str(record)} for record in source1_data])
        insert_data(conn, 'source2', [{'data': str(record)} for record in source2_data])

        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM source1 WHERE uid = (SELECT MAX(uid) FROM source1)")
        source1_count = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM source2 WHERE uid = (SELECT MAX(uid) FROM source2)")
        source2_count = cursor.fetchone()[0]

        if source1_count > 0 and source2_count > 0:
            merge_id = str(time.time())
            merge_data(conn, 'source1', 'source2', 'merge_data')

            create_etl_metadata(conn, source1_id, source2_id, merge_id)

        time.sleep(random.randint(MERGE_WAIT_TIME, MERGE_WAIT_TIME))

        etl_interval = random.choice(ETL_INTERVALS)
        time.sleep(random.randint(etl_interval[0], etl_interval[1]))


if __name__ == '__main__':
    main()
