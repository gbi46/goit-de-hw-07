"""Import a medal CSV; preserve an already populated table."""
import csv
import io
import os
import re
from pathlib import Path
from urllib.request import urlopen
import pymysql

def main():
    connection = pymysql.connect(host="mysql", user="medals", password="medals",
        database="olympic_dataset", charset="utf8mb4", autocommit=False)
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE 'athlete_event_results'")
            if cursor.fetchone():
                cursor.execute("SELECT COUNT(*) FROM athlete_event_results")
                count = cursor.fetchone()[0]
                if count:
                    print(f"Dataset already loaded: {count} rows", flush=True)
                    return
            local_file = Path("/data/athlete_event_results.csv")
            if local_file.exists():
                content = local_file.read_bytes()
            else:
                url = os.environ.get("DATA_URL", "your_data_url").strip()
                if not url or url == "your_data_url":
                    raise ValueError(
                        "Set DATA_URL in .env to a CSV URL, or provide "
                        "data/athlete_event_results.csv"
                    )
                print(f"Downloading {url}", flush=True)
                with urlopen(url, timeout=120) as response:
                    content = response.read()
            reader = csv.reader(io.StringIO(content.decode("utf-8-sig")))
            headers = next(reader)
            if "medal" not in headers or len(headers) != len(set(headers)):
                raise ValueError(f"Invalid CSV headers: {headers}")
            if not all(re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", name) for name in headers):
                raise ValueError("Unsafe CSV column name")
            cursor.execute("DROP TABLE IF EXISTS athlete_event_results_loading")
            columns = ", ".join(f"`{name}` TEXT" for name in headers)
            cursor.execute(f"CREATE TABLE athlete_event_results_loading ({columns})")
            placeholders = ", ".join(["%s"] * len(headers))
            query = f"INSERT INTO athlete_event_results_loading VALUES ({placeholders})"
            batch = []
            count = 0
            for row in reader:
                if not row:
                    continue
                if len(row) != len(headers):
                    raise ValueError(f"Invalid CSV row {reader.line_num}")
                batch.append(row)
                if len(batch) == 1000:
                    cursor.executemany(query, batch)
                    count += len(batch)
                    batch.clear()
            if batch:
                cursor.executemany(query, batch)
                count += len(batch)
            if not count:
                raise ValueError("CSV contains no data")
            cursor.execute("SELECT COUNT(*) FROM athlete_event_results_loading WHERE medal IN ('Bronze', 'Silver', 'Gold')")
            if not cursor.fetchone()[0]:
                raise ValueError("CSV contains no medal records")
            connection.commit()
            cursor.execute("DROP TABLE IF EXISTS athlete_event_results")
            cursor.execute("RENAME TABLE athlete_event_results_loading TO athlete_event_results")
            print(f"Imported {count} rows", flush=True)

if __name__ == "__main__":
    main()
