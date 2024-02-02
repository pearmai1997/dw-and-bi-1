import glob
import json
import os
from typing import List

import psycopg2


def get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files


def process(cur, conn, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                print(
                    each["id"], 
                    each["type"],
                    each["actor"]["id"],
                    each["actor"]["login"],
                    each["repo"]["id"],
                    each["repo"]["name"],
                    each["created_at"],
                )

                try:
                    # Insert data into tables here
                    insert_statement = f"""
                        INSERT INTO actor (
                            actor_id,
                            login,
                            gravatar_id,
                            avatar_url
                        ) VALUES ('{each["actor"]["id"]}', '{each["actor"]["login"]}','{each["actor"].get("gravatar_id", "")}'
                        ,'{each["actor"].get("avatar_url", "")}')
                        ON CONFLICT (actor_id) DO NOTHING
                    """
                    cur.execute(insert_statement)
                except Exception as e:
                    print(f"Error inserting into actor table: {e}")

                try:
                    # Insert data into tables here
                    insert_statement = f"""
                        INSERT INTO org (
                            org_id,
                            login,
                            gravatar_id,
                            avatar_url
                        ) VALUES ('{each["org"]["id"]}', '{each["org"]["login"]}','{each["org"]['gravatar_id']}'
                        ,'{each["org"]["avatar_url"]}')
                        ON CONFLICT (org_id) DO NOTHING
                    """
                    cur.execute(insert_statement)
                except Exception as e:
                    print(f"Error inserting into org table: {e}")

                try:
                    # Insert data into tables here
                    insert_statement = f"""
                        INSERT INTO repo (
                            repo_id,
                            name,
                            url
                        ) VALUES ('{each["repo"]["id"]}', '{each["repo"]["name"]}','{each["repo"]["url"]}')
                        ON CONFLICT (repo_id) DO NOTHING
                    """
                    cur.execute(insert_statement)
                except Exception as e:
                    print(f"Error inserting into repo table: {e}")

                try:
                    # Insert data into tables here
                    insert_statement = f"""
                        INSERT INTO payload (
                            payload_push_id,
                            action,
                            node_id,
                            type,
                            number,
                            title,
                            commits_sha
                        ) VALUES ('{each["payload"]["push_id"]}', '{each["payload"]["action"]}','{each["payload"]["node_id"]}',
                        '{each["payload"].get("type", "")}','{each["payload"].get("number", 0)}','{each["payload"].get("title", "")}'
                        ,'{each["commits"]["sha"]}')
                        ON CONFLICT (payload_push_id) DO NOTHING
                    """
                    cur.execute(insert_statement)
                except Exception as e:
                    print(f"Error inserting into payload table: {e}")

                try:
                    # Insert data into tables here
                    insert_statement = f"""
                        INSERT INTO commits (
                            commits_sha,
                            name,
                            distinct
                        ) VALUES ('{each["commits"]["sha"]}', '{each["commits"]["name"]}', '{each["commits"]["distinct"]})
                        ON CONFLICT (commits_sha) DO NOTHING
                    """
                    cur.execute(insert_statement)
                except Exception as e:
                    print(f"Error inserting into commits table: {e}")

                try:
                    # Insert data into tables here
                    insert_statement = f"""
                        INSERT INTO event (
                            event_id,
                            event_type,
                            event_url,
                            public,
                            created_at
                        ) VALUES ('{each["id"]}', '{each["type"]}','{each["events_url"]}'
                        ,'{each["public"]}','{each["created_at"]}',
                        '{each["commits"]["sha"]}','{each["payload"]["push_id"]}','{each["repo"]["id"]}','{each["org"]["id"]}','{each["actor"]["id"]}')
                        ON CONFLICT (event_id) DO NOTHING
                    """
                    cur.execute(insert_statement)
                except Exception as e:
                    print(f"Error inserting into event table: {e}")

                conn.commit()


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    process(cur, conn, filepath="../data")

    conn.close()


if __name__ == "__main__":
    main()