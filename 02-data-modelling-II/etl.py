import glob
import json
import os
from typing import List

from cassandra.cluster import Cluster


table_drop_events = "DROP TABLE events"
table_drop_actor = "DROP TABLE actor"
table_drop_repo = "DROP TABLE repo"

table_create_events = """
    CREATE TABLE IF NOT EXISTS events
    (
        event_id varchar,
        type varchar,
        public boolean,
        created_at timestamp,
        actor_id varchar,
        actor_login varchar,
        actor_display_login varchar,
        actor_url varchar,
        repo_id varchar,
        repo_name varchar,
        repo_url varchar,
        PRIMARY KEY (
            event_id,
            type
        )
    )
"""

table_create_actor = """
    CREATE TABLE IF NOT EXISTS actor
    (
        actor_id varchar,
        actor_login varchar,
        PRIMARY KEY (
            actor_id,
            actor_login
        )
    )
"""
table_create_repo = """
    CREATE TABLE IF NOT EXISTS repo
    (
        repo_id varchar,
        repo_name varchar,
        PRIMARY KEY (
        repo_id,
        repo_name
        )
    )
"""



create_table_queries = [
    table_create_events,
    table_create_actor,
    table_create_repo
]
drop_table_queries = [
    table_drop_events,
    table_drop_actor,
    table_drop_repo
]

def drop_tables(session):
    for query in drop_table_queries:
        try:
            rows = session.execute(query)
        except Exception as e:
            print(e)


def create_tables(session):
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)


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


def process(session, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                print(each["id"], each["type"], each["actor"]["login"])

                # Insert data into tables here
                insert_statement = f"""
                INSERT INTO events (
                event_id,
                type,
                public,
                created_at,
                actor_id,
                actor_login,
                actor_display_login,
                actor_url,
                repo_id,
                repo_name,
                repo_url
                ) VALUES ('{each["id"]}', '{each["type"]}', {each["public"]}, '{each["created_at"]}',
                '{each["actor"]["id"]}', '{each["actor"]["login"]}', '{each["actor"]["display_login"]}', '{each["actor"]["url"]}',
                '{each["repo"]["id"]}', '{each["repo"]["name"]}', '{each["repo"]["url"]}')
                """
                session.execute(insert_statement)


                insert_statement = f"""
                INSERT INTO actor (
                actor_id,
                actor_login
                ) VALUES ('{each["actor"]["id"]}','{each["actor"]["login"]}')
                """
                session.execute(insert_statement)


                insert_statement = f"""
                INSERT INTO repo (
                repo_id,
                repo_name
                ) VALUES ('{each["repo"]["id"]}','{each["repo"]["name"]}')
                """
                session.execute(insert_statement)


# def insert_sample_data(session):
#     query = f"""
#     INSERT INTO events (id, type, public) 
#     VALUES ('23487929637', 'IssueCommentEvent', true)
#     """
#     session.execute(query)


def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    # Create keyspace
    try:
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS github_events
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
            """
        )
    except Exception as e:
        print(e)

    # Set keyspace
    try:
        session.set_keyspace("github_events")
    except Exception as e:
        print(e)

    drop_tables(session)
    create_tables(session)
    process(session, filepath="../data")
    #insert_sample_data(session)

    # Select data in Cassandra and print them to stdout
    query = """
    SELECT event_id, actor_id, created_at FROM events 
    WHERE created_at >= '2022-08-17T15:51:04Z' 
    AND created_at <= '2022-08-17T15:51:05Z'
    AND type = 'IssuesEvent'
    ALLOW FILTERING;
    """

    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()