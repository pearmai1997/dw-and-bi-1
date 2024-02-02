from typing import NewType

import psycopg2


PostgresCursor = NewType("PostgresCursor", psycopg2.extensions.cursor)
PostgresConn = NewType("PostgresConn", psycopg2.extensions.connection)

table_drop_event = "DROP TABLE IF EXISTS event"
table_drop_actor = "DROP TABLE IF EXISTS actor"
table_drop_repo = "DROP TABLE IF EXISTS repo"
table_drop_org = "DROP TABLE IF EXISTS org"
table_drop_payload = "DROP TABLE IF EXISTS payload"
table_drop_commit = "DROP TABLE IF EXISTS commit"

table_create_actor = """
    CREATE TABLE IF NOT EXISTS actor (
        actor_id int,
        login varchar(100) NOT NULL,
        gravatar_id varchar(100),
        avatar_url varchar(100),
        PRIMARY KEY(actor_id)
    )
"""

table_create_org = """
    CREATE TABLE IF NOT EXISTS org (
        org_id int,
        login varchar(100) NOT NULL,
        gravatar_id varchar(100),
        avatar_url varchar(100),
        PRIMARY KEY(org_id)
    )
"""

table_create_repo = """
    CREATE TABLE IF NOT EXISTS repo (
        repo_id int,
        name varchar(100) NOT NULL,
        url varchar(100),
        PRIMARY KEY(repo_id)
    )
"""

table_create_commit = """
    CREATE TABLE IF NOT EXISTS commit (
        commit_id varchar(100) PRIMARY KEY,
        name varchar(100) NOT NULL,
        "distinct" varchar(100)
    )
"""
table_create_payload = """
    CREATE TABLE IF NOT EXISTS payload (
        payload_push_id int,
        action varchar(100) NOT NULL,
        node_id varchar(100),
        type varchar(100),
        number int,
        title varchar(100),
        commit_id varchar(100),
        PRIMARY KEY(payload_push_id),
        CONSTRAINT fk_commit FOREIGN KEY(commit_id) REFERENCES commit(commit_id)
    )
"""

table_create_event = """
    CREATE TABLE IF NOT EXISTS event (
        event_id varchar(100) NOT NULL,
        event_type varchar(100) NOT NULL,
        event_url varchar(100) NOT NULL,
        public boolean NOT NULL,
        create_at timestamp NOT NULL,
        actor_id int,
        repo_id int,
        payload_push_id int,
        org_id int,
        PRIMARY KEY(event_id),
        CONSTRAINT fk_actor FOREIGN KEY(actor_id) REFERENCES actor(actor_id),
        CONSTRAINT fk_repo FOREIGN KEY(repo_id) REFERENCES repo(repo_id),
        CONSTRAINT fk_payload FOREIGN KEY(payload_push_id) REFERENCES payload(payload_push_id),
        CONSTRAINT fk_org FOREIGN KEY(org_id) REFERENCES org(org_id)
    )
"""



create_table_queries = [
    table_create_actor,
    table_create_org,
    table_create_repo,
    table_create_commit,
    table_create_payload,
    table_create_event
]

drop_table_queries = [
    table_drop_event,
    table_drop_actor,
    table_drop_org,
    table_drop_repo,
    table_drop_payload,
    table_drop_commit
]


def drop_tables(cur: PostgresCursor, conn: PostgresConn) -> None:
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur: PostgresCursor, conn: PostgresConn) -> None:
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database.
    - Establishes connection with the sparkify database and gets
    cursor to it.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes the connection.
    """
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
