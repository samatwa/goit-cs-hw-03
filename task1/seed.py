import psycopg2 as psycopg
from connect import create_connection, database_url
from contextlib import contextmanager
from faker import Faker
import random


def create_users(conn, num_users=10):
    fake = Faker()
    with conn.cursor() as cur:
        for _ in range(num_users):
            fullname = fake.name()
            email = fake.email()
            cur.execute(
                "INSERT INTO users (fullname, email) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (fullname, email)
            )


def create_tasks(conn, num_tasks=8):
    fake = Faker()
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM users")
        user_ids = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT id FROM status")
        status_ids = [row[0] for row in cur.fetchall()]

        for _ in range(num_tasks):
            title = fake.paragraph(nb_sentences=1)
            description = fake.bs()
            status_id = random.choice(status_ids)
            user_id = random.choice(user_ids)
            cur.execute(
                "INSERT INTO tasks (title, description, status_id, user_id) VALUES (%s, %s, %s, %s)",
                (title, description, status_id, user_id)
            )


def create_status(conn):
    statuses = [('new',), ('in progress',), ('completed',)]
    with conn.cursor() as cur:
        for status in statuses:
            cur.execute(
                "INSERT INTO status (name) VALUES (%s) ON CONFLICT DO NOTHING",
                status
            )


if __name__ == '__main__':
    with create_connection(database_url) as conn:
        if conn is not None:
            create_users(conn)
            create_status(conn)
            create_tasks(conn)
        else:
            print("Error! cannot create the database connection.")
