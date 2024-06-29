import psycopg2 as psycopg
from psycopg2 import Error
from faker import Faker
from connect import create_connection, database_url


def select_tasks_by_user(conn, user_id):
    """
    Query all tasks for a specific user
    :param conn: the Connection object
    :param user_id: the user ID
    :return: rows of tasks
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT title, description FROM tasks WHERE user_id = %s", (user_id,))
        rows = cur.fetchall()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows


def select_tasks_by_status(conn, status_name):
    """
    Query tasks by status name
    :param conn: the Connection object
    :param status_name: the name of the status
    :return: rows of tasks
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT * FROM tasks WHERE status_id = (
                SELECT id FROM status WHERE name = %s
            )
        """, (status_name,))
        rows = cur.fetchall()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows


def update_task_status(conn, task_id, new_status_name):
    """
    Update the status of a specific task
    :param conn: the Connection object
    :param task_id: the ID of the task to update
    :param new_status_name: the new status name
    """
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE tasks SET status_id = (
                SELECT id FROM status WHERE name = %s
            ) WHERE id = %s
        """, (new_status_name, task_id))
        conn.commit()
        print(f"Updated task_id = {task_id} to status = {new_status_name}")
    except Error as e:
        print(e)
    finally:
        cur.close()


def select_users_without_tasks(conn):
    """
    Query users who do not have any tasks
    :param conn: the Connection object
    :return: rows of users
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, fullname FROM users WHERE id NOT IN (
                SELECT user_id FROM tasks
            )
        """)
        rows = cur.fetchall()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows


def insert_new_task(conn, user_id):
    """
    Insert a new task for a specific user
    :param conn: the Connection object
    :param title: the title of the new task
    :param description: the description of the new task
    :param status_name: the status name of the new task
    :param user_id: the user ID to assign the task to
    """
    fake = Faker()
    title = fake.sentence(nb_words=4)
    description = fake.paragraph(nb_sentences=1)
    status_id = 1

    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM users WHERE id = %s", (user_id,))
        if cur.fetchone() is None:
            print(f"No user found with user_id = {user_id}")
            return

        cur.execute("""
            INSERT INTO tasks (title, description, status_id, user_id)
            VALUES (%s, %s,  %s, %s)      
        """, (title, description, status_id, user_id))
        conn.commit()
        print(f"Inserted new task for user_id = {user_id}")
    except Error as e:
        print(e)
    finally:
        cur.close()


def select_incomplete_tasks(conn):
    """
    Query all tasks that are not completed
    :param conn: the Connection object
    :return: rows of tasks
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT * FROM tasks WHERE status_id != (
                SELECT id FROM status WHERE name = 'completed'
            )
        """)
        rows = cur.fetchall()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows


def delete_task(conn, id):
    """
    Delete a specific task by ID
    :param conn: the Connection object
    :param task_id: the ID of the task to delete
    """
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM tasks WHERE id = %s", (id,))
        conn.commit()
        print(f"Deleted task_id = {id}")
    except Error as e:
        print(e)
    finally:
        cur.close()


def select_users_by_email(conn, email):
    """
    Query users by email pattern
    :param conn: the Connection object
    :param email_pattern: the email pattern to match
    :return: rows of users
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM users WHERE email LIKE %s",
                    (email,))
        rows = cur.fetchall()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows


def update_user_fullname(conn, id):
    """
    Update the fullname of a specific user
    :param conn: the Connection object
    :param user_id: the ID of the user to update
    :param new_fullname: the new fullname
    """
    fake = Faker()
    new_fullname = fake.name()
    new_email = fake.email()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE users SET fullname = %s, email = %s WHERE id = %s",
                    (new_fullname, new_email, id))
        conn.commit()
        print("Updated fullname for id =", id)
    except Error as e:
        print(e)
    finally:
        cur.close()


def count_tasks_by_status(conn):
    """
    Count the number of tasks for each status
    :param conn: the Connection object
    :return: rows of status counts
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT s.name, COUNT(t.id) 
            FROM tasks as t
            JOIN status as s ON t.status_id = s.id
            GROUP BY s.name
        """)
        rows = cur.fetchall()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows


def select_tasks_by_user_email_domain(conn, domain_pattern):
    """
    Query tasks assigned to users with a specific email domain
    :param conn: the Connection object
    :param domain_pattern: the email domain pattern to match
    :return: rows of tasks
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT u.id, t.title, t.description FROM tasks as t
            JOIN users as u ON u.id = t.user_id
            WHERE u.email LIKE %s
        """, (domain_pattern,))
        rows = cur.fetchall()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows


def select_tasks_without_description(conn):
    """
    Query tasks that do not have a description
    :param conn: the Connection object
    :return: rows of tasks
    """
    rows = None
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT * FROM tasks WHERE description IS NULL OR description = ''")
        rows = cur.fetchall()
    except Error as e:
        print(e)
    finally:
        cur.close()
    return rows

def select_users_and_tasks_in_progress(conn):
    """
    Select users and their tasks that are in 'in progress' status
    :param conn: the Connection object
    :return: list of users and their tasks in 'in progress' status
    """
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT u.id, u.fullname, t.id, t.title, t.description
            FROM users as u
            INNER JOIN tasks as t ON u.id = t.user_id
            INNER JOIN status as s ON t.status_id = s.id
            WHERE s.name = 'in progress'
        """)
        rows = cur.fetchall()
        for row in rows:
            print(row)
    except Error as e:
        print(e)
    finally:
        cur.close()

def select_users_and_task_counts(conn):
    """
    Select users and the count of their tasks
    :param conn: the Connection object
    :return: list of users and their task counts
    """
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT u.id, u.fullname, COUNT(t.id) AS task_count
            FROM users as u
            LEFT JOIN tasks as t ON u.id = t.user_id
            GROUP BY u.id, u.fullname
            ORDER BY u.id
        """)
        rows = cur.fetchall()
        for row in rows:
            print(row)
    except Error as e:
        print(e)
    finally:
        cur.close()


if __name__ == '__main__':
    with create_connection(database_url) as conn:
        user_id = 10
        print(f"Tasks for user_id = {user_id}:")
        tasks_by_user = select_tasks_by_user(conn, user_id)
        print(tasks_by_user)

        status_name = 'new'
        print(f"\nTasks with status '{status_name}':")
        tasks_by_status = select_tasks_by_status(conn, status_name)
        print(tasks_by_status)

        task_id = 18
        new_status_name = 'in progress'
        update_task_status(conn, task_id, new_status_name)

        print("\nUsers without tasks:")
        users_without_tasks = select_users_without_tasks(conn)
        print(users_without_tasks)

        user_id = 7
        insert_new_task(conn, user_id)

        print("\nIncomplete tasks:")
        incomplete_tasks = select_incomplete_tasks(conn)
        print(incomplete_tasks)

        id = 1
        delete_task(conn, id)

        email_pattern = 'grahamsusan@example.com'
        print(f"\nUsers with email like '{email}':")
        users_by_email = select_users_by_email(conn, email)
        print(users_by_email)

        id = 2
        update_user_fullname(conn, id)

        print("\nTask counts by status:")
        task_counts = count_tasks_by_status(conn)
        print(task_counts)

        domain_pattern = '%@example.com'
        print(f"\nTasks assigned to users with email like '{domain_pattern}':")
        tasks_by_email_domain = select_tasks_by_user_email_domain(
            conn, domain_pattern)
        print(tasks_by_email_domain)

        print("\nTasks without description:")
        tasks_without_description = select_tasks_without_description(conn)
        print(tasks_without_description)

        print("Users and their tasks in 'in progress' status:")
        select_users_and_tasks_in_progress(conn)

        print("Users and their task counts:")
        select_users_and_task_counts(conn)