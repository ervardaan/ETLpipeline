import pytest
import psycopg2

@pytest.fixture
def db_connection():
    conn = psycopg2.connect(
        dbname="test_db",
        user="test_user",
        password="test_password",
        host="localhost",
        port="5432"
    )
    yield conn
    conn.close()

def test_database_connection(db_connection):
    assert db_connection is not None

def test_insert_and_query(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("INSERT INTO user_summary (user_id, total_experiments) VALUES (1, 2);")
    db_connection.commit()
    cursor.execute("SELECT * FROM user_summary WHERE user_id = 1;")
    result = cursor.fetchone()
    assert result is not None
    assert result[0] == 1
    assert result[1] == 2
