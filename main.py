import psycopg2
from psycopg2 import OperationalError

from db_config import get_db_info
from fake_data import generate_fake_data


filename='db_info.ini'
section='postgres-sample-db'

db_info = get_db_info(filename,section)
# print(db_info)
db_connection = None
try:
    db_connection = psycopg2.connect(**db_info)
    print("Successfully connected to the database.")
    db_connection.autocommit=True
    
    # create a db cursor
    db_cursor = db_connection.cursor()
    
    # create table
    create_table = '''CREATE TABLE people(
                          id SERIAL PRIMARY KEY,
                          name varchar(50) NOT NULL,
                          city varchar(40),
                          profession varchar(60));'''
    db_cursor.execute("DROP TABLE IF EXISTS people;") # if running multiple times, drop table and create a new one for each run.
    db_cursor.execute(create_table)
    
    # insert record
    insert_record = "INSERT INTO people (name,city,profession) VALUES (%s, %s, %s);"
    insert_value = ('Jane Lee','RustMore','Rust programmer')
    db_cursor.execute(insert_record, insert_value)
    
    # insert multiple records
    records = tuple(generate_fake_data(100))
    for record in records:
        db_cursor.execute(insert_record,record)
        
    # retrieve records
    db_cursor.execute("SELECT * FROM people;")
    print(db_cursor.fetchone())
    for record in db_cursor.fetchmany(10):
        print(record)
    for record in db_cursor.fetchall():
        print(record)
    get_count ='''SELECT city, COUNT(*)
                  FROM people
                  GROUP BY city HAVING COUNT(*)>1;'''
    db_cursor.execute(get_count)
    print(db_cursor.fetchall())
    
    # update records
    update_query = "UPDATE people SET city=%s WHERE city=%s;"
    values = ('Mathville','Johnsonmouth')
    db_cursor.execute(update_query,values)
    
    # delete records
    delete_record = "DELETE FROM people WHERE city=%s;"
    record = ('Mathville',)
    db_cursor.execute(delete_record,record)
    
except OperationalError:
    print("Error connecting to the database :/")

finally:
    if db_cursor:
        db_cursor.close()
    if db_connection:
        db_connection.close()
        print("Closed connection.")
