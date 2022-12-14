# using connection and db cursor as context managers
import psycopg2
from psycopg2 import OperationalError
from db_config import get_db_info
from fake_data import generate_fake_data

filename='db_info.ini'
section='postgres-sample-db'

db_info = get_db_info(filename,section)
# print(db_info)
connection = None
try:
    with psycopg2.connect(**db_info) as db_connection:
        print("Successfully connected to the database.")
        # Create table 
        with db_connection.cursor() as db_cursor:
            create_table = '''CREATE TABLE people(
                          id SERIAL PRIMARY KEY,
                          name varchar(50) NOT NULL,
                          city varchar(40),
                          profession varchar(60));'''
            db_cursor.execute('DROP TABLE IF EXISTS people;')
            db_cursor.execute(create_table)
            
        # Insert one record
        
            insert_record = 'INSERT INTO people (name,city,profession) VALUES (%s, %s, %s);'
            insert_value = ('Jane Lee','RustMore','Rust programmer')
            db_cursor.execute(insert_record, insert_value)
            
        # Insert multiple records
        
            records = tuple(generate_fake_data(100))
            for record in records:
                db_cursor.execute(insert_record,record)
                
        # Retrieve data from the table
        
                db_cursor.execute('SELECT * FROM people')
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
            
        # Update records
        
            update_query = 'UPDATE people SET city=%s WHERE city=%s;'
            values = ('Mathville','Johnsonmouth')
            db_cursor.execute(update_query,values)
            
        # Delete records
        
            delete_record = 'DELETE FROM people WHERE city=%s;'
            record = ('Mathville',)
            db_cursor.execute(delete_record,record)
            
except OperationalError:
    print("Error connecting to the database :/")

finally:
    if connection:
        connection.close()
        print("Closed connection.")
