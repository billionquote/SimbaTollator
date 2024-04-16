import os
import psycopg2

# Ensure you have this environment variable set in your development environment,
# or configure it in your deployment settings on Heroku.

DATABASE_URL = 'postgres://ktbzjfczfdhzls:894a3004b174c857f5188cc7148b20e9a660ae6b9c70ce8071287bd7700689de@ec2-35-169-9-79.compute-1.amazonaws.com:5432/d2jinffuso3col'
DATABASE_URL='postgres://ktbzjfczfdhzls:894a3004b174c857f5188cc7148b20e9a660ae6b9c70ce8071287bd7700689de@ec2-35-169-9-79.compute-1.amazonaws.com:5432/d2jinffuso3col'
print(f'{DATABASE_URL}')
conn = psycopg2.connect(DATABASE_URL, sslmode='require')
try:
    # Connect to your postgres DB
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    
    # Open a cursor to perform database operations
    cur = conn.cursor()
    
    # Create a table
    cur.execute("DROP TABLE IF EXISTS test;")
    cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
    
    # Insert some data
    cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, 'abc'))
    
    # Query the database and obtain data as Python objects
    cur.execute("SELECT * FROM test;")
    results = cur.fetchall()
    print(results)  # Output the results to the console to verify connection

    # Make the changes to the database persistent
    conn.commit()
    
    # Close communication with the database
    cur.close()
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if conn:
        conn.close()

