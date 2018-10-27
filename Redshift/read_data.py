import psycopg2
import pprint
import os

# Configuration dictionary
config = {'dbname': 'log',
          'user': os.environ['RTFA-REDSHIFT-USR'],
          'pwd': os.environ['RTFA-REDSHIFT-PWD'],
          'host': 'movement-log.cnadblxzqjnp.eu-central-1.redshift.amazonaws.com',
          'port': '5439'
          }

# Create a connection to the Redshift database
conn = psycopg2.connect(dbname=config['dbname'], host=config['host'], port=config['port'], user=config['user'],
                        password=config['pwd'])


# Create a cursor that can be used to execute commands (can make many cursors)
cursor = conn.cursor()

# Execute an SQL query to get everything from the log table
cursor.execute("select * from log")

# Get all the results of the query and print them
rows = cursor.fetchall()
for row in rows:
    print(row)

# Close the cursor (but not the overall connection)
cursor.close()

# The connection can send notices to the client - print them
for n in list(conn.notices):
    pprint.PrettyPrinter(n)

# Close the whole connection to the database
conn.close()
