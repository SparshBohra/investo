# importing libraries
import psycopg2
import csv

hostname = 'localhost'
database = 'investo'
username = 'postgres'
pwd = 'idek'
port_id = 5432

# establishing connection with db
conn = psycopg2.connect(
    host = hostname,
    dbname = database,
    user = username,
    password = pwd,
    port = port_id)

cur = conn.cursor()

# sql statement for table
create_table = ''' CREATE TABLE IF NOT EXISTS HINDALCO (
                        id serial NOT NULL,
                        datetime timestamp NOT NULL, 
                        close float, 
                        high float, 
                        low float, 
                        open float, 
                        volume int, 
                        instrument varchar(255),
                        PRIMARY KEY (id)
)
'''
cur.execute(create_table)

csv_data = csv.reader(open('./stock_data.csv'))
header = next(csv_data)

# sql statement for inserting data
print('Importing the file')
for row in csv_data:
    print(row)
    insert_data = '''INSERT INTO HINDALCO (datetime, close, high, low, open, volume, instrument) VALUES (%s, %s, %s, %s, %s, %s, %s)'''
    cur.execute(insert_data, row)

# sql statement to read table
read_table = "SELECT * FROM HINDALCO"
cur.execute(read_table)
data = cur.fetchall()

# putting data into dataframe
df = pd.DataFrame(data)
df.columns =['id','datetime','close','high','low','open','volume','instrument']
print(df)

conn.commit()
conn.close()






