import sys
import numpy as np
from datetime import datetime, timedelta
from time import sleep
from sqlalchemy import create_engine
import psycopg2
from psycopg2.extras import execute_values
from win32com.client import Dispatch



db_endpoint = "testinhk.ckensqtixcpt.ap-southeast-2.rds.amazonaws.com"
db_port = "5432"
db_name = "HKdatabase"
db_user = "postgres"
db_password = "Mincka.2024"

# Create the connection string
connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_endpoint}:{db_port}/{db_name}"

# Using SQLAlchemy to create engine
engine = create_engine(connection_string)

def connect_to_db():
    try:
        cnx = engine.raw_connection()
        print('Connection made')
        return cnx
    except psycopg2.DatabaseError as e:
        print(f'Error {e}')
        return None

cnx = connect_to_db()

if cnx is None:
    sys.exit(1)

#--MySQL DB Cursor--# 
cursor = cnx.cursor()

# Check if table exists and create it if it does not exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS "100hz_python_SIM_ALL_SENSORS_WORKING_AUGUST"
    (
        timestamp TIMESTAMP,
        AI1 DOUBLE PRECISION,
        AI2 DOUBLE PRECISION,
        AI3 DOUBLE PRECISION,
        AI4 DOUBLE PRECISION,
        AI5 DOUBLE PRECISION,
        AI6 DOUBLE PRECISION,
        AI7 DOUBLE PRECISION,
        AI8 DOUBLE PRECISION,
        AI9 DOUBLE PRECISION,
        AI10 DOUBLE PRECISION,
        AI11 DOUBLE PRECISION,
        AI12 DOUBLE PRECISION,
        AI13 DOUBLE PRECISION,
        AI14 DOUBLE PRECISION,
        AI15 DOUBLE PRECISION,
        AI16 DOUBLE PRECISION
    )
""")
cnx.commit()

#--MySQL DB Insert Query Template--# 
query_t= '''
INSERT INTO "100hz_python_SIM_ALL_SENSORS_WORKING_AUGUST" 
(timestamp, AI1, AI2, AI3, AI4,AI5,AI6,AI7,AI8,AI9,AI10,AI11,AI12,AI13,AI14,AI15,AI16)
VALUES %s
'''

dw = Dispatch("Dewesoft.App")
dw.Init()

channels_list = {
    "AI 1": True,
    "AI 2": True,
    "AI 3": True,
    "AI 4": True,
    "AI 5": True,
    "AI 6": True,
    "AI 7": True,
    "AI 8": True,
    "AI 9": True,
    "AI 10": True,
    "AI 11": True,
    "AI 12": True,
    "AI 13": True,
    "AI 14": True,
    "AI 15": True,
    "AI 16": True
}

dw.Visible = 1
# set window dimensions
dw.Top = 0
dw.Left = 0 
dw.Width = 1024
dw.Height = 768

print('Dewesoft open')

set_up_file = r"C:\Users\MINCKA\Desktop\Hailcreek.dxs"
dw.LoadSetup(set_up_file)

sleep(2)

AI = []
TS = []
loop = 0
pkg_sz = 10
channels = 4
values = ""

#------Update Channel List-------#
dw.Data.BuildChannelList()
#------Create IConnections List-------#
conn_list = []
#------Start Data Sync (Multi-Channels)-------#
for i in range(0, dw.Data.AllChannels.Count):
    selected_channel  = dw.Data.AllChannels.Item(i).Name 
    if (selected_channel in channels_list.keys()):
        dw.Data.AllChannels.Item(i).Used = channels_list[selected_channel]
        if(channels_list[selected_channel]):
            conn_list.append(dw.Data.UsedChannels.Item(i).CreateConnection())
            print(f"selected channel {selected_channel}")

dw.MeasureSampleRate = 100
print('Channels set up')
stop = True
#--Setup connections
for con in conn_list:
    con.AType = 3
    con.BlockSize = 100

sample_frecuency  = 1/100
choosen_delta = timedelta(milliseconds=1000 * sample_frecuency)
    
#---Start Acquisition---#        
dw.Start()

#---Wait until the first block of data is full---#
sleep(1.5)

while True:
    print("Reading data... \n")
    query = query_t
    current_time = datetime.now()
    c1 = current_time
    
    print(current_time)
    numberofpacke=0

    dw.Data.StartDataSync()

    for con in conn_list:
        AI.append(con.GetDataBlocks(1))

    dw.Data.EndDataSync()
    sleep(1)
    
    AI2 = np.array(AI)
    AI = []
    data_to_insert = []
    for row in AI2.T:
        if row[0] != 0:    
            current_str = current_time.strftime('%Y-%m-%d %H:%M:%S.%f') 
            data_to_insert.append((current_str,) + tuple(row.tolist()))
            current_time = current_time + choosen_delta
            numberofpacke = numberofpacke + 1 
        
    print("data2insert",data_to_insert)
    print(current_time)
    print(current_time-c1)
    print(numberofpacke)
    
    try:
        execute_values(cursor, query, data_to_insert)
        cnx.commit()
        print("Uploaded data ")
    except psycopg2.DatabaseError as e:
        print(f"Error during data insertion: {e}")
        print("Attempting to reconnect...")
        cnx = connect_to_db()
        if cnx is not None:
            cursor = cnx.cursor()
            try:
                execute_values(cursor, query, data_to_insert)
                cnx.commit()
                print("Uploaded data after reconnection")
            except psycopg2.DatabaseError as e:
                print(f"Reconnection and data insertion failed: {e}")
        else:
            print("Reconnection failed. Skipping this batch of data.")

    values = ""
    loop += 1
