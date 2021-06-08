#omnik_server.py

import socket
import sys
import mysql.connector as mysql
from datetime import datetime

# Constants
STRINGS = 2

# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# Bind the socket to the address given on the command line
server_name = "192.168.2.201"
server_address = (server_name, 10000)
print('starting up on {} port {}'.format(*server_address))
sock.bind(server_address)
sock.listen(1)

def get_data():
    print('waiting for a connection')
    connection, client_address = sock.accept()
    first_frame = 0
    
    try:
        print('client connected:', client_address)
        while True:
            data = connection.recv(256)
            if first_frame == 0:
                first_frame = data
            
            if not data:
                break
    finally:
        connection.close()
    
    return first_frame

def parse_data(data, start_byte, num_bytes, div):
    result = int.from_bytes(received_data[start_byte:start_byte + num_bytes], byteorder='big')
    if div != 1:
        result /= div
    return result

def insert_database(values):
    db = mysql.connect(user='rhmswink', password='password',
                              host='127.0.0.1',
                              database='omnik')
    
    cursor = db.cursor()
#     query_new_table = ("""CREATE TABLE omnik_data (id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
#                                                     DateTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
#                                                     VoltageDC1 float(23), VoltageDC2 float(23),
#                                                     CurrentDC1 float(23), CurrentDC2 float(23),
#                                                     VoltageAC float(23), CurrentAC float(23),
#                                                     Frequency float(23), Temperature float(23),
#                                                     Power_W smallint, Power_today_Wh smallint, Power_total_kWh float(23),
#                                                     total_hours int)""")
#     cursor.execute(query_new_table)

    query = """INSERT INTO omnik_data (
            VoltageDC1, VoltageDC2,
            CurrentDC1, CurrentDC2,
            VoltageAC, CurrentAC,
            Frequency, Temperature,
            Power_W, Power_today_Wh,
            Power_total_kWh, total_hours) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    
    cursor.execute(query, values)
    
    db.commit()
    
    ## getting records from the table
    cursor.execute("SELECT * FROM omnik_data")

    ## fetching all records from the 'cursor' object
    records = cursor.fetchall()

    ## Showing the data
    for record in records:
        print(record)
            
    db.close()

while True:
    received_data = get_data()
    
    vdc1 = parse_data(received_data, 33, 2, 10)
    vdc2 = parse_data(received_data, 35, 2, 10)
    idc1 = parse_data(received_data, 39, 2, 10)
    idc2 = parse_data(received_data, 41, 2, 10)
    vac   = parse_data(received_data,51,2,10)
    iac   = parse_data(received_data,45,2,10)
    freq  = parse_data(received_data,57,2,100)
    temp  = parse_data(received_data, 31,2,10)
    power = parse_data(received_data, 59, 2, 1)
    power_today = 10 * parse_data(received_data,69,2,1)
    total_power = parse_data(received_data,71,4,10)
    total_hours = parse_data(received_data, 75,4,1)
    
    all_values = (vdc1, vdc2, idc1, idc2, vac, iac, freq, temp, power, power_today, total_power, total_hours)
    insert_database(all_values)
    
#     for i in range(STRINGS):
#         print('DC[{}] voltage is: {}V'.format(i+1,vdc[i]))
#         print('DC[{}] current is: {}A'.format(i+1,idc[i]))
#     print('Current power is:', power,'W')
#     print('temp is:', temp)
#     print('vac is:',vac)
#     print('iac is:',iac)
#     print('freq is:',freq)
#     print('power_today is:',power_today)
#     print('total_power is:',total_power)
#     print('total_hours is:',total_hours)
    
    


