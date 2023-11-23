# Tạo consumer để đọc thông điệp từ Kafka
from kafka import  KafkaConsumer
import pyodbc 
import time

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-O7T6R5H;'
                      'Database=feedback_data;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

consumer = KafkaConsumer('topic-example', bootstrap_servers='localhost:9092', group_id='my-group')
for message in consumer:
    try:
        
        value = message.value.decode('utf-8')
        value =  tuple(value.split('==='))
        name_producer =  value[0]
        print("receive data from {}".format(name_producer))
        query =  'INSERT INTO [feedback] ([user],[text_review],[sentiment_review],[sentiment_score],[rate],[time_visit],[language],[platform]) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        print("Insert data into table SQL : ",value[1],value[-1])
        cursor.execute(query,value[1:])
        conn.commit()
        print("sent to SQL successfully !")
   
    except :
       continue
    time.sleep(3)