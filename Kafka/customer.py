# Tạo consumer để đọc thông điệp từ Kafka
from kafka import  KafkaConsumer
import pyodbc 

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-O7T6R5H;'
                      'Database=feedback;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

consumer = KafkaConsumer('topic-example', bootstrap_servers='localhost:9092', group_id='my-group')
for message in consumer:
    #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
    #print(str(message.value))
    value = message.value.decode('utf-8')
    value =  tuple(value.split('==='))
    # user,text_review,rate,time_visit,language,platform = value
    #print(user,rate,time_visit,language,platform)
    query =  'INSERT INTO [testdbo] ([user],[text_review],[rate],[time_visit],[language],[platform]) VALUES (?, ?, ?, ?, ?, ?)'
    #print(query)
    print("Insert data into table : ",value[0],value[-1])
    cursor.execute(query,value)
    conn.commit()
    print("Successfully !!!")
    # try:
    #     json_data = json.loads(message.value().decode('utf-8'))
    #     # Xử lý dữ liệu JSON ở đây
    #     print('Received JSON data:', json_data)
    # except json.JSONDecodeError as e:
    #     print('Error decoding JSON:', e)