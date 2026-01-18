import csv
import time
from kafka import KafkaProducer


BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'electric_vehicles'
CSV_FILE_PATH = '/home/omer2/pyspark/input/Electric_Vehicle_Population_Data.csv'

def run_producer():
    
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: str(v).encode('utf-8')
    )

    print(f"Starting to send data from {CSV_FILE_PATH} to topic: {TOPIC_NAME}...")

    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)
            header = next(reader)  # דילוג על שורת הכותרות
            
            for row in reader:
                
                message = ",".join(row)
                
                
                producer.send(TOPIC_NAME, value=message)
                
                print(f"Sent: {row[0]}, {row[1]}") 
                
                time.sleep(0.1) 
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    run_producer()
