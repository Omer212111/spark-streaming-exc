import csv
import time
from kafka import KafkaProducer

# הגדרות ה-Kafka
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'electric_vehicles'
CSV_FILE_PATH = '/home/omer2/pyspark/input/Electric_Vehicle_Population_Data.csv'

def run_producer():
    # יצירת ה-Producer
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
                # הפיכת השורה למחרוזת אחת מופרדת בפסיקים (כמו ב-CSV המקורי)
                message = ",".join(row)
                
                # שליחת ההודעה ל-Kafka
                producer.send(TOPIC_NAME, value=message)
                
                # הדפסה קטנה לביקורת
                print(f"Sent: {row[0]}, {row[1]}") 
                
                # השהייה קלה כדי לדמות זרם נתונים בזמן אמת
                time.sleep(0.1) 
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    run_producer()
