import time

from confluent_kafka import Producer
import glob


def send_message_to_kafka(file_path):
    producer = Producer({'bootstrap.servers':'localhost:9092'})
    topic = "logs_topic"

    processed_files = set()

    while True:
        files = glob.glob(file_path+"/*.json")
        # print(files)

        for file in files:
            if file not in processed_files: # New file found
                with open(file,"r") as f:
                    for line in f:
                        log = line.strip()   # One json object
                        producer.produce(topic,log)
                        producer.flush()
                        print("Sent: ", log)

                processed_files.add(file)

        time.sleep(2)


if __name__ == "__main__":
    input_path = "/home/sachin/Downloads/Datasets/Input/Real_time_logs"
    send_message_to_kafka(input_path)



