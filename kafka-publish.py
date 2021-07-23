import random
import time
from faker import Faker
import tweepy
from kafka import KafkaProducer
from json import dumps
import config as conf

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8')
                         )

def fake_date():
    fake = Faker()
    data = {}
    for i in range(0, 5):
        data[i] = {}
        data[i]['id'] = random.randint(0, 5)
        data[i]['name'] = fake.name()
        data[i]['city'] = fake.city()
        data[i]['country'] = fake.country()
        producer.send('sample', data[i])
    print(data)
    producer.flush()


if __name__ == '__main__':
    while True:
        print("Publish new tweets: ")
        fake_date()
        time.sleep(5)
