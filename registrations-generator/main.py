
# -*- coding: utf-8 -*-
import json
import settings
import csv
import random
import datetime
import time
from kafka import KafkaProducer

def readSessionsFrom(sessionsFile):
    sessions = []
    with open(sessionsFile, newline='') as csvfile:
        file = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in file:
            sessions.append({'id': row[0],'date': row[1],'speaker': row[2],'title': row[3]})
    return sessions
    
def main():
    producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_HOSTS,
            value_serializer=lambda v: json.dumps(v, default=str, sort_keys=True,indent=4).encode('utf-8')
            )
    if not producer.bootstrap_connected():
        raise Exception('Failed to connect to kafka bootstrap')

    sessions = readSessionsFrom(settings.SESSIONS_FILE)
    for session in sessions:
        producer.send(settings.KAFKA_SESSIONS_TOPIC, session)

    while True:
        session = sessions[random.randint(0,len(sessions)-1)]
        producer.send(settings.KAFKA_REGISTRATIONS_TOPIC, {'reg_ts':datetime.datetime.now(), 'session_id':session['id']})
        time.sleep(random.randint(0,5))

if __name__ == "__main__":
    main()