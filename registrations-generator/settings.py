import os

# kafka settings
KAFKA_CONSUMER_GROUP = 'registrations-generator'
try:
    KAFKA_HOSTS = os.environ['KAFKA_HOSTS']
except KeyError:
    KAFKA_HOSTS = 'localhost:9092'
try:
    KAFKA_SESSIONS_TOPIC = os.environ['KAFKA_SESSIONS_TOPIC']
except KeyError:
    KAFKA_SESSIONS_TOPIC = 'sessions'
try:
    KAFKA_REGISTRATIONS_TOPIC = os.environ['KAFKA_REGISTRATIONS_TOPIC']
except KeyError:
    KAFKA_REGISTRATIONS_TOPIC = 'registrations'
try:
    SESSIONS_FILE = os.environ['SESSIONS_FILE']
except KeyError:
    SESSIONS_FILE = 'sessions.csv'
