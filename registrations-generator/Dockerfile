FROM python:3.14.0a1-alpine3.20
RUN apk add git
COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt
RUN pip3 install --break-system-packages git+https://github.com/dpkp/kafka-python.git
WORKDIR /src

COPY . .

ENV KAFKA_HOSTS ''
ENV KAFKA_SESSIONS_TOPIC 'sessions'
ENV KAFKA_REGISTRATIONS_TOPIC 'registrations'
ENV SESSIONS_FILE './sessions.csv'

CMD ["python3", "main.py"]`