FROM python:3.10

WORKDIR /usr/src/app

COPY . .

RUN pip install --no-cache-dir --upgrade pip

RUN pip install --no-cache-dir -r requirements.txt

ENV KAFKA_BROKER="localhost:9092"

CMD ["python", "-u", "./producer.py"]