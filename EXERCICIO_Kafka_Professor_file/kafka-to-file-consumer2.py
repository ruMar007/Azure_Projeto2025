from kafka import KafkaConsumer
import gzip
import json

FILE_INDEX = 0
N_MESSAGES = 0
NUMBER_OF_LINES = 100

consumer = KafkaConsumer(auto_offset_reset='earliest',
                         group_id='a-group',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
consumer.subscribe(['test-topic'])

for msg in consumer:
    print(msg.value)

    with gzip.open('C:\\Estudos\\Programação\\UpSkill\\05_Modulo_Azure\\Projeto_Avaliacao\\Exercicios\\Exercise_22\\files2\\files_' + str(FILE_INDEX) + '.gz', 'at') as f:
        f.write(json.dumps(msg.value) + "\n")

    N_MESSAGES += 1
    if N_MESSAGES % NUMBER_OF_LINES == 0:
        FILE_INDEX += 1
