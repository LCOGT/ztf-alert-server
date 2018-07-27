import re
from kafka import KafkaConsumer

#TOPIC = '^(ztf_\d{8}_programid1)'
TOPIC = 'ztf_20180725_programid1'
#TOPIC = 'test-topic'
GROUP_ID = 'LCOGT-test01'
#PRODUCER_HOST = 'localhost'
PRODUCER_HOST = 'public.alerts.ztf.uw.edu'
PRODUCER_PORT = '9092'

consumer = KafkaConsumer(TOPIC, bootstrap_servers=f'{PRODUCER_HOST}:{PRODUCER_PORT}', group_id=GROUP_ID, auto_offset_reset='earliest')

def main():
    for msg in consumer:
        value = bytearray(msg.value)
        print(value)

main()