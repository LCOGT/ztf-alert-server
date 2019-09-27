#!/bin/env python
import fastavro
import io
import boto3
import os
import base64
import time
from datetime import datetime, timedelta
from confluent_kafka import Consumer
from astropy.coordinates import SkyCoord
from astropy.time import Time
from botocore.exceptions import ClientError
from sqlalchemy import exc

from ztf import Alert, NonDetection, db, app, logger

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('S3_BUCKET')


session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

s3 = session.resource('s3')

# ZTF Kafka Configuration
ZTF_TOPIC = '^(ztf_[0-9]{8}_programid1)'
TESS_PUBLIC_TOPIC = '^(ztf_[0-9]{8}_programid3_public)'
GROUP_ID = os.getenv('GROUP_ID', default='LCOGT-test')
PRODUCER_HOST = 'public.alerts.ztf.uw.edu'
PRODUCER_PORT = '9092'


def insert_or_update_alert(alert):
    try:
        existing_alert = db.session.query(Alert).filter_by(alert_candid=alert.alert_candid).limit(1).all()
        if not existing_alert:
            db.session.add(alert)
            db.session.commit()
            ingest_delay = datetime.now() - alert.wall_time
            logger.info('Successfully inserted object', extra={'tags': {
                'candid': alert.alert_candid,
                'ingest_delay': str(ingest_delay),
                'ingest_delay_seconds': ingest_delay.total_seconds(),
                'successful_ingest': 'true'
            }})
        else:
            logger.info('Alert already exists in database.', extra={'tags': {
                'candid': alert.alert_candid,
                'successful_ingest': 'false'
            }})
        return True
    except exc.SQLAlchemyError as e:
        db.session.rollback()
        logger.warn('Failed to insert object', extra={'tags': {
            'candid': alert.alert_candid,
            'sql_error': e.orig.args[0],
            'successful_ingest': 'false'
        }})
        return False


def do_ingest(encoded_packet):
    f_data = base64.b64decode(encoded_packet)
    freader = fastavro.reader(io.BytesIO(f_data))
    for packet in freader:
        start_ingest = datetime.now()
        successful_ingestion, candid = ingest_avro(packet)
        if successful_ingestion:
            logger.info('Time to ingest avro', extra={'tags': {
                'ingest_time': (datetime.now() - start_ingest).total_seconds()
            }})
            fname = '{}.avro'.format(packet['candid'])
            start_upload = datetime.now()
            upload_avro(io.BytesIO(f_data), fname, packet)
            logger.info('Time to upload avro', extra={'tags': {
                'upload_time': (datetime.now() - start_upload).total_seconds()
            }})
        return successful_ingestion, candid


def ingest_avro(packet):
    with app.app_context():
        ra = packet['candidate'].pop('ra')
        dec = packet['candidate'].pop('dec')
        location = f'srid=4035;POINT({ra} {dec})'
        c = SkyCoord(ra, dec, unit='deg')
        galactic = c.galactic

        deltamaglatest = None
        if packet['prv_candidates']:
            prv_candidates = sorted(packet['prv_candidates'], key=lambda x: x['jd'], reverse=True)
            non_detections = {}
            for candidate in prv_candidates:
                if all(candidate[key] is None for key in ('candid', 'isdiffpos', 'ra', 'dec', 'magpsf', 'sigmapsf', 'ranr', 'decnr')):
                    non_detections[packet['objectId'] + str(candidate['jd'])] = NonDetection(
                        objectId=packet['objectId'],
                        jd=candidate['jd'],
                        diffmaglim=candidate['diffmaglim'],
                        fid=candidate['fid']
                    )
                elif not deltamaglatest and packet['candidate']['fid'] == candidate['fid'] and candidate['magpsf']:
                    deltamaglatest = packet['candidate']['magpsf'] - candidate['magpsf']
            for key, nd in non_detections.items():
                count = db.session.query(NonDetection).filter(NonDetection.objectId == nd.objectId).filter(NonDetection.jd==nd.jd).count()
                if count == 0:
                    db.session.add(nd)

        deltamagref = None
        if packet['candidate']['distnr'] < 2:
            deltamagref = packet['candidate']['magnr'] - packet['candidate']['magpsf']

        alert = Alert(
            objectId=packet['objectId'],
            publisher=packet.get('publisher', ''),
            alert_candid=packet['candid'],
            location=location,
            deltamaglatest=deltamaglatest,
            deltamagref=deltamagref,
            gal_l=galactic.l.value,
            gal_b=galactic.b.value,
            **packet['candidate']
            )
        return insert_or_update_alert(alert), alert.alert_candid


def upload_avro(f, fname, packet):
    date_key = packet_path(packet)
    filename = '{0}{1}'.format(date_key, fname)
    try:
        s3.Object(BUCKET_NAME, filename).put(
            Body=f,
            ContentDisposition=f'attachment; filename={filename}',
            ContentType='avro/binary'
        )
        logger.info('Successfully uploaded file to s3', extra={'tags': {
            'filename': filename,
            'successful_upload': 'true'
        }})
    except ClientError:
        logger.warn('Failed to upload file to s3', extra={'tags': {
            'filename': filename,
            'successful_upload': 'false'
        }})


def packet_path(packet):
    jd_time = Time(packet['candidate']['jd'], format='jd')
    return '{0}/{1}/{2}/'.format(
        jd_time.datetime.year, str(jd_time.datetime.month).zfill(2), str(jd_time.datetime.day).zfill(2)
    )


# This method was originally used to subscribe to the last 7 days of topics, updating at midnight UTC. However, due to
# issues with offsets resetting every night, this has been reduced to a single night, and should be updated once the
# offset issue is resolved.
def update_topic_list(consumer, current_topic_date=None):
    current_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    if current_topic_date is None or (current_date - current_topic_date).days > 0:
        current_topics = []
        for i in range(0, 1):
            topic_date = current_date - timedelta(days=i)
            current_topics.append('ztf_{}{:02}{:02}_programid1'.format(
                topic_date.year,
                topic_date.month,
                topic_date.day
            )) # Add ZTF topics
            current_topics.append('ztf_{}{:02}{:02}_programid3_public'.format(
                topic_date.year,
                topic_date.month,
                topic_date.day
            )) # Add TESS public topics
        consumer.subscribe(current_topics)

        logger.info('New topics', extra={'tags': {
            'subscribed_topics': ['{0} - {1}'.format(topic.topic, topic.partition) for topic in consumer.assignment()],
            'subscribed_topics_count': len(consumer.assignment())
        }})
    return current_date


def on_commit(err, partitions):
    if err:
        logger.warn('Commit failed with error {error}'.format(error=str(err)))


def start_consumer():
    logger.info('Starting consumer', extra={'tags': {
        'group_id': GROUP_ID
    }})
    consumer = Consumer({
        'bootstrap.servers': f'{PRODUCER_HOST}:{PRODUCER_PORT}',
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'queued.max.messages.kbytes': 100000,
        'enable.auto.commit': 'false',
        'on_commit': on_commit
    })
    current_date = update_topic_list(consumer)

    while True:
        if int(time.time()) % 300 == 0:
            current_date = update_topic_list(consumer, current_topic_date=current_date)
        msg = consumer.poll(1)
        if msg is None:
            continue
        if msg.error():
            logger.error('Consumer error: {}'.format(msg.error()))
            continue

        process_start_time = datetime.now()
        alert = base64.b64encode(msg.value()).decode('utf-8')
        logger.info('Received alert from stream')
        success, candid = do_ingest(alert)
        logger.info('Finished processing message from {topic} with offset {offset}'.format(
                    topic=msg.topic() + '-' + str(msg.partition()), offset=msg.offset()),
                    extra={'tags': {
                                'candid': candid,
                                'success': success,
                                'record_processing_time': (datetime.now() - process_start_time).total_seconds(),
                                'processing_latency': datetime.now().timestamp() - msg.timestamp()[1]/1000
                          }}
                    )
        consumer.commit(msg)

    consumer.close()


if __name__ == '__main__':
    db.create_all()
    start_consumer()
