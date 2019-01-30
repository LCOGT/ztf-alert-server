#!/bin/env python
import fastavro
import io
import boto3
import os
import logging
import base64
from datetime import datetime, timedelta
from kafka.errors import CorruptRecordException
from kafka import KafkaConsumer
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
TOPIC = '^(ztf_\d{8}_programid1)'
GROUP_ID = 'LCOGT'
PRODUCER_HOST = 'public.alerts.ztf.uw.edu'
PRODUCER_PORT = '9092'


def do_ingest(encoded_packet):
    f_data = base64.b64decode(encoded_packet)
    freader = fastavro.reader(io.BytesIO(f_data))
    for packet in freader:
        ingest_avro(packet)
        fname = '{}.avro'.format(packet['candid'])
        upload_avro(io.BytesIO(f_data), fname, packet)


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
        try:
            db.session.add(alert)
            db.session.commit()
            ingest_delay = datetime.now() - alert.wall_time
            logger.info('Successfully inserted object', extra={'tags': {'candid': alert.alert_candid, 'ingest_delay': str(ingest_delay), 'ingest_delay_seconds': ingest_delay.total_seconds()}})
        except exc.SQLAlchemyError:
            db.session.rollback()
            logger.warn('Failed to insert object', extra={'tags': {'candid': alert.alert_candid}})


def upload_avro(f, fname, packet):
    date_key = packet_path(packet)
    filename = '{0}{1}'.format(date_key, fname)
    try:
        s3.Object(BUCKET_NAME, filename).put(
            Body=f,
            ContentDisposition=f'attachment; filename={filename}',
            ContentType='avro/binary'
        )
        logger.info('Successfully uploaded file to s3', extra={'tags': {'filename': filename}})
    except ClientError:
        logger.warn('Failed to upload file to s3', extra={'tags': {'filename': filename}})


def packet_path(packet):
    jd_time = Time(packet['candidate']['jd'], format='jd')
    return '{0}/{1}/{2}/'.format(
        jd_time.datetime.year, str(jd_time.datetime.month).zfill(2), str(jd_time.datetime.day).zfill(2)
    )


def start_consumer():
    consumer = KafkaConsumer(bootstrap_servers=f'{PRODUCER_HOST}:{PRODUCER_PORT}', group_id=GROUP_ID, auto_offset_reset='earliest')
    consumer.subscribe(pattern=TOPIC)
    logger.info('Successfully subscribed to Kafka topic', extra={'tags': {'subscribed_topics': list(consumer.subscription())}})
    while True:
        try:
            msg = next(consumer)
            alert = msg.value
            logger.debug('Received alert from stream')
            do_ingest(base64.b64encode(alert).decode('UTF-8'))
            consumer.commit()
            logger.debug('Committed index to Kafka producer')
        except CorruptRecordException:
            logger.info('Got a Corrupt Record')
        except StopIteration:
            pass


if __name__ == '__main__':
    db.create_all()
    start_consumer()
