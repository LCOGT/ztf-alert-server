#!/bin/env python
import tarfile
import fastavro
import sys
import io
import boto3
import os
import logging
import requests
import dramatiq
import base64
import time
import redis
from dramatiq.brokers.redis import RedisBroker
from astropy.coordinates import SkyCoord
from astropy.time import Time

from ztf import Alert, db, app

REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
r = redis.StrictRedis(host=REDIS_HOST, charset='utf-8', decode_responses=True)
redis_broker = RedisBroker(url=f'redis://{REDIS_HOST}:6379/0')
dramatiq.set_broker(redis_broker)

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('S3_BUCKET')

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

s3 = session.resource('s3')


@dramatiq.actor
def do_ingest(encoded_packet):
    f_data = base64.b64decode(encoded_packet)
    freader = fastavro.reader(io.BytesIO(f_data))
    for packet in freader:
        ingest_avro(packet)
    fname = '{}.avro'.format(packet['candid'])
    upload_avro(io.BytesIO(f_data), fname, packet)


do_ingest.logger.setLevel(logging.CRITICAL)


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
            for candidate in prv_candidates:
                if packet['candidate']['fid'] == candidate['fid'] and candidate['magpsf']:
                    deltamaglatest = packet['candidate']['magpsf'] - candidate['magpsf']
                    break

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
        db.session.add(alert)
        db.session.commit()
        logger.info(alert.objectId)


def upload_avro(f, fname, packet):
    date_key = packet_path(packet)
    filename = '{0}{1}'.format(date_key, fname)
    s3.Object(BUCKET_NAME, filename).put(
        Body=f,
        ContentDisposition=f'attachment; filename={filename}',
        ContentType='avro/binary'
    )
    logger.info('Uploaded %s to s3', filename)


def packet_path(packet):
    jd_time = Time(packet['candidate']['jd'], format='jd')
    return '{0}/{1}/{2}/'.format(
        jd_time.datetime.year, str(jd_time.datetime.month).zfill(2), str(jd_time.datetime.day).zfill(2)
    )


def read_avros(url):
    with requests.get(url, stream=True) as response:
        with tarfile.open(fileobj=response.raw, mode='r|gz') as tar:
            while True:
                while r.info()['used_memory'] > 1410612736:
                    time.sleep(1)
                member = tar.next()
                if member is None:
                    logger.info('Done ingesting this package')
                    break
                with tar.extractfile(member) as f:
                    if f:
                        fencoded = base64.b64encode(f.read()).decode('UTF-8')
                        do_ingest.send(fencoded)
            logger.info('done sending tasks')


if __name__ == '__main__':
    db.create_all()
    url = sys.argv[1]
    read_avros(url)
