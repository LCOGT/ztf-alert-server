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
from dramatiq.brokers.redis import RedisBroker
from astropy.coordinates import SkyCoord
from astropy.time import Time

from ztf import Alert, db, app

REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
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
def ingest_avro(packet):
    with app.app_context():
        packet = b64_decode_images(packet)
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

        extract_upload_images(packet)

        alert = Alert(
            objectId=packet['objectId'],
            publisher=packet.get('publisher', ''),
            alert_candid=packet['candid'],
            location=location,
            deltamaglatest=deltamaglatest,
            deltamagref=deltamagref,
            gal_l=galactic.l.value,
            gal_b=galactic.b.value,
            cutoutScienceFileName=packet['cutoutScience'].get('fileName'),
            cutoutTemplateFileName=packet['cutoutTemplate'].get('fileName'),
            cutoutDifferenceFileName=packet['cutoutDifference'].get('fileName'),
            **packet['candidate']
            )
        db.session.add(alert)
        db.session.commit()
        logger.info(alert.objectId)


ingest_avro.logger.setLevel(logging.CRITICAL)


def extract_upload_images(packet):
    jd_time = Time(packet['candidate']['jd'], format='jd')
    date_key = '{0}/{1}/{2}/'.format(
        jd_time.datetime.year, str(jd_time.datetime.month).zfill(2), str(jd_time.datetime.day).zfill(2)
    )

    for cutout in ['cutoutScience', 'cutoutTemplate', 'cutoutDifference']:
        filename = '{0}{1}'.format(date_key, packet[cutout]['fileName'])
        s3.Object(BUCKET_NAME, filename).put(
            Body=io.BytesIO(packet[cutout]['stampData']),
            ContentDisposition=f'attachment; filename={filename}',
            ContentType='image/fits',
            ContentEncoding='gzip',
        )
        logger.info('Uploaded %s to s3', filename)


def b64_encode_images(packet):
    for cutout in ['cutoutScience', 'cutoutTemplate', 'cutoutDifference']:
        packet[cutout]['stampData'] = base64.b64encode(packet[cutout]['stampData']).decode('UTF-8')
    return packet


def b64_decode_images(packet):
    for cutout in ['cutoutScience', 'cutoutTemplate', 'cutoutDifference']:
        packet[cutout]['stampData'] = base64.b64decode(packet[cutout]['stampData'])
    return packet


def read_avros(url):
    with requests.get(url, stream=True) as response:
        with tarfile.open(fileobj=response.raw, mode='r|gz') as tar:
            while True:
                member = tar.next()
                if member is None:
                    logger.info('Done ingesting this package')
                    break
                f = tar.extractfile(member)
                if f:
                    freader = fastavro.reader(f)
                    for packet in freader:
                        logger.info('sending task for packet %s', packet['objectId'])
                        packet = b64_encode_images(packet)
                        ingest_avro.send(packet)
            logger.info('done sending tasks')


if __name__ == '__main__':
    db.create_all()
    url = sys.argv[1]
    read_avros(url)
