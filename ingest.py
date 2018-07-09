#!/bin/env python
import tarfile
import fastavro
import sys
import io
import boto3
import os
import logging
from astropy.coordinates import SkyCoord
from astropy.time import Time

from ztf import Alert, db

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


def ingest_avro(avro):
    freader = fastavro.reader(avro)
    for packet in freader:
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


def read_avros(filename):
    tar = tarfile.open(filename, 'r:gz')
    for member in tar.getmembers():
        f = tar.extractfile(member)
        if f:
            ingest_avro(f)


if __name__ == '__main__':
    db.create_all()
    filename = sys.argv[1]
    read_avros(filename)
