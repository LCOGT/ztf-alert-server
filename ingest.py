#!/bin/env python
import tarfile
import fastavro
import sys

from ztf import Alert, db


def ingest_avro(avro):
    freader = fastavro.reader(avro)
    for packet in freader:
        ra = packet['candidate'].pop('ra')
        dec = packet['candidate'].pop('dec')
        location = f'srid=4035;POINT({ra} {dec})'
        alert = Alert(
            objectId=packet['objectId'],
            publisher=packet.get('publisher', ''),
            alert_candid=packet['candid'],
            location=location,
            **packet['candidate']
            )
        db.session.add(alert)
        db.session.commit()
        print(alert.objectId)


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
