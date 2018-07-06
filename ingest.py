#!/bin/env python
import tarfile
import fastavro
import sys
from astropy.coordinates import SkyCoord

from ztf import Alert, db


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
