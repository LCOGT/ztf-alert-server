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

        latest_mag_diff = None
        if packet['prv_candidates']:
            prv_candidates = sorted(packet['prv_candidates'], key=lambda x: x['jd'], reverse=True)
            for candidate in prv_candidates:
                if packet['candidate']['fid'] == candidate['fid']:
                    if not candidate['magpsf']:
                        latest_mag_diff = packet['candidate']['magpsf'] - candidate['diffmaglim']
                    else:
                        latest_mag_diff = packet['candidate']['magpsf'] - candidate['magpsf']
                    break

        alert = Alert(
            objectId=packet['objectId'],
            publisher=packet.get('publisher', ''),
            alert_candid=packet['candid'],
            location=location,
            latest_mag_diff=latest_mag_diff,
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
