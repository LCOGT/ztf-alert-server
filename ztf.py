from flask import Flask, jsonify, request, render_template, send_file, abort
from flask_sqlalchemy import SQLAlchemy
from geoalchemy2 import Geography, Geometry, shape
from sqlalchemy import cast
from urllib.parse import urlencode
from astropy.time import Time
from datetime import datetime, timedelta
import math
import json
import os
import io
import fastavro
import requests
import logging
import boto3
from logging.config import dictConfig
from lcogt_logging import LCOGTFormatter

"""
Convert degrees to meters so we can use geography type:
2 * PI * 6371008.77141506 * DEGREES / 360

SRID for normal sphere: https://epsg.io/4035

"""
EARTH_RADIUS_METERS = 6371008.77141506
PRV_CANDIDATES_RADIUS = 0.000416667  # 1.5 arc seconds, same that ztf uses.
FILTERS = ['g', 'r', 'i']

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'ztf')
DB_PASS = os.getenv('DB_PASS', 'ztf')
DB_NAME = os.getenv('DB_NAME', 'ztf')

LOG_SETTINGS = {
    "formatters": {
        "default": {
            "()": LCOGTFormatter
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default"
        }
    },
    "loggers": {
        "listener": {
            "handlers": ["console"],
            "level": logging.INFO
        }
    },
    "version": 1
}
dictConfig(LOG_SETTINGS)
logger = logging.getLogger('listener')


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

def get_s3_client():
    config = boto3.session.Config(region_name='us-west-2', signature_version='s3v4')
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        config=config,
    )

def generate_presigned_url(key):
    client = get_s3_client()
    return client.generate_presigned_url(
        'get_object',
        ExpiresIn=3600 * 24 * 7,
        Params={'Bucket': os.getenv('S3_BUCKET'), 'Key': key}
    )

class NonDetection(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    objectId = db.Column(db.String(50), index=True)
    diffmaglim = db.Column(db.Float, nullable=False)
    jd = db.Column(db.Float, nullable=False, index=True)
    fid = db.Column(db.Integer, nullable=False)

    @property
    def wall_time(self):
        t = Time(self.jd, format='jd')
        return t.datetime

    @property
    def filter(self):
        return FILTERS[self.fid - 1]

    def serialized(self):
        return {
            'candidate': {
                'id': self.id,
                'objectId': self.objectId,
                'diffmaglim': self.diffmaglim,
                'jd': self.jd,
                'fid': self.fid
            }
        }

    @staticmethod
    def serialize_list(non_detections):
        return [nd.serialized() for nd in non_detections]


class Alert(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    publisher = db.Column(db.String(200), nullable=False, default='')
    objectId = db.Column(db.String(50), index=True)
    alert_candid = db.Column(db.BigInteger, nullable=True, default=None, index=True, unique=True)

    jd = db.Column(db.Float, nullable=False, index=True)
    fid = db.Column(db.Integer, nullable=False)
    pid = db.Column(db.BigInteger, nullable=False)
    diffmaglim = db.Column(db.Float, nullable=True, default=None)
    pdiffimfilename = db.Column(db.String(200), nullable=True, default=None)
    programpi = db.Column(db.String(200), nullable=True, default=None)
    programid = db.Column(db.Integer, nullable=False)
    candid = db.Column(db.BigInteger, nullable=True, default=None)
    isdiffpos = db.Column(db.String(1), nullable=False)
    tblid = db.Column(db.BigInteger, nullable=True, default=None)
    nid = db.Column(db.Integer, nullable=True, default=None)
    rcid = db.Column(db.Integer, nullable=True, default=None)
    field = db.Column(db.Integer, nullable=True, default=None)
    xpos = db.Column(db.Float, nullable=True, default=None)
    ypos = db.Column(db.Float, nullable=True, default=None)
    location = db.Column(Geography('POINT', srid=4035), nullable=False, index=True)
    magpsf = db.Column(db.Float, nullable=False, index=True)
    sigmapsf = db.Column(db.Float, nullable=False, index=True)
    deltamaglatest = db.Column(db.Float, nullable=True, default=None, index=True)
    deltamagref = db.Column(db.Float, nullable=True, default=None, index=True)
    chipsf = db.Column(db.Float, nullable=True, default=None)
    magap = db.Column(db.Float, nullable=True, default=None, index=True)
    sigmagap = db.Column(db.Float, nullable=True, default=None)
    distnr = db.Column(db.Float, nullable=True, default=None)
    magnr = db.Column(db.Float, nullable=True, default=None)
    sigmagnr = db.Column(db.Float, nullable=True, default=None)
    chinr = db.Column(db.Float, nullable=True, default=None)
    sharpnr = db.Column(db.Float, nullable=True, default=None)
    sky = db.Column(db.Float, nullable=True, default=None)
    magdiff = db.Column(db.Float, nullable=True, default=None)
    fwhm = db.Column(db.Float, nullable=True, default=None, index=True)
    classtar = db.Column(db.Float, nullable=True, default=None, index=True)
    mindtoedge = db.Column(db.Float, nullable=True, default=None)
    magfromlim = db.Column(db.Float, nullable=True, default=None)
    seeratio = db.Column(db.Float, nullable=True, default=None)
    aimage = db.Column(db.Float, nullable=True, default=None)
    bimage = db.Column(db.Float, nullable=True, default=None)
    aimagerat = db.Column(db.Float, nullable=True, default=None)
    bimagerat = db.Column(db.Float, nullable=True, default=None)
    elong = db.Column(db.Float, nullable=True, default=None, index=True)
    nneg = db.Column(db.Integer, nullable=True, default=None)
    nbad = db.Column(db.Integer, nullable=True, default=None, index=True)
    rb = db.Column(db.Float, nullable=True, default=None, index=True)
    rbversion = db.Column(db.String(200), nullable=False, default='')
    drb = db.Column(db.Float, nullable=True, default=None, index=True)
    drbversion = db.Column(db.String(200), nullable=False, default='')
    ssdistnr = db.Column(db.Float, nullable=True, default=None, index=True)
    ssmagnr = db.Column(db.Float, nullable=True, default=None)
    ssnamenr = db.Column(db.String(200), nullable=False, default='')
    sumrat = db.Column(db.Float, nullable=True, default=None)
    magapbig = db.Column(db.Float, nullable=True, default=None)
    sigmagapbig = db.Column(db.Float, nullable=True, default=None)
    ranr = db.Column(db.Float, nullable=False)
    decnr = db.Column(db.Float, nullable=False)
    ndethist = db.Column(db.Integer, nullable=False)
    ncovhist = db.Column(db.Integer, nullable=False)
    jdstarthist = db.Column(db.Float, nullable=True, default=None)
    jdendhist = db.Column(db.Float, nullable=True, default=None)
    scorr = db.Column(db.Float, nullable=True)
    tooflag = db.Column(db.SmallInteger, nullable=False, default=0)
    gal_l = db.Column(db.Float, nullable=False, index=True)
    gal_b = db.Column(db.Float, nullable=False, index=True)

    objectidps1 = db.Column(db.BigInteger, nullable=True, default=None, index=True)
    sgmag1 = db.Column(db.Float, nullable=True, default=None)
    srmag1 = db.Column(db.Float, nullable=True, default=None)
    simag1 = db.Column(db.Float, nullable=True, default=None)
    szmag1 = db.Column(db.Float, nullable=True, default=None)
    sgscore1 = db.Column(db.Float, nullable=True, default=None)
    distpsnr1 = db.Column(db.Float, nullable=True, default=None)

    objectidps2 = db.Column(db.BigInteger, nullable=True, default=None, index=True)
    sgmag2 = db.Column(db.Float, nullable=True, default=None)
    srmag2 = db.Column(db.Float, nullable=True, default=None)
    simag2 = db.Column(db.Float, nullable=True, default=None)
    szmag2 = db.Column(db.Float, nullable=True, default=None)
    sgscore2 = db.Column(db.Float, nullable=True, default=None)
    distpsnr2 = db.Column(db.Float, nullable=True, default=None)

    objectidps3 = db.Column(db.BigInteger, nullable=True, default=None, index=True)
    sgmag3 = db.Column(db.Float, nullable=True, default=None)
    srmag3 = db.Column(db.Float, nullable=True, default=None)
    simag3 = db.Column(db.Float, nullable=True, default=None)
    szmag3 = db.Column(db.Float, nullable=True, default=None)
    sgscore3 = db.Column(db.Float, nullable=True, default=None)
    distpsnr3 = db.Column(db.Float, nullable=True, default=None)

    nmtchps = db.Column(db.Integer, nullable=False)
    rfid = db.Column(db.BigInteger, nullable=False)
    jdstartref = db.Column(db.Float, nullable=False)
    jdendref = db.Column(db.Float, nullable=False)
    nframesref = db.Column(db.Integer, nullable=False)

    cutoutScienceFileName = db.Column(db.String(200), nullable=True, default=None)
    cutoutTemplateFileName = db.Column(db.String(200), nullable=True, default=None)
    cutoutDifferenceFileName = db.Column(db.String(200), nullable=True, default=None)
    dsnrms = db.Column(db.Float, nullable=True, default=None)
    ssnrms = db.Column(db.Float, nullable=True, default=None)
    dsdiff = db.Column(db.Float, nullable=True, default=None)
    magzpsci = db.Column(db.Float, nullable=True, default=None)
    magzpsciunc = db.Column(db.Float, nullable=True, default=None)
    magzpscirms = db.Column(db.Float, nullable=True, default=None)
    nmatches = db.Column(db.Integer, nullable=True, default=None)
    clrcoeff = db.Column(db.Float, nullable=True, default=None)
    clrcounc = db.Column(db.Float, nullable=True, default=None)
    zpclrcov = db.Column(db.Float, nullable=True, default=None)
    zpmed = db.Column(db.Float, nullable=True, default=None)
    clrmed = db.Column(db.Float, nullable=True, default=None)
    clrrms = db.Column(db.Float, nullable=True, default=None)
    neargaia = db.Column(db.Float, nullable=True, default=None)
    neargaiabright = db.Column(db.Float, nullable=True, default=None)
    maggaia = db.Column(db.Float, nullable=True, default=None)
    maggaiabright = db.Column(db.Float, nullable=True, default=None)
    exptime = db.Column(db.Float, nullable=True, default=None)

    @property
    def ra(self):
        ra = shape.to_shape(self.location).x
        if ra < 0:
            ra = ra + 360
        return ra

    @property
    def dec(self):
        return shape.to_shape(self.location).y

    @property
    def prv_candidate(self):
        point = db.session.scalar(self.location.ST_AsText())
        query = db.session.query(Alert).filter(
            Alert.id != self.id,
            Alert.location.ST_DWithin(f'srid=4035;{point}', degrees_to_meters(PRV_CANDIDATES_RADIUS))
        )
        return query.order_by(Alert.jd.desc())

    @property
    def non_detection(self):
        query = db.session.query(NonDetection).filter(
            NonDetection.objectId == self.objectId
        )
        return query.order_by(NonDetection.jd.desc())

    @property
    def wall_time(self):
        t = Time(self.jd, format='jd')
        return t.datetime

    @property
    def wall_time_format(self):
        return '{0}/{1}/{2}'.format(
            self.wall_time.year, str(self.wall_time.month).zfill(2), str(self.wall_time.day).zfill(2)
        )

    @property
    def avro(self):
        key = '{0}/{1}.avro'.format(self.wall_time_format, self.alert_candid)
        return generate_presigned_url(key)

    @property
    def avro_packet(self):
        response = requests.get(self.avro)
        freader = fastavro.reader(io.BytesIO(response.content))
        for packet in freader:
            if packet['candidate']['candid'] == self.candid:
                return packet
        return None

    @property
    def cutoutScience(self):
        return self.avro_packet['cutoutScience']

    @property
    def cutoutTemplate(self):
        return self.avro_packet['cutoutTemplate']

    @property
    def cutoutDifference(self):
        return self.avro_packet['cutoutDifference']

    def serialized(self, prv_candidate=False):
        alert = {
            'lco_id': self.id,
            'objectId': self.objectId,
            'publisher': self.publisher,
            'candid': self.alert_candid,
            'avro': self.avro,
            'candidate': {
                'jd': self.jd,
                'wall_time': self.wall_time,
                'fid': self.fid,
                'filter': self.filter,
                'pid': self.pid,
                'diffmaglim': self.diffmaglim,
                'pdiffimfilename': self.pdiffimfilename,
                'programpi': self.programpi,
                'programid': self.programid,
                'candid': self.candid,
                'isdiffpos': self.isdiffpos,
                'tblid': self.tblid,
                'nid': self.nid,
                'rcid': self.rcid,
                'field': self.field,
                'xpos': self.xpos,
                'ypos': self.ypos,
                'ra': self.ra,
                'dec': self.dec,
                'l': self.gal_l,
                'b': self.gal_b,
                'magpsf': self.magpsf,
                'sigmapsf': self.sigmapsf,
                'deltamaglatest': self.deltamaglatest,
                'deltamagref': self.deltamagref,
                'chipsf': self.chipsf,
                'magap': self.magap,
                'distnr': self.distnr,
                'sigmagap': self.sigmagap,
                'magnr': self.magnr,
                'sigmagnr': self.sigmagnr,
                'chinr': self.chinr,
                'sharpnr': self.sharpnr,
                'sky': self.sky,
                'magdiff': self.magdiff,
                'fwhm': self.fwhm,
                'classtar': self.classtar,
                'mindtoedge': self.mindtoedge,
                'magfromlim': self.magfromlim,
                'seeratio': self.seeratio,
                'aimage': self.aimage,
                'bimage': self.bimage,
                'aimagerat': self.aimagerat,
                'bimagerat': self.bimagerat,
                'elong': self.elong,
                'nneg': self.nneg,
                'nbad': self.nbad,
                'rb': self.rb,
                'rbversion': self.rbversion,
                'drb': self.drb,
                'drbversion': self.drbversion,
                'ssdistnr': self.ssdistnr,
                'ssmagnr': self.ssmagnr,
                'ssnamenr': self.ssnamenr,
                'sumrat': self.sumrat,
                'magapbig': self.magapbig,
                'sigmagapbig': self.sigmagapbig,
                'ranr': self.ranr,
                'decnr': self.decnr,
                'ndethist': self.ndethist,
                'ncovhist': self.ncovhist,
                'jdstarthist': self.jdstarthist,
                'jdendhist': self.jdendhist,
                'scorr': self.scorr,
                'tooflag': self.tooflag,
                'objectidps1': self.objectidps1,
                'sgmag1': self.sgmag1,
                'srmag1': self.srmag1,
                'simag1': self.simag1,
                'szmag1': self.szmag1,
                'sgscore1': self.sgscore1,
                'distpsnr1': self.distpsnr1,
                'objectidps2': self.objectidps2,
                'sgmag2': self.sgmag2,
                'srmag2': self.srmag2,
                'simag2': self.simag2,
                'szmag2': self.szmag2,
                'sgscore2': self.sgscore2,
                'distpsnr2': self.distpsnr2,
                'objectidps3': self.objectidps3,
                'sgmag3': self.sgmag3,
                'srmag3': self.srmag3,
                'simag3': self.simag3,
                'szmag3': self.szmag3,
                'sgscore3': self.sgscore3,
                'distpsnr3': self.distpsnr3,
                'nmtchps': self.nmtchps,
                'rfid': self.rfid,
                'jdstartref': self.jdstartref,
                'jdendref': self.jdendref,
                'nframesref': self.nframesref,
                'dsnrms': self.dsnrms,
                'ssnrms': self.ssnrms,
                'dsdiff': self.dsdiff,
                'magzpsci': self.magzpsci,
                'magzpsciunc': self.magzpsciunc,
                'magzpscirms': self.magzpscirms,
                'nmatches': self.nmatches,
                'clrcoeff': self.clrcoeff,
                'clrcounc': self.clrcounc,
                'zpclrcov': self.zpclrcov,
                'zpmed': self.zpmed,
                'clrmed': self.clrmed,
                'clrrms': self.clrrms,
                'neargaia': self.neargaia,
                'neargaiabright': self.neargaiabright,
                'maggaia': self.maggaia,
                'maggaiabright': self.maggaiabright,
                'exptime': self.exptime,
            }
        }
        if prv_candidate:
            prv_candidate = Alert.serialize_list(self.prv_candidate)
            non_detections = NonDetection.serialize_list(self.non_detection)
            Alert.add_candidates(prv_candidate, non_detections)
            alert['prv_candidate'] = prv_candidate
        return alert

    @staticmethod
    def add_candidates(candidates, new_candidates):
        for candidate in new_candidates:
            low = 0
            high = len(candidates)
            while low < high:
                mid = (low+high)//2
                if candidates[mid]['candidate']['jd'] < candidate['candidate']['jd']:
                    low = mid+1
                else:
                    high = mid
            candidates.insert(low, candidate)
        return candidates

    def get_photometry(self):
        filter_mapping = ['g', 'r', 'i']
        prv_candidates = Alert.serialize_list(self.prv_candidate)
        non_detections = NonDetection.serialize_list(self.non_detection)
        Alert.add_candidates(prv_candidates, non_detections)
        photometry = {}
        index = 0
        for candidate in prv_candidates:
            values = candidate['candidate']
            photometry[index] = {}
            for key in values.keys():
                if key in ['jd', 'diffmaglim', 'magpsf', 'sigmapsf']:
                    photometry[index][key] = values[key]
                elif key == 'fid':
                    photometry[index]['filter'] = filter_mapping[values[key] - 1]
            index += 1
        photometry[index] = {
            'jd': self.jd,
            'filter': filter_mapping[self.fid - 1],
            'magpsf': self.magpsf,
            'sigmapsf': self.sigmapsf,
            'diffmaglim': self.diffmaglim
        }
        return photometry

    @staticmethod
    def serialize_list(alerts):
        return [alert.serialized() for alert in alerts]

    @property
    def pretty_serialized(self):
        return json.dumps(self.serialized(prv_candidate=True), indent=2)

    @property
    def filter(self):
        return FILTERS[self.fid - 1]

    def __str__(self):
        return self.objectId


def degrees_to_meters(degrees):
    return 2 * math.pi * EARTH_RADIUS_METERS * degrees / 360


def apply_filters(query, request_args):
    # Perfom a cone search. Paramter is comma seperated ra, dec origin and radius to search. Ex: ?cone=23,29,0.5
    if request_args.get('cone'):
        ra, dec, radius = request_args['cone'].split(',')
        query = query.filter(
            Alert.location.ST_DWithin(f'srid=4035;POINT({ra} {dec})', degrees_to_meters(float(radius)))
        )

    if request_args.get('objectcone'):
        objectname, radius = request_args['objectcone'].split(',')
        ra, dec = get_simbad2k_coords(objectname)
        query = query.filter(
            Alert.location.ST_DWithin(f'srid=4035;POINT({ra} {dec})', degrees_to_meters(float(radius)))
        )

    # Return alerts with an RA greater than a given value in degrees. Ex: ?ra__gt=20
    if request_args.get('ra__gt'):
        ra = float(request_args['ra__gt'])
        if ra > 180:
            ra = ra - 360
        query = query.filter(cast(Alert.location, Geometry).ST_X() > ra)

    # Return alerts with an RA less than a given value in degrees. Ex: ?ra__lt=20
    if request_args.get('ra__lt'):
        ra = float(request_args['ra__lt'])
        if ra > 180:
            ra = ra - 360
        query = query.filter(cast(Alert.location, Geometry).ST_X() < ra)

    # Return alerts with an Dec greater than a given value in degrees. Ex: ?dec__gt=20
    if request_args.get('dec__gt'):
        query = query.filter(cast(Alert.location, Geometry).ST_Y() > float(request_args['dec__gt']))

    # Return alerts with an RA less than a given value in degrees. Ex: ?dec__lt=20
    if request_args.get('dec__lt'):
        query = query.filter(cast(Alert.location, Geometry).ST_Y() < float(request_args['dec__lt']))

    # Return alerts with galactic l greater than a given value in degrees. Ex: ?l__gt=20
    if request_args.get('l__gt'):
        query = query.filter(Alert.gal_l > float(request_args['l__gt']))

    # Return alerts with galactic l less than a given value in degrees. Ex: ?l__lt=20
    if request_args.get('l__lt'):
        query = query.filter(Alert.gal_l < float(request_args['l__lt']))

    # Return alerts with galactic b greater than a given value in degrees. Ex: ?b__gt=20
    if request_args.get('b__gt'):
        query = query.filter(Alert.gal_b > float(request_args['b__gt']))

    # Return alerts with galactic b less than a given value in degrees. Ex: ?b__lt=20
    if request_args.get('b__lt'):
        query = query.filter(Alert.gal_b < float(request_args['b__lt']))

    # Return alerts with a wall time after given date. Ex: ?time__gt=2018-07-17
    if request_args.get('time__gt'):
        a_time = Time(request_args['time__gt'], format='isot')
        query = query.filter(Alert.jd > a_time.jd)

    # Return alerts with a JD after given date. Ex: ?jd__gt=2458302.6906713
    if request_args.get('jd__gt'):
        query = query.filter(Alert.jd > request_args['jd__gt'])

    # Return alerts with a wall time previous to a given date. Ex: ?time__lt=2018-07-17
    if request_args.get('time__lt'):
        a_time = Time(request_args['time__lt'], format='isot')
        query = query.filter(Alert.jd < a_time.jd)

    # Return alerts with a JD previous to a given date. Ex: ?jd__lt=2458302.6906713
    if request_args.get('jd__lt'):
        query = query.filter(Alert.jd < request_args['jd__lt'])

    # Return alerts from the previous x seconds
    if request_args.get('time__since'):
        time_since = datetime.utcnow() - timedelta(seconds=int(request_args['time__since']))
        a_time = Time(time_since)
        query = query.filter(Alert.jd > a_time.jd)

    if request_args.get('filter'):
        query = query.filter(Alert.fid == FILTERS.index(request_args['filter']) + 1)

    # Return alerts with a brightness greater than the given value. Ex: ?magpsf__lt=20
    if request_args.get('magpsf__lte'):
        query = query.filter(Alert.magpsf <= float(request_args['magpsf__lte']))

    if request_args.get('magpsf__gte'):
        query = query.filter(Alert.magpsf >= float(request_args['magpsf__gte']))

    # Return alerts with a brightness uncertainty less than the given value. Ex: ?sigmapsf__lte=0.4
    if request_args.get('sigmapsf__lte'):
        query = query.filter(Alert.sigmapsf <= float(request_args['sigmapsf__lte']))

    # Return alerts with a magnitude of object in difference image less than value. ex: ?magap__lte=0.4
    if request_args.get('magap__lte'):
        query = query.filter(Alert.magap <= float(request_args['magap__lte']))

    if request_args.get('magap__gte'):
        query = query.filter(Alert.magap >= float(request_args['magap__gte']))

    # Return alerts where the distance to the nearest source is less than value. ex: ?distnr__lte=1.0
    if request_args.get('distnr__lte'):
        query = query.filter(Alert.distnr <= float(request_args['distnr__lte']))

    if request_args.get('distnr__gte'):
        query = query.filter(Alert.distnr >= float(request_args['distnr__gte']))

    # Return alerts with a magnitude difference greater than the given value (abs value). Ex: ?deltamaglatest__gte=1
    if request_args.get('deltamaglatest__gte'):
        query = query.filter(Alert.deltamaglatest >= float(request_args['deltamaglatest__gte']))
    if request_args.get('deltamaglatest__lte'):
        query = query.filter(Alert.deltamaglatest <= float(request_args['deltamaglatest__lte']))

    # Return alerts with a mag diff on the reference image greater than the given value. Ex: ?deltamagref__gte=1
    if request_args.get('deltamagref__gte'):
        query = query.filter(Alert.deltamagref >= float(request_args['deltamagref__gte']))

    if request_args.get('deltamagref__lte'):
        query = query.filter(Alert.deltamagref <= float(request_args['deltamagref__lte']))

    # Return alerts with a real/bogus score greater or equal to the given value. Ex: ?rb__gte=0.3
    if request_args.get('rb__gte'):
        query = query.filter(Alert.rb >= float(request_args['rb__gte']))

    # Return alerts with a deep learning real/bogus score greater or equal to the given value. Ex: ?drb__gte=0.3
    if request_args.get('drb__gte'):
        query = query.filter(Alert.drb >= float(request_args['drb__gte']))

    # Return alerts with a start/galaxy score greater or equal to the given value. Ex: ?clastar__gte=0.4
    if request_args.get('classtar__gte'):
        query = query.filter(Alert.classtar >= float(request_args['classtar__gte']))

    # Return alerts with a start/galaxy score less or equal to the given value. Ex: ?clastar__lte=0.4
    if request_args.get('classtar__lte'):
        query = query.filter(Alert.classtar <= float(request_args['classtar__lte']))

    # Return alerts with a fwhm less than the given value. Ex: ?fwhm__lte=1.123
    if request_args.get('fwhm__lte'):
        query = query.filter(Alert.fwhm <= float(request_args['fwhm__lte']))

    # Return alerts with a number of bad pixels less than or equal to the given value. Ex: ?nbad__lte=3
    if request_args.get('nbad__lte'):
        query = query.filter(Alert.nbad <= request_args['nbad__lte'])

    # Return alerts with a elong less than or equal to the given value. Ex: ?elong__lte=1.2
    if request_args.get('elong__lte'):
        query = query.filter(Alert.elong <= float(request_args['elong__lte']))

    if request_args.get('objectId'):
        query = query.filter(Alert.objectId == request_args['objectId'])

    if request_args.get('candid'):
        query = query.filter(Alert.alert_candid == request_args['candid'])

    # Search for alerts near a PS1 object ID. Ex: ?objectidps=178183210973037920
    if request_args.get('objectidps'):
        psid = int(request_args['objectidps'])
        query = query.filter((Alert.objectidps1 == psid) | (Alert.objectidps2 == psid) | (Alert.objectidps3 == psid))

    sort_by = request_args.get('sort_value', 'jd')
    sort_order = request_args.get('sort_order', 'desc')

    if sort_order == 'desc':
        query = query.order_by(getattr(Alert, sort_by).desc())
    elif sort_order == 'asc':
        query = query.order_by(getattr(Alert, sort_by).asc())

    return query


def request_wants_json():
    if request.args.get('format', 'html', type=str) == 'json':
        return True
    else:
        best = request.accept_mimetypes.best_match(['application/json', 'text/html'])
        return best == 'application/json' and \
            request.accept_mimetypes[best] > \
            request.accept_mimetypes['text/html']


def get_simbad2k_coords(objectname):
    response = requests.get(f'https://simbad2k.lco.global/{objectname}?target_type=sidereal')
    response.raise_for_status()
    result = response.json()
    return result['ra_d'], result['dec_d']


@app.route('/help/')
def help():
    return render_template('help.html')


@app.route('/<int:id>/')
def alert_detail(id):
    alert = db.session.query(Alert).get(id)
    if request_wants_json():
        return jsonify(alert.serialized(prv_candidate=True))
    else:
        return render_template('detail.html', alert=alert)


@app.route('/<int:id>/photometry/')
def alert_photometry(id):
    alert = db.session.query(Alert).get(id)
    return jsonify(alert.get_photometry())


@app.route('/<int:id>/cutout/<cutout>/')
def cutoutScience(id, cutout):
    if cutout not in ['Science', 'Template', 'Difference']:
        abort(404)

    alert = db.session.query(Alert).get(id)
    cutout_file = getattr(alert, 'cutout' + cutout)
    return send_file(
        io.BytesIO(cutout_file['stampData']),
        mimetype='image/fits',
        as_attachment=True,
        attachment_filename=cutout_file['fileName']
    )


@app.route('/', methods=['GET', 'POST'])
def alerts():
    forwarded_ips = request.headers.getlist('X-Forwarded-For')
    client_ip = forwarded_ips[0].split(',')[0] if len(forwarded_ips) >= 1 else ''
    logger.info('Incoming request', extra={'tags': {'requesting_ip': client_ip, 'request_args': request.args}})
    page = request.args.get('page', 1, type=int)
    response = {}

    if request.method == 'GET':
        query = db.session.query(Alert)
        query = apply_filters(query, request.args)
        latest = db.session.query(Alert).order_by(Alert.jd.desc()).first()
        paginator = query.paginate(page, 100, True, count=False)
        response = {
            'has_next': paginator.has_next,
            'has_prev': paginator.has_prev,
            'results': Alert.serialize_list(paginator.items)
        }

    if request.method == 'POST':
        searches = request.get_json().get('queries')
        search_results = []
        total = 0
        for search_args in searches:
            search_result = {}
            query = db.session.query(Alert)
            query = apply_filters(query, search_args)
            search_result['query'] = search_args
            search_result['num_alerts'] = query.count()
            search_result['results'] = Alert.serialize_list(query.all())
            search_results.append(search_result)
            total += search_result['num_alerts']
        response = {
            'total': total,
            'results': search_results
        }

    if request_wants_json() or request.method == 'POST':
        return jsonify(response)
    else:
        args = request.args.copy()
        try:
            args.pop('page')
        except KeyError:
            pass
        arg_str = urlencode(args)
        return render_template('index.html', context=response, page=paginator.page, arg_str=arg_str, latest=latest)
