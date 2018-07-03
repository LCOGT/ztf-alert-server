from flask import Flask, jsonify, request, render_template
from flask_sqlalchemy import SQLAlchemy
from geoalchemy2 import Geography, Geometry
from sqlalchemy import cast
from urllib.parse import urlencode
import math
import json

"""
Convert degrees to meters so we can use geography type:
2 * PI * 6371008.77141506 * DEGREES / 360

SRID for normal sphere: https://epsg.io/4035

"""
EARTH_RADIUS_METERS = 6371008.77141506

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql+psycopg2://postgres:postgres@localhost:5432/ztf'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class Alert(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    publisher = db.Column(db.String(200), nullable=False, default='')
    objectId = db.Column(db.String(50), index=True)
    alert_candid = db.Column(db.BigInteger, nullable=True, default=None)

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
    chipsf = db.Column(db.Float, nullable=True, default=None)
    magap = db.Column(db.Float, nullable=True, default=None)
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
    nbad = db.Column(db.Integer, nullable=True, default=None)
    rb = db.Column(db.Float, nullable=True, default=None, index=True)
    rbversion = db.Column(db.String(200), nullable=False, default='')
    ssdistnr = db.Column(db.Float, nullable=True, default=None)
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
    scorr = db.Column(db.Float, nullable=True, index=True)
    tooflag = db.Column(db.SmallInteger, nullable=False, default=0)

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

    @property
    def ra(self):
        ra = db.session.scalar(cast(self.location, Geometry).ST_X())
        if ra < 0:
            ra = ra + 360
        return ra

    @property
    def dec(self):
        return db.session.scalar(cast(self.location, Geometry).ST_Y())

    @property
    def serialized(self):
        return {
            'objectId': self.objectId,
            'publisher': self.publisher,
            'candid': self.alert_candid,
            'candidate': {

                'jd': self.jd,
                'fid': self.fid,
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
                'location': json.loads(db.session.scalar(self.location.ST_AsGeoJSON())),
                'magpsf': self.magpsf,
                'sigmapsf': self.sigmapsf,
                'chipsf': self.chipsf,
                'magap': self.magap,
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
            }
        }

    @staticmethod
    def serialize_list(alerts):
        return [alert.serialized for alert in alerts]

    def __str__(self):
        return self.objectId


def degrees_to_meters(degrees):
    return 2 * math.pi * EARTH_RADIUS_METERS * degrees / 360


def apply_filters(query, request):
    # Perfom a cone search. Paramter is comma seperated ra, dec origin and radius to search. Ex: ?cone=23,29,0.5
    if request.args.get('cone'):
        ra, dec, radius = request.args['cone'].split(',')
        query = query.filter(Alert.location.ST_DWithin(f'srid=4035;POINT({ra} {dec})', degrees_to_meters(float(radius))))

    # Return alerts with an RA greater than a given value in degrees. Ex: ?ra__gt=20
    if request.args.get('ra__gt'):
        ra = float(request.args['ra__gt'])
        if ra > 180:
            ra = ra - 360
        query = query.filter(cast(Alert.location, Geometry).ST_X() > ra)

    # Return alerts with an RA less than a given value in degrees. Ex: ?ra__lt=20
    if request.args.get('ra__lt'):
        ra = float(request.args['ra__lt'])
        if ra > 180:
            ra = ra - 360
        query = query.filter(cast(Alert.location, Geometry).ST_X() < ra)

    # Return alerts with an Dec greater than a given value in degrees. Ex: ?dec__gt=20
    if request.args.get('dec__gt'):
        query = query.filter(cast(Alert.location, Geometry).ST_Y() > float(request.args['dec__gt']))

    # Return alerts with an RA less than a given value in degrees. Ex: ?dec__lt=20
    if request.args.get('dec__lt'):
        query = query.filter(cast(Alert.location, Geometry).ST_Y() < float(request.args['dec__lt']))

    # Return alerts with a JD after given date. Ex: ?jd__gt=2458302.6906713
    if request.args.get('jd__gt'):
        query = query.filter(Alert.jd > request.args['jd__gt'])

    # Return alerts with a JD prevous to a given date. Ex: ?jd__lt=2458302.6906713
    if request.args.get('jd__lt'):
        query = query.filter(Alert.jd < request.args['jd__lt'])

    # Return alerts with a brightness greater than the given value. Ex: ?magpsf__lt=20
    if request.args.get('magpsf__lte'):
        query = query.filter(Alert.magpsf <= float(request.args['magpsf__lte']))

    # Return alerts with a brightness uncertainty less than the given value. Ex: ?sigmapsf__lte=0.4
    if request.args.get('sigmapsf__lte'):
        query = query.filter(Alert.sigmapsf <= float(request.args['sigmapsf__lte']))

    # Return alerts with a real/bogus score greater or equal to the given value. Ex: ?rb__gte=0.3
    if request.args.get('rb__gte'):
        query = query.filter(Alert.rb >= float(request.args['rb__gte']))

    # Return alerts with a start/galaxy score greater or equal to the given value. Ex: ?clastar__gte=0.4
    if request.args.get('classtar__gte'):
        query = query.filter(Alert.classtar >= float(request.args['classtar__gte']))

    # Return alerts with a start/galaxy score less or equal to the given value. Ex: ?clastar__lte=0.4
    if request.args.get('classtar__lte'):
        query = query.filter(Alert.classtar <= float(request.args['classtar__lte']))

    # Return alerts with a fwhm less than the given value. Ex: ?fwhm__lte=1.123
    if request.args.get('fwhm__lte'):
        query = query.filter(Alert.fwhm <= float(request.args['fwhm__lte']))

    # Return alerts with a signal to noise ratio grater than the given value. Ex: ?scorr__gte=25
    if request.args.get('scorr__gte'):
        query = query.filter(Alert.scorr >= float(request.args['scorr__gte']))

    if request.args.get('objectId'):
        query = query.filter(Alert.objectId == request.args['objectId'])

    # Search for alerts near a PS1 object ID. Ex: ?objectidps=178183210973037920
    if request.args.get('objectidps'):
        psid = int(request.args['objectidps'])
        query = query.filter((Alert.objectidps1 == psid) | (Alert.objectidps2 == psid) | (Alert.objectidps3 == psid))
    return query

def request_wants_json():
    best = request.accept_mimetypes.best_match(['application/json', 'text/html'])
    return best == 'application/json' and \
        request.accept_mimetypes[best] > \
        request.accept_mimetypes['text/html']


@app.route('/')
def alerts():
    page = request.args.get('page', 1, type=int)
    query = db.session.query(Alert)
    query = apply_filters(query, request).order_by(Alert.jd.desc())

    paginator = query.paginate(page, 20, True)
    response = {
        'total': paginator.total,
        'pages': paginator.pages,
        'has_next': paginator.has_next,
        'has_prev': paginator.has_prev,
        'results': Alert.serialize_list(paginator.items)

    }
    if request_wants_json() or request.args.get('format', 'html', type=str) == 'json':
        return jsonify(response)
    else:
        args = request.args.copy()
        try:
            args.pop('page')
        except KeyError:
            pass
        arg_str = urlencode(args)
        return render_template('index.html', context=response, page=paginator.page, arg_str=arg_str)
