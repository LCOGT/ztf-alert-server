# MARS
Make Alerts Really Simple

This repository contains the source code behind [the MARS broker](https://mars.lco.global) for alerts from the Zwicky Transient Facility.

Please note this code is in a state of flux as the interface between ZTF and MARS continues to evolve.

### Requirements
* Python 3.6+
* Postgresql 10 with PostGIS installed.
* Redis
* Amazon S3

### Libraries used
* [Flask](http://flask.pocoo.org/)
* [Flask-SqlAlchemy](http://flask-sqlalchemy.pocoo.org/2.3/)
* [GeoAlchemy2](https://geoalchemy-2.readthedocs.io/en/latest/)
* [PostGIS](https://postgis.net/)
* [Astropy](http://www.astropy.org/)
* [fastavro](https://github.com/fastavro/fastavro)
* [Dramatiq](https://dramatiq.io/)
* [JS9](https://js9.si.edu/)
* [Plotly](https://plot.ly/)


### Layout
[ztf.py](ztf.py) Is the main entrypoint for the webapp. It contains the Flask endpoints as well as the SqlAlchemy schema and database connections.

[ingest.py](ingest.py) Contains the code to download and parse [the tarballs from ztf](https://ztf.uw.edu/alerts/public/) that contain the individual alerts. It farms out the ingest of each file to one of several Dramatiq workers in order to speed up the process.

[templates/](templates/) contains the server side rendered html templates for the web ui.

### How it works

The ingester streams the
https response from a request to the ztf tar.gz file. While this response is streaming the tar file is
constructed in memory and each file in it read sequentially. A task is created for each file which
is then executed by one of the Dramatiq workers.

Each worker inserts a record into the database as well as uploads the original avro file to Amazon s3.
The database schema resembles a flattened avro alert, so inserting a records is really just a matter
of parsing it with fastavro and sending it to Postgres.

Meanwhile the flask app is serving incoming http requests with some simple endpoints to return results from
the single `alert` table.

One exception is the handling of the image cutouts: to display them in the browser the backend makes a request to download
an alert's original avro file, opens it in memory with fastavro, and returns the binary to the browser.
The browser handles decompressing the file (since it is gzipped) and then JS9 has no trouble displaying it.

### Handling spatial queries

MARS allows for cone searches on alerts. "Previous Alerts" is also implemented as a cone search on a single
alert's location.

This is implemented using a spatial index on ra/dec, or since it is created by PostGIS, lat/long. Since PostGIS
is mainly used for location on earth, it takes a little tweaking to make it accurate for astronomy.

First, since we are calculating distance we want to use spherical instead of planar geometry. PostGIS makes this
easy with it's Geography type. So RA/Dec is stored as a POINT type in the alert table.

If you are using the Geography type, PostGIS also specifies a SRID to use for calculations and projections. By
default SRID 4326 is used, which is great if you're dealing with locations on the ellipsoidal earth. The
celestial sphere is not an ellipsoid, though, so we want to use an SRID that represents a perfectly spherical
earth. It is surprisingly hard to fine one, actually, but luckily there exists a deprecated (😁)
[SRID 4035](https://epsg.io/4035) which is spherical. So in various places in the code you'll want to make sure
to specify this SRID.

Lastly, when using the Geography type, PostGIS will be helpful and use meters instead of degrees as the inputs
and results for all operations. Boo! Since we want to specify degrees, we'll need to convert degrees to meters.
This requires knowing the radius of the earth that PostGIS uses. You can find the
[magic constant](https://github.com/postgis/postgis/blob/svn-trunk/liblwgeom/liblwgeom.h.in#L130) inside of the
source code for PostGIS.

PostGIS seems well suited for this purpose, it is very fast and well supported. There is a
[docker image](https://hub.docker.com/r/mdillon/postgis/) available to give it a try. It's even supported
by amazon RDS.


### Questions, comments?
[Contact me](mailto:ariba@lco.global)
