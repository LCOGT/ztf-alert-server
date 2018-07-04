FROM python:3.6-slim
MAINTAINER Austin Riba <ariba@lco.global>

EXPOSE 8080
ENTRYPOINT [ "/usr/local/bin/gunicorn", "ztf:app", "-b", "0.0.0.0:8080", "--access-logfile", "-", "--error-logfile", "-", "-k", "gevent", "--timeout", "300", "--workers", "4" ]
WORKDIR /ztf

COPY requirements.txt /ztf

RUN pip --no-cache-dir --trusted-host=buildsba.lco.gtn install gunicorn[gevent] -r /ztf/requirements.txt

COPY . /ztf
