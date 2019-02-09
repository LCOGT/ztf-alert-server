from ingest import do_ingest
from ztf import db, logger
import sys
import requests
import tarfile
import base64
from datetime import datetime, timedelta


ZTF_ALERT_ARCHIVE = 'https://ztf.uw.edu/alerts/public/'


def get_ztf_url(date):
    return '{0}ztf_public_{1}.tar.gz'.format(ZTF_ALERT_ARCHIVE, datetime.strftime(date, '%Y%m%d'))


def read_avros(url):
    with requests.get(url, stream=True) as response:
        with tarfile.open(fileobj=response.raw, mode='r|gz') as tar:
            while True:
                member = tar.next()
                if member is None:
                    logger.info('Done ingesting this package')
                    break
                with tar.extractfile(member) as f:
                    if f:
                        fencoded = base64.b64encode(f.read()).decode('UTF-8')
                        do_ingest(fencoded)
            logger.info('done sending tasks', extra={'tags': {'processed_tarball': url}})

if __name__ == '__main__':
    db.create_all()
    start_date = datetime.strptime(sys.argv[1], '%Y%m%d')
    end_date = datetime.strptime(sys.argv[2], '%Y%m%d')
    for i in range(0, (end_date - start_date).days + 1):
        url = get_ztf_url(start_date + timedelta(days=i))
        read_avros(url)

