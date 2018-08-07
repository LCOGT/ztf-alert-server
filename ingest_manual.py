from ingest import do_ingest
from ztf import db, logger
import sys
import requests
import tarfile
import base64

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
            logger.info('done sending tasks')

if __name__ == '__main__':
    db.create_all()
    url = sys.argv[1]
    read_avros(url)