#!/bin/env/python
import sys
import requests
import tarfile
import io

from ingest import logger, BUCKET_NAME, s3


def member_key(url):
    filename = url.split('/')[-1]
    date_str = filename.split('_')[-1][:8]
    year = date_str[:4]
    month = date_str[4:6]
    day = date_str[6:8]
    return f'{year}/{month}/{day}/'


def read_avros(url):
    with requests.get(url, stream=True) as response:
        with tarfile.open(fileobj=response.raw, mode='r|gz') as tar:
            while True:
                member = tar.next()
                if member is None:
                    logger.info('Done uploading avros')
                    break
                with tar.extractfile(member) as f:
                    if f:
                        f_data = f.read()
                        key = member_key(url) + member.name
                        s3.Object(BUCKET_NAME, key).put(
                            Body=io.BytesIO(f_data),
                            ContentDisposition=f'attachment; filename={key}',
                            ContentType='avro/binary'
                        )
                        logger.info('Uploaded %s to s3', key)
            logger.info('done reading tar')


if __name__ == '__main__':
    url = sys.argv[1]
    read_avros(url)
