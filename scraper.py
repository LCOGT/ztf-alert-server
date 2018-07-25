import requests
import os
import boto3
import urllib.request
import ingest
import logging
from bs4 import BeautifulSoup
from apscheduler.schedulers.blocking import BlockingScheduler

BUCKET_NAME = os.getenv('S3_BUCKET')

session = boto3.Session()

s3 = session.resource('s3')

ZTF_BASE_URL = 'https://ztf.uw.edu/alerts/public/'
INGESTED_FILES_LIST_FILENAME = 'ingested_files.csv'

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)


def get_published_alert_files():
    html = requests.get(ZTF_BASE_URL)
    soup = BeautifulSoup(html.text, 'html.parser')
    files_table = soup.table.find_all('tr')
    processed_links = []
    for tr in files_table:
        tds = tr.find_all('td')
        if len(tds) > 0:  # skip the table header
            filename = ZTF_BASE_URL + tds[1].a['href']
            last_modified = tds[2].string
            file_size = requests.head(filename).headers.get('Content-Length', None)
            processed_links.append([filename, last_modified.strip().replace(' ', '-'), file_size])
    return processed_links


def get_ingested_alerts_file():
    try:
        s3.Object(BUCKET_NAME).download_file(INGESTED_FILES_LIST_FILENAME, 'ingested_files.csv')
    except:
        print("Failed to download ingested files list")


def push_ingested_alerts_file():
    try:
        s3.Object(BUCKET_NAME, INGESTED_FILES_LIST_FILENAME).put(Body=open(INGESTED_FILES_LIST_FILENAME, 'rb'))
        s3.Bucket(BUCKET_NAME).upload_file(INGESTED_FILES_LIST_FILENAME, INGESTED_FILES_LIST_FILENAME)
    except:
        logger.error("Unable to push file of ingested alerts")



def perform_file_processing(new_alerts):
    with open('ingested_files.csv', 'a') as ingested_files:
        writer = csv.writer(ingested_files)
        for file_to_ingest in new_alerts:
            success = read_avros(file_to_ingest[0])
            if success:
                writer.writerow([file_to_ingest])
                logger.info("Successfully processed alert file {0}", file_to_ingest[0])


def process_new_files():
    new_alerts = []
    ingested_alerts_file = get_ingested_alerts_file()
    published_alerts = get_published_alert_files()
    for pa in published_alerts:
        with csv.reader(open(INGESTED_FILES_LIST_FILENAME), delimiter=" ") as file_reader:
            for row in file_reader:
                if pa[0] == row[0]: # check if published file name exists in log of ingested files
                    if published_file[1] != row[1]: # check if published file size is the same as the previously ingested one
                        pass
                        # do nothing until we can properly ingest partial tarballs
                        # new_alerts.append(pa[0])
                elif pa[2] > 44: # if published file name is not in the log, ensure it isn't empty
                    new_alerts.append(pa[0])

    perform_file_processing(new_alerts)
    push_ingested_alerts_file()


def test_schedule():
    print("test")