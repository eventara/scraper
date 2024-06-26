import threading
from flask import Flask
from time import sleep
from threading import Thread
from datetime import datetime, date
import requests
from pymongo import MongoClient
import random
import os
import google.cloud.logging
from google.cloud import storage
from slugify import slugify
import json
import concurrent.futures
import textwrap

try:
    log_client = google.cloud.logging.Client()
    log_client.setup_logging()
except Exception as e:
    print("Running without google cloud logging")

app = Flask(__name__)

client = MongoClient(os.environ.get("MONGODB_URI"))
proxy = os.environ.get("PROXY_URI")
db = client['db']

service_uri = os.environ.get("SERVICE_URI")
verify_token = os.environ.get("VERIFY_TOKEN")
service_account_info = json.loads(os.environ.get("GCP_SERVICE_ACCOUNT"))

TWELVE_HOURS = 12*60*60

client = storage.Client.from_service_account_info(service_account_info)
adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
client._http.mount("https://", adapter)
client._http._auth_request.session.mount("https://", adapter)

BUCKET_NAME = 'eventara-images'
bucket = client.bucket(BUCKET_NAME)

def update_image_url(doc):
    image_content = requests.get(doc['image_url']).content

    blob = bucket.blob(f"posts/{date.today().year}-{date.today().month}/{doc['_id']}.jpg")
    blob.upload_from_string(image_content, content_type='image/jpeg')
    blob.cache_control = 'public, max-age=31536000'
    blob.make_public()

    doc['image_url'] = blob.public_url

def get_instagramaccounts():
    instagramaccount_collection = db['instagramaccounts']
    return list(instagramaccount_collection.find({
        '$or': [
            {'last_fetched_at': {'$lt': datetime.fromtimestamp(
                int(datetime.utcnow().timestamp()) - TWELVE_HOURS)}},
            {'last_fetched_at': {'$exists': False}}
        ]
    }))


def update_instagramaccount_timestamp(username):
    instagramaccount_collection = db['instagramaccounts']
    instagramaccount_collection.find_one_and_update(
        {'username': username},
        {
            '$set': {'last_fetched_at': datetime.utcnow().replace(microsecond=0)}
        }
    )

def upsert_docs(post_docs):
    to_be_inserted = []

    for doc in post_docs:
        if doc['implicit_value'] >= 6:
            del doc['implicit_value']
            to_be_inserted.append(doc)

    with concurrent.futures.ThreadPoolExecutor(max(len(to_be_inserted), 1)) as executor:
        futures = [executor.submit(update_image_url, doc) for doc in to_be_inserted]
        concurrent.futures.wait(futures)

    if to_be_inserted:
        # logic to push to service
        requests.post('{}/api/posts'.format(service_uri), json={
            "verify_token": verify_token,
            "posts": to_be_inserted
        }, verify=False)

    app.logger.info("Inserted {} posts!".format(len(to_be_inserted)))

def get_edges(username):
    url = "https://www.instagram.com/{}/?__a=1&__d={}".format(
        username, random.randint(12345, 67890))
    r = requests.get(
        url,
        proxies={'http': proxy, 'https': proxy},
        headers={"User-Agent": "Mozilla/5.0 (Linux; Android 9; GM1903 Build/PKQ1.190110.001; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/75.0.3770.143 Mobile Safari/537.36 Instagram 103.1.0.15.119 Android (28/9; 420dpi; 1080x2260; OnePlus; GM1903; OnePlus7; qcom; sv_SE; 164094539)"}
    )
    result = r.json()
    edges = result['graphql']['user']['edge_owner_to_timeline_media']['edges']
    return edges

def get_title(caption: str):
    return textwrap.shorten(caption, width=120, placeholder="")

def get_implicit_value(caption: str):
    hotwords = ['zoom', 'sat', 'http', 'bit', 'ly', 'line', 'wa', 'cp', 'contact', 'seminar', 'talk', 'show', 'daftar', 'regist', 'form', ':', 'certificate', 'workshop', 'point', 'event', 'meeting', 'webinar', 'acara', 'benefit', 'speaker', 'tema', 'topik', 'comserv']
    tokens = caption.lower().split()
    cleaned = ' '.join([token for token in tokens if token[0] != '#'])
    implicit_value = 0
    for hotword in hotwords:
        if hotword in cleaned:
            implicit_value += 1
    
    return implicit_value

def scraper():
    instagramaccounts = get_instagramaccounts()
    instagramaccounts_length = len(instagramaccounts)

    app.logger.info("Found {} instagramaccounts to be scraped!".format(instagramaccounts_length))

    i = 0
    retries = 5

    while i < instagramaccounts_length:
        app.logger.info("Scraping account {}".format(instagramaccounts[i]['username']))
        try:
            edges = get_edges(instagramaccounts[i]['username'])
        except Exception as e:
            if retries == 0:
                app.logger.info("Reached maximum retries! Skipping account!")
                i += 1
                retries = 5
                continue
            app.logger.info("Error getting user medias: {}".format(e))
            app.logger.info("Retrying...")
            retries -= 1
            continue

        post_docs = []
        for edge in edges:
            post = edge['node']
            try:
                caption = post['edge_media_to_caption']['edges'][0]['node']['text']
            except IndexError:
                caption = "No Caption"

            implicit_value = get_implicit_value(caption)
            posted_at = datetime.utcfromtimestamp(post['taken_at_timestamp'])
            post_docs.append({
                '_id': post['shortcode'],
                'title': get_title(caption),
                'description': caption,
                'image_url': post['display_url'],
                'user_id': 'vd6WfRXDKEal94dQ8OMaSe5v00c2',
                'created_at': posted_at.isoformat(),
                'updated_at': posted_at.isoformat(),
                'implicit_value': implicit_value,
            })

        upsert_docs(post_docs)
        update_instagramaccount_timestamp(instagramaccounts[i]['username'])

        i += 1
        retries = 5
    app.logger.info("Scraper finished scraping!")

@app.route("/active")
def active():
    return '<br />'.join([t.name for t in threading.enumerate()])


@app.route("/trigger")
def trigger():
    thread_names = [t.name for t in threading.enumerate()]
    if "IGScraper" in thread_names:
        return "Scraper is already running!"
    thread = Thread(target=scraper, name="IGScraper", daemon=True)
    thread.start()
    return "Started scraper!"


@app.route("/")
def index():
    thread_names = [t.name for t in threading.enumerate()]
    result = "HEALTHY<br />Scraper is "
    if "IGScraper" in thread_names:
        result += "running!"
    else:
        result += "not running!"
    return result


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
