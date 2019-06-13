""" sozluk99 iceriklerini parse eder """
from datetime import datetime
from os import listdir
from os.path import join
from urllib.parse import parse_qs, urlparse

import gevent
from gevent import monkey

monkey.patch_all()

from bs4 import BeautifulSoup
from pymongo import MongoClient, ReplaceOne

CLIENT = MongoClient("localhost", 27017)
MONGO_TOPICS = CLIENT.eksi.topics

TOPICS_PATH = "data/"
GEVENT_JOBS = []


def get_topic_files():
    """ basliklari getirir """
    topics = (topic for topic in listdir(TOPICS_PATH) if topic.startswith("show.asp"))

    return topics


def get_content_from_file(path):
    """ verilen pathin icerigini getirir """
    file_path = join(TOPICS_PATH, path)
    with open(file_path, mode="r", encoding="iso-8859-9") as file:
        return file.read()


def get_entry_tags(html_doc):
    """ verilen html icerisindeki entryleri getirir """
    soup = BeautifulSoup(html_doc, "html.parser")
    return soup.select("ol#el li")


def parse_entry_tag(entry_tag):
    """ verilen html entry'i objeye cevirir """
    id = entry_tag["id"].strip("d")

    nick_tag = entry_tag.select_one("div.aul a")
    nick = nick_tag.get_text()
    nick_tag.decompose()

    date_tag = entry_tag.select_one("div.aul")
    date_str = date_tag.get_text()
    date_str = date_str.split("~")[0]
    date_str = date_str.strip(" (),")
    try:
        date = datetime.strptime(date_str, "%d.%m.%Y")
    except:
        date = datetime.strptime(date_str, "%d.%m.%Y %H:%M")

    date_tag.decompose()

    entry_tag.select_one("div").decompose()

    text = entry_tag.get_text()

    return {"id": id, "nick": nick, "date": date, "text": text}


def get_title_from_file(file_name):
    """ dosya isminden topic adini al """
    file_name = file_name.replace("@", "?").replace(".htm", "")
    qs_list = parse_qs(urlparse(file_name).query)

    if "t" not in qs_list:
        return ""

    file_name = qs_list["t"][0]

    return file_name


def insert_documents(counter, requests, last_insert=False):
    """ bulk insert  """
    global GEVENT_JOBS

    if counter > 1000 or last_insert:
        GEVENT_JOBS.append(
            gevent.spawn(MONGO_TOPICS.bulk_write, requests.copy(), False)
        )
        requests.clear()
        return 0

    return counter + 1


def parse():
    """ donusum islemini gerceklestirir """
    global GEVENT_JOBS

    topic_files = get_topic_files()

    requests = []

    counter = 0

    for topic_file in topic_files:
        topic_html = get_content_from_file(topic_file)
        entry_tags = get_entry_tags(topic_html)

        title = get_title_from_file(topic_file)

        if not entry_tags or not title:
            continue

        for entry_tag in entry_tags:
            entry = parse_entry_tag(entry_tag)
            entry["title"] = title

            requests.append(ReplaceOne({"id": entry["id"]}, entry, upsert=True))
            counter = insert_documents(counter, requests)

    insert_documents(counter, requests, True)
    gevent.joinall(GEVENT_JOBS)


if __name__ == "__main__":
    print(datetime.now())
    parse()
    print(datetime.now())
    print("finished!")
