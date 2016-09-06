import os
import glob
import math
import pandas as pd
import urllib.request
import xml.etree.ElementTree as ET

from settings import OH_API_ENDPOINT, OH_API_KEY, XML_FOLDER, MSG_FOLDER

DIR_PATH_FOR_FTYPE = {
    "xml": XML_FOLDER,
    "msg": MSG_FOLDER,
}

XPATH_FOR_TARGET = {
    "projects": 'result/project',
    "contributors": 'result/contributor_fact',
}


def get_number_of_contrib_pages(pid):
    first_xml_file = glob.glob(os.path.join(
        XML_FOLDER,
        "contribs_{}_1of*.xml".format(pid))
    )

    if len(first_xml_file):
        print("found file  {}".format(first_xml_file[0]))
        f = first_xml_file[0]
    else:
        url = get_url("contributors", pid=pid)
        print("openning: {}".format(url))
        f = urllib.request.urlopen(url)

    tree = ET.parse(f)
    elem = tree.getroot()

    items_returned = elem.find('items_returned')
    items_available = elem.find('items_available')
    items_returned = int(items_returned.text)
    items_available = int(items_available.text)
    print("project ID: {}".format(pid))
    print("items_returned: ", items_returned)
    print("items_available: ", items_available)
    # trying to prevent ZeroDivisionError
    if items_returned:
        number_of_pages = math.ceil(items_available / items_returned)
    else:
        number_of_pages = 0
    print("number_of_pages: ", number_of_pages)
    return number_of_pages


def get_url(target, page=1, pid=None):
    if target == "projects":
        return "{}projects.xml?api_key={}&sort=rating&page={}".format(
            OH_API_ENDPOINT,
            OH_API_KEY,
            page,
        )

    if target == "contributors":
        if not pid:
            raise ValueError("pid missing for contributors")
        return "{}projects/{}/contributors.xml?api_key={}&page={}".format(
            OH_API_ENDPOINT,
            pid,
            OH_API_KEY,
            page,
        )
    raise ValueError("Unknown target: {}".format(target))


def get_file_path(target, ftype, page=1, pid=None):
    if ftype not in DIR_PATH_FOR_FTYPE.keys():
        raise ValueError("ftype must be one of {}".format(
            DIR_PATH_FOR_FTYPE.keys()))

    if target == "projects":
        return os.path.join(
            DIR_PATH_FOR_FTYPE[ftype],
            "project_{}.{}".format(page, ftype)
        )

    if target == "contributors":
        if not pid:
            raise ValueError("pid missing for contributors")
        return os.path.join(
            DIR_PATH_FOR_FTYPE[ftype],
            "contribs_{}_{}of{}".format(
                pid, page, get_number_of_contrib_pages(pid))
        )

    raise ValueError("Unknown target: {}".format(target))


def fetch_xml(target, page=1, pid=None):
    # get filepath and check if exists
    xml_file_path = get_file_path(target, "xml", page=page, pid=pid)
    store_xml_file = False
    if os.path.exists(xml_file_path):
        f = xml_file_path
    # otherwise fetch xml from url and store file
    else:
        url = get_url(target, pid=pid, page=page)
        print("openning: {}".format(url))
        f = urllib.request.urlopen(url)
        store_xml_file = True
    tree = ET.parse(f)
    elem = tree.getroot()

    # store xml file if fetched from url
    if store_xml_file:
        with open(xml_file_path, "w") as xmlf:
            xmlf.write(ET.tostring(elem).decode("utf-8"))

    return elem


def fetch_df_from_xml(target, page=1, pid=None):
    msg_file_path = get_file_path(target, "msg", page=page, pid=pid)
    if os.path.exists(msg_file_path):
        df = pd.read_msgpack(msg_file_path, encoding="utf-8")
    else:
        elem = fetch_xml(target, page=page, pid=pid)

        # for contribs
        records = [
            r for
            r in elem.findall(XPATH_FOR_TARGET[target])
        ]
        df = pd.DataFrame(
            [
                {x.tag: x.text.strip() for x in r if x.text}
                for r in records
            ]
        )
        # store the intermediate dataframe
        df.to_msgpack(msg_file_path, encoding="utf-8")
    return df
