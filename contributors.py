import os
import glob  # will use for searching for files
import urllib.request
import xml.etree.ElementTree as ET
import pandas as pd
import math
# import fnmatch

MSG_FOLDER = os.path.join(".", "msg")
XML_FOLDER = os.path.join(".", "xml")

for k in range(1, 21):
    project_df = pd.read_msgpack(
        os.path.join(MSG_FOLDER, "project_{}.msg".format(k)))

    # for pid in project_df.id:
    for pid in project_df.id:
        first_xml_file = glob.glob(os.path.join(
            "xml", "contribs_{}_1of*.xml".format(pid)))
        if len(first_xml_file):
            print("found file  {}".format(first_xml_file[0]))
            f = first_xml_file[0]
        else:
            url = "https://www.openhub.net/projects/{}/contributors.xml?"\
                "api_key=45f9e6ab501adaade3fc276d98cfd2f528af71bdb90a1524af2b"\
                "1c3e9d57e18e&page=0".format(pid)

            print("openning url: {}".format(url))
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
        if items_returned != 0:
            number_of_pages = math.ceil(items_available / items_returned)
            print("number_of_pages: ", number_of_pages)

            # for p in range(number_of_pages):
            for p in range(1, number_of_pages + 1):
                print("### fetching contribs page {}".format(p))

                # construct unique filename
                cfile_name = "contribs_{}_{}of{}".format(
                    pid, p, number_of_pages)

                # check if msg file exists, then skip
                msgfile_path = os.path.join(
                    MSG_FOLDER, cfile_name + ".msg")

                if os.path.exists(msgfile_path):
                    print("{} exists, skipping".format(msgfile_path))
                    continue

                # check if xml exists (then use it instead of url)
                # not skip!
                cxml_file = os.path.join("xml", cfile_name + ".xml")
                fetched_data_from_url = False
                if os.path.exists(cxml_file):
                    print("{} exists, using it".format(cxml_file))
                    f = cxml_file
                else:
                    url = "https://www.openhub.net/projects/{}/"\
                        "contributors.xml?api_key=45f9e6ab501adaade3f"\
                        "c276d98cfd2f528af71bdb90a1524af2b1c3e9d57e18e"\
                        "&page={}".format(pid, p)
                    f = urllib.request.urlopen(url)
                    fetched_data_from_url = True
                    tree = ET.parse(f)
                    elem = tree.getroot()

                # if data is from url, store xml file
                if fetched_data_from_url:
                    xml_file_path = os.path.join(
                        ".", "xml", cfile_name + ".xml")
                    # dont use the word file
                    # it is a reserved keyword in python
                    with open(xml_file_path, "w") as xmlf:
                        xmlf.write(ET.tostring(elem).decode("utf-8"))

                # this should be up one level
                # it should work for url and xml files
                contribs_info = [
                    project for
                    project in elem.findall('result/contributor_fact')
                ]
                contribs_df = pd.DataFrame(
                    [
                        {x.tag: x.text.strip() for x in proj if x.text}
                        for proj in contribs_info
                    ]
                )
                print("record 0:", contribs_df.iloc[0])
                print("NOW THE DF ======= ")
                msg_file_path = os.path.join(
                    MSG_FOLDER, cfile_name + ".msg")
                contribs_df.to_msgpack(msg_file_path, encoding="utf-8")
                # contribs_345345_0of41.msg .xml (example)
                # print the DataFrame
                print(contribs_df)
        else:
            print("There's no information regarding this project")
            continue


# create a merged contributors file
success = True
# new way to append (concat using lists of dfs)
merged_contribs = []
for k in range(1, 21):
    project_df = pd.read_msgpack(
        os.path.join(MSG_FOLDER, "project_{}.msg".format(k)))

    print("### merging contribs data from project dataframe {}".format(k))
    for pid in project_df.id:
        # number_ofpages is not set correctly
        # need to read xml for first page to set it for each project

        first_xml_file = glob.glob(os.path.join(
            "xml", "contribs_{}_1of*.xml".format(pid)))[0]
        print("found {}".format(first_xml_file))
        tree = ET.parse(first_xml_file)

        elem = tree.getroot()

        items_returned = elem.find('items_returned')
        items_available = elem.find('items_available')
        items_returned = int(items_returned.text)
        items_available = int(items_available.text)

        print("project ID: {}".format(pid))
        print("items_returned: ", items_returned)
        print("items_available: ", items_available)
        if items_returned:
            number_of_pages = math.ceil(items_available / items_returned)
            print("number_of_pages: ", number_of_pages)
        else:
            number_of_pages = 0

        for p in range(1, number_of_pages + 1):
            msgfile_name = "contribs_{}_{}of{}.msg".format(
                pid, p, number_of_pages)
            msgfile_path = os.path.join(MSG_FOLDER, msgfile_name)
            print(msgfile_name)

            # this part needs to be inside the loop
            # check if msg file exist so we can merge
            if os.path.exists(msgfile_path):
                print("{} exists, merging".format(msgfile_name))
                merged_contribs.append(
                    pd.read_msgpack(msgfile_path, encoding="utf-8"))
            else:
                print(
                    "{} is missing, check/rerun script".format(msgfile_path))
                success = False
                break
merged_contribs = pd.concat(merged_contribs, axis=0)


# storing merged projects into project.msg
if success:
    merged_df_file = os.path.join(MSG_FOLDER, "contributors.msg")
    print("successfully merged everything into {}".format(merged_df_file))
    # merged_projects_df.to_msgpack(merged_df_file, encoding="utf-8")
    merged_contribs.to_msgpack(merged_df_file, encoding="utf-8")
