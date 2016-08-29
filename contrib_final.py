import os
import urllib.request
import xml.etree.ElementTree as ET
import pandas as pd
import math


for k in range(1, 200):
    project_df = pd.read_msgpack("project_{}.msg".format(k))





    for pid in project_df.id:
        url = "https://www.openhub.net/projects/{}/contributors.xml?api_key=974844c96bce6c2843450458d78d8abf1079c2df7a0dafb27c56876afedd69d2&page=0".format(pid)
        f = urllib.request.urlopen(url)
        tree = ET.parse(f)
        elem = tree.getroot()

        items_returned = elem.find('items_returned')
        items_available = elem.find('items_available')
        items_returned = int(items_returned.text)
        items_available = int(items_available.text)
        number_of_pages = math.ceil(items_available/items_returned)
        print("items_returned ", items_returned)
        print("items_available ", items_available)
        print("number_of_pages ", number_of_pages)

        # for p in range(number_of_pages):
        for p in range(1,number_of_pages+1):
            print("### fetching contribs page {}".format(p))

            # construct unique filename
            cfile_name = "contribs_{}_{}of{}".format(pid, p, number_of_pages)

            # check if msg file exists, then skip
            check_msg = os.path.join(".", "contribs_msg", cfile_name + ".msg")
            if os.path.exists(check_msg):
                print("{}.msg exists, skipping".format(cfile_name))
                continue
            #check if xml exists
            cxml_file = os.path.join("xml", cfile_name + ".xml")
            check_xml = os.path.join(".", "contribs_xml", cxml_file + ".xml")
            fetched_data_from_url = False
            if os.path.exists(check_xml):
                print("{}.xml exists, skipping".format(cxml_file))
                continue
                f = cxml_file
            else:
                url = "https://www.openhub.net/projects/{}/contributors.xml?api_key=974844c96bce6c2843450458d78d8abf1079c2df7a0dafb27c56876afedd69d2&page={}".format(pid,p)
                f = urllib.request.urlopen(url)
                fetched_data_from_url = True
                tree = ET.parse(f)
                elem = tree.getroot()

            # if data is from url, store xml file
            if fetched_data_from_url:
                xml_file_path = os.path.join(".", "contribs_xml", cfile_name + ".xml")
                with open(xml_file_path,"w") as file:
                     file.write(ET.tostring(elem).decode("utf-8"))

                contribs_info = [project for project in elem.findall('result/contributor_fact')]
                contribs_df = pd.DataFrame([{x.tag: x.text.strip() for x in proj if x.text} for proj in contribs_info])
                print("record 0:", contribs_df.iloc[0])
                print("NOW THE DF ======= ")
                # no need for this
                #contrib=contrib.append(records)
                #contrib.to_msgpack('contributors_for_project_with_id_{}.msg'.format(pid), encoding="utf-8")
                msg_file_path = os.path.join(".", "contribs_msg", cfile_name + ".msg")
                contribs_df.to_msgpack(msg_file_path, encoding="utf-8")
                #contribs_345345_0of41.msg .xml (example)
                print(contribs_df)
