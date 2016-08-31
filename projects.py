import urllib.request
import xml.etree.ElementTree as ET
import pandas as pd
import os


MSG_FOLDER = os.path.join(".","msg")
XML_FOLDER = os.path.join(".","xml")

if not os.path.exists(MSG_FOLDER):
    os.makedirs(MSG_FOLDER)

if not os.path.exists(XML_FOLDER):
    os.makedirs(XML_FOLDER)

for p in range(1,21):
    print("### fetching project page {}".format(p))

    # construct unique filename
    pfile_name = "project_{}".format(p)

    # check if xml file exists, then skip
    if os.path.exists(pfile_name + ".xml"):
        print("{}.xml exists, skipping".format(pfile_name))
        continue
    #check if xml exists
    pxml_file = os.path.join("xml", pfile_name + ".xml")
    fetched_data_from_url = False
    if os.path.exists(pxml_file):
       f = pxml_file
    else:
        url = "https://www.openhub.net/projects.xml?api_key=58c554bf40d74f151057d1b276b22f0c5ccadc77b79c1fd762cdbe5fadfeb26a&sort=rating&page={}".format(p)
        f = urllib.request.urlopen(url)
        fetched_data_from_url = True
        tree = ET.parse(f)
        elem = tree.getroot()
    if fetched_data_from_url:
       xml_file_path = os.path.join(".", "xml", "project_{}.xml".format(p))
       with open(xml_file_path,"w") as file:
            file.write(ET.tostring(elem).decode("utf-8"))
       projects = [project for project in elem.findall('result/project') ]
       records = [{x.tag: x.text.strip() for x in proj if x.text} for proj in projects]
       projects_df = pd.DataFrame(records)
       msg_file_path = os.path.join(".", "msg", pfile_name + ".msg")
       projects_df.to_msgpack(msg_file_path, encoding="utf-8")


# now that we are done let's create a merged project file
#merged_projects_df = pd.DataFrame()
success = True
#new way to append (concat using lists of dfs)
merged_projects = []
for p in range(1,21):
    print("### merging project data from dataframe {}".format(p))
    # construct unique filename
    msgfile_name = "project_{}.msg".format(p)
    msgfile_path = os.path.join(MSG_FOLDER, msgfile_name)
    # check if msg file exist so we can merge
    if os.path.exists(msgfile_path):
        print("{}.msg exists, merging".format(msgfile_name))
        #merged_projects_df.append(pd.read_msgpack(msgfile_path, encoding="utf-8"))
        merged_projects.append(pd.read_msgpack(msgfile_path, encoding="utf-8"))
    else:
        print("{}.msg is missing, check the script and rerun to fetch it".format(msgfile_path))
        success = False
        break
merged_projects = pd.concat(merged_projects, axis=0)


# storing merged projects into project.msg
if success:
    merged_df_file = os.path.join(MSG_FOLDER, "projects.msg")
    print("successfully merged everything into {}".format(merged_df_file))
    #merged_projects_df.to_msgpack(merged_df_file, encoding="utf-8")
    merged_projects.to_msgpack(merged_df_file, encoding="utf-8")
