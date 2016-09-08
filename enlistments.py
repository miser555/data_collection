import os
import pandas as pd
from bs4 import BeautifulSoup
import urllib.request
from common import fetch_df_from_xml
from settings import MSG_FOLDER

urls = []
repo_type = []


def xml_beutifulSoup():
    url = "https://www.openhub.net/p/641496/enlistments"
    content = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(content)
    soup.findAll('table')[0].tbody.findAll('tr')
    for row in soup.findAll('table')[0].tbody.findAll('tr'):
        url = row.findAll('td')[0].text.strip()
        repo = row.findAll('td')[1].text.strip()
        urls.append(url)
        repo_type.append(repo)
    # we need the ID as well.
    data = {"URL": urls, "repo_type": repo_type, "ID": pid}
    df = pd.DataFrame(data)
    print("### enlistments DF: ")
    print(df)

merged_contribs = []
for k in range(1, 21):
    projects_df = fetch_df_from_xml("projects", page=k)
    try:
        # for pid in project_df.id:
        for pid in projects_df.id:
            merged_contribs.append(
                fetch_df_from_xml("enlistments", pid=pid)
            )
    except urllib.error.HTTPError as err:
        print(err)
        #xml_beutifulSoup()



merged_contribs = pd.concat(merged_contribs, axis=0)
merged_df_file = os.path.join(MSG_FOLDER, "enlistments.msg")
merged_contribs.to_msgpack(merged_df_file, encoding="utf-8")
print("successfully merged everything into {}".format(merged_df_file))
