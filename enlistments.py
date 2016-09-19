import os
import pandas as pd
from bs4 import BeautifulSoup
import urllib.request
from common import fetch_df_from_xml
from settings import MSG_FOLDER

urls = []
repo_type = []


def fetch_enlistment_from_html(pid):
    # use parameters in function names
    url = "https://www.openhub.net/p/{}/enlistments".format(pid)
    content = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(content, "lxml") # set the parser as lxml
    soup.findAll('table')[0].tbody.findAll('tr')
    for row in soup.findAll('table')[0].tbody.findAll('tr'):
        url = row.findAll('td')[0].text.strip()
        repo = row.findAll('td')[1].text.strip()
        urls.append(url)
        repo_type.append(repo)
    # we need the ID as well.
    # if should have been passed from the function parameters
    data = {"URL": urls, "repo_type": repo_type, "ID": pid}
    df = pd.DataFrame(data)
    print("### enlistments DF: ")
    # do not forget to return the df!
    return df

# use proper variable names, like merged_enlistments .. or _data
merged_data = []


# this if statement is to ensure
# that the script is only executed from command line
# this would allow us e to import the function for testing
# without executing the script
if __name__ == '__main__':

    for k in range(1, 21):
        projects_df = fetch_df_from_xml("projects", page=k)
        # for pid in project_df.id:
        for pid in projects_df.id:
            # the exception would occur only in
            # fetch_df_from_xml,
            # you still want to continue the loop
            # if there is an error
            # you also want to use pid to set the correctly
            # url, see how I changed the indentation level for
            # the try statement
            try:
                edf = fetch_df_from_xml("enlistments", pid=pid)
                if edf:
                    merged_data.append(
                        edf
                    )
            except urllib.error.HTTPError as err:
                # your function did not return a df
                # you have to return a df and then append it
                # to merged_data
                merged_data.append(
                    fetch_enlistment_from_html(pid)
                )


    merged_data = pd.concat(merged_data, axis=0)
    merged_df_file = os.path.join(MSG_FOLDER, "enlistments.msg")
    merged_data.to_msgpack(merged_df_file, encoding="utf-8")
    print("successfully merged everything into {}".format(merged_df_file))
