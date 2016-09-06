import os
import pandas as pd

from common import fetch_df_from_xml, get_number_of_contrib_pages
from settings import MSG_FOLDER

merged_contribs = []
for k in range(1, 21):
    projects_df = fetch_df_from_xml("projects", page=k)

    # for pid in project_df.id:
    for pid in projects_df.id:
        no_pages = get_number_of_contrib_pages(pid)
        for page in range(1, no_pages+1):
            merged_contribs.append(
                fetch_df_from_xml("contributors", pid=pid, page=page)
            )

merged_contribs = pd.concat(merged_contribs, axis=0)
merged_df_file = os.path.join(MSG_FOLDER, "contributors.msg")
merged_contribs.to_msgpack(merged_df_file, encoding="utf-8")
print("successfully merged everything into {}".format(merged_df_file))
