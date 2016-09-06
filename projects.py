import pandas as pd
import os

from common import fetch_df_from_xml
from settings import MSG_FOLDER

merged_projects = []
for k in range(1, 21):
    merged_projects.append(
        fetch_df_from_xml("projects", page=k)
    )

merged_projects = pd.concat(merged_projects, axis=0)
merged_df_file = os.path.join(MSG_FOLDER, "projects.msg")
merged_projects.to_msgpack(merged_df_file, encoding="utf-8")
print("successfully merged everything into {}".format(merged_df_file))
