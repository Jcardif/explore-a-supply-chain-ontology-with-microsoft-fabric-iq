# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Install Fabric IQ Ontology Accelerator Package
%pip install semantic-link-labs --q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy_labs as labs
import sempy.fabric as fabric
from sempy_labs._helper_functions import (
    _base_api,
)
import os
import pandas as pd
import requests
import json

lakehouse_name = "RetailLH"
eventhouse_name = "RetailEH"
mount_point = "/mnt/RetailLH"
workspace_id = fabric.get_workspace_id()

try:
    payload = {"displayName":lakehouse_name,"description":lakehouse_name,"creationPayload": {"enableSchemas": True}}

    _base_api(
            request=f"/v1/workspaces/{workspace_id}/lakehouses",
            method="post",
            payload=payload,
            status_codes=[201, 202],
            lro_return_status_code=True,
    )
except:
    print("Lakehouse Exists")

lakehouses = labs.lakehouse.list_lakehouses()
lakehouse_id = lakehouses[lakehouses["Lakehouse Name"] == lakehouse_name].reset_index()["Lakehouse ID"][0]

try:
    labs.create_eventhouse(eventhouse_name,{"displayName": eventhouse_name,"description": eventhouse_name})
except:
    print("Eventhouse Exists")

eventhouses = labs.list_eventhouses()
eventhouse_id = eventhouses[eventhouses["Eventhouse Name"] == eventhouse_name].reset_index()["Eventhouse Id"][0]

try:
    payload = {"displayName":eventhouse_name,"description":eventhouse_name,"creationPayload": {"databaseType":"ReadWrite","parentEventhouseItemId":eventhouse_id}}

    _base_api(
            request=f"/v1/workspaces/{workspace_id}/kqlDatabases",
            method="post",
            payload=payload,
            status_codes=[201, 202],
            lro_return_status_code=True,
            client="fabric_sp",
        )
except:
    print("KQLDatabase Exists")

mounts = notebookutils.fs.mounts()

mounts = list(filter(lambda e: e.mountPoint == mount_point,mounts))

if len(mounts) > 0:
    notebookutils.fs.unmount(
    mount_point,
    )

notebookutils.fs.mount(
    f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}",
    mount_point,
)
path = notebookutils.fs.getMountPath(mount_point)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

git_repo = "https://raw.githubusercontent.com/ecotte/fabric-rti-iq-workshops/refs/heads/main/ontology-lab/"
iq_file = "retail_ontology_package.iq"
whl_file = "fabriciq_ontology_accelerator-0.1.0-py3-none-any.whl"
notebook_file = "Create_Ontology_Lab.ipynb"


response = requests.get(git_repo + iq_file)
with open(f"{path}/Files/{iq_file}", "wb") as file:
    file.write(response.content)

response = requests.get(git_repo + whl_file)
with open(f"{path}/Files/{whl_file}", "wb") as file:
    file.write(response.content)

response = requests.get(git_repo + notebook_file)

notebook = json.loads(response.content)

notebook["metadata"]["dependencies"]["lakehouse"]["known_lakehouses"] = [{"id": lakehouse_id}]
notebook["metadata"]["dependencies"]["lakehouse"]["default_lakehouse"] = lakehouse_id
notebook["metadata"]["dependencies"]["lakehouse"]["default_lakehouse_name"] = lakehouse_name
notebook["metadata"]["dependencies"]["lakehouse"]["default_lakehouse_workspace_id"] = workspace_id

try:
    labs.create_notebook(name = "Create_Ontology_Lab",notebook_content=json.dumps(notebook),format="ipynb",folder="Setup")
except:
    print("")

notebooklist = labs.list_notebooks()
notebook_id = notebooklist[notebooklist["Notebook Name"] == "Create_Ontology_Lab"].reset_index()["Notebook Id"][0]
notebook_id

# ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

from IPython.display import display, HTML

url = f"https://app.fabric.microsoft.com/groups/{workspace_id}/synapsenotebooks/{notebook_id}?experience=fabric-developer"

html_button = f'''

    Open Notebook To Finish the Setup

'''
display(HTML(html_button))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
