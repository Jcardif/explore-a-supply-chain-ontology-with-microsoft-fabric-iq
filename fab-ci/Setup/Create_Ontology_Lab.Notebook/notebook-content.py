# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "383865c6-debf-4f4f-bb01-62ae5ad5d699",
# META       "default_lakehouse_name": "RetailLH",
# META       "default_lakehouse_workspace_id": "49503630-ec37-4ca2-9d21-e82113bc9324",
# META       "known_lakehouses": [
# META         {
# META           "id": "383865c6-debf-4f4f-bb01-62ae5ad5d699"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Fabric IQ Accelerator Sample
#         
# ### Create Ontology from Package

# CELL ********************

# Install Fabric IQ Ontology Accelerator Package
%pip install /lakehouse/default/Files/fabriciq_ontology_accelerator-0.1.0-py3-none-any.whl --q
%pip install semantic-link-labs --q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy.fabric as fabric
import json
import sempy_labs as labs
from fabricontology import create_ontology_item, generate_definition_from_package
from fabricontology.generate_data import generate_instance_data, generate_events_data
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lakehouse_name = "RetailLH"
eventhouse_name = "RetailEH"
workspace_id = fabric.get_workspace_id()
lakehouses = labs.lakehouse.list_lakehouses()
lakehouse_id = lakehouses[lakehouses["Lakehouse Name"] == lakehouse_name].reset_index()["Lakehouse ID"][0]
eventhouses = labs.list_eventhouses()
eventhouse_id = eventhouses[eventhouses["Eventhouse Name"] == eventhouse_name].reset_index()["Eventhouse Id"][0]
eventhouse_uri = eventhouses[eventhouses["Eventhouse Name"] == eventhouse_name].reset_index()["Query Service URI"][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = fabric.get_workspace_id()
access_token = notebookutils.credentials.getToken('pbi')

ontology_item_name = "RetailOntology"
ontology_package_path = "/lakehouse/default/Files/retail_ontology_package.iq"

binding_lakehouse_name = lakehouse_name
binding_lakehouse_schema_name = "dbo"  # replace this if using lakehouse without schemas
binding_eventhouse_name = eventhouse_name
binding_eventhouse_cluster_uri = eventhouse_uri
binding_eventhouse_database_name = eventhouse_name

items_df = fabric.list_items()
binding_lakehouse_item_id = str(items_df[(items_df["Type"] == "Lakehouse") & (items_df["Display Name"] == binding_lakehouse_name)].iloc[0].Id)
binding_eventhouse_item_id = str(items_df[(items_df["Type"] == "Eventhouse") & (items_df["Display Name"] == binding_eventhouse_name)].iloc[0].Id)
binding_workspace_id = workspace_id

ontology_definition, entity_types, relationship_types, data_bindings, contextualizations = generate_definition_from_package(
    ontology_package_path=ontology_package_path,
    ontology_name=ontology_item_name, 
    binding_workspace_id=binding_workspace_id,
    binding_lakehouse_item_id=binding_lakehouse_item_id,
    binding_lakehouse_schema_name=binding_lakehouse_schema_name,
    binding_eventhouse_item_id=binding_eventhouse_item_id,
    binding_eventhouse_cluster_uri=binding_eventhouse_cluster_uri,    
    binding_eventhouse_database_name=binding_eventhouse_database_name)

response = create_ontology_item(workspace_id=workspace_id, 
                           access_token=access_token,
                           ontology_item_name=ontology_item_name, 
                           ontology_definition=ontology_definition)
print(response.json())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ontology_package_path = "/lakehouse/default/Files/retail_ontology_package.iq"

# Create delta tables in the default lakehouse
lakehouse_schema = "dbo"  # replace this if using lakehouse without schemas.
response = generate_instance_data(spark, ontology_package_path=ontology_package_path, database=lakehouse_schema, mode="overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create eventhouse tables 
eventhouse_cluster_uri = eventhouse_uri
eventhouse_database = eventhouse_name
access_token=mssparkutils.credentials.getToken(eventhouse_cluster_uri)

response = generate_events_data(spark, 
        ontology_package_path=ontology_package_path,
        eventhouse_cluster_uri=eventhouse_cluster_uri,
        eventhouse_database=eventhouse_database,
        access_token=access_token )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
