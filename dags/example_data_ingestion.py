from xmljson import badgerfish as bf
from json import dumps
import pyarrow.orc
from pyarrow.lib import Table
import os
import uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, BlobPrefix
from pathlib import PurePosixPath
import tempfile
import xml.etree.ElementTree as ET
import pandas as pd
import trino
import sys
from trino.dbapi import connect
import uuid

# various constants. These should be changed in a Jupyter notebook, or loaded from env variables
storage_account_name = "scienzstorageacc"
container_name = "content"
ccd_prefix_in_container = "anonymized/"
schema = "ccds"
dim_ccd_raw = "dim_ccd_raw"
hostname = "trino"
ANONYMIZE = False

# we will use these to store the serialized XML-as-JSON data
file_names = []
serialized = []

def anonymize_node(node):
    new_node = {}
    if isinstance(node, str) or isinstance(node, int) or isinstance(node, float):
        return str(uuid.uuid4())
    if isinstance(node, list):
        return [anonymize_node(x) for x in node]
    for k, v in node.items():
        new_val = anonymize_node(v)
        new_node[k] = new_val
    return new_node

# we create trino assets *before* we upload anything
# Note: we cannot drop Hive tables unless we use the same blob storage client instantiated here to 
# delete blobs first. This could be fixed if it becomes a real issue. 
conn = connect(
    host=hostname,
    port=8080,
    user="trino",
    catalog="hive",
    schema=schema,
)
cur = conn.cursor()
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema} WITH (location='abfs://{container_name}@{storage_account_name}.dfs.core.windows.net/{schema}/')")
rows = cur.fetchall()
cur = conn.cursor()
cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{dim_ccd_raw} (ccd_json VARCHAR)")
rows = cur.fetchall()


# getting ready to interact w/ Azure Blob Storage
account_url = f"https://{storage_account_name}.blob.core.windows.net"
default_credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(account_url, credential=default_credential)
container_client = blob_service_client.get_container_client(container_name)
blob_lists = [container_client.walk_blobs(name_starts_with=ccd_prefix_in_container, delimiter="/")]

# download blobs one by one and convert them to JSON
# note: this works as an example, if we have 100k XMLs we might want to revisit exactly how this works
with tempfile.TemporaryDirectory() as td:
    while len(blob_lists) > 0:
        blob_list = blob_lists.pop()
        for blob in blob_list:
            path_in_container = blob.name
            file_name = PurePosixPath(path_in_container).name
            extension = PurePosixPath(path_in_container).suffix
            xml_path = os.path.join(td, file_name)
            if extension == ".xml":
                with open(file=xml_path, mode="wb") as download_file:
                    print(blob.name)
                    download_file.write(container_client.download_blob(blob.name).readall())
                tree = ET.parse(xml_path).getroot()
                file_names.append(xml_path)
                if ANONYMIZE:
                    serialized.append(dumps(anonymize_node(bf.data(tree))))
                else:
                    serialized.append(dumps(bf.data(tree)))
            elif isinstance(blob, BlobPrefix):
                blob_lists.append(container_client.walk_blobs(name_starts_with=blob.name, delimiter="/"))
    # this Pandas data frame holds all the data. As of right now all the XMLs must fit into memory, but this
    # requirement is easily relaxed via batching
    dt = pd.DataFrame({"ccd_json": serialized})
    out_file_name = "output.orc"
    out_path = os.path.join(td, out_file_name)
    # ORC file is written here
    with open(out_path, "wb") as f:
        pyarrow.orc.write_table(Table.from_pandas(dt), f)
    # Upload data
    orc_blob_path = os.path.join(schema, dim_ccd_raw, out_file_name)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=orc_blob_path)
    with open(file=out_path, mode="rb") as data:
        blob_client.upload_blob(data)

# Prove that interaction with Trino works:
cur = conn.cursor()
cur.execute("""
SELECT
    JSON_EXTRACT(section, '$["{urn:hl7-org:v3}code"]["@code"]') AS maybe_diagnostic_code,
    JSON_EXTRACT(section, '$["{urn:hl7-org:v3}code"]["@displayName"]') AS maybe_diagnostic_description,
    JSON_EXTRACT(patient, '$["{urn:hl7-org:v3}administrativeGenderCode"]["@code"]') AS maybe_sex, 
    JSON_EXTRACT(patient, '$["{urn:hl7-org:v3}birthTime"]["@value"]') AS maybe_birth_time,
    COALESCE(
        JSON_EXTRACT_SCALAR(name['{urn:hl7-org:v3}given'], '$["$"]'),
        JSON_EXTRACT_SCALAR(name['{urn:hl7-org:v3}given'], '$[0]["$"]')
    ) AS first_name,
    COALESCE(
        JSON_EXTRACT_SCALAR(name['{urn:hl7-org:v3}family'], '$["$"]'),
        JSON_EXTRACT_SCALAR(name['{urn:hl7-org:v3}family'], '$[0]["$"]')
    ) AS last_name,
    code
FROM (
  SELECT 
      patient_role, patient, code, clinical_document, record_target,
      COALESCE(TRY(CAST(name AS MAP<VARCHAR, JSON>)), CAST(name AS ARRAY<MAP<VARCHAR, JSON>>)[1]) AS name,
      documentation_of, service_event, service_document, component, section
  FROM (
    SELECT 
      patient_role, patient, code, clinical_document, record_target,
      JSON_EXTRACT(patient, '$["{urn:hl7-org:v3}name"]') AS name,
      documentation_of,
      service_event,
      service_document,
      component,
      section
    FROM (
      SELECT patient_role, 
          record_target,
          JSON_EXTRACT(patient_role, '$["{urn:hl7-org:v3}patient"]') AS patient, code, clinical_document,
          documentation_of,
          service_event,
          service_document,
          component,
          section
      FROM (
        SELECT
          clinical_document,
          record_target,
          documentation_of,
          service_document,
          JSON_EXTRACT(record_target, '$["{urn:hl7-org:v3}patientRole"]') AS patient_role,
          JSON_EXTRACT(service_document, '$["{urn:hl7-org:v3}serviceEvent"]') AS service_event,
          JSON_EXTRACT_SCALAR(clinical_document, '$["{urn:hl7-org:v3}code"]["@code"]') AS code,
          JSON_EXTRACT(component, '$["{urn:hl7-org:v3}section"]') AS section,
          component
        FROM (
          SELECT 
              clinical_document,
              JSON_EXTRACT(clinical_document, '$["{urn:hl7-org:v3}recordTarget"]') AS record_target,
              JSON_EXTRACT(clinical_document, '$["{urn:hl7-org:v3}documentationOf"]') AS documentation_of,
              JSON_EXTRACT(clinical_document, '$["{urn:hl7-org:v3}component"]["{urn:hl7-org:v3}structuredBody"]["{urn:hl7-org:v3}component"]') 
                  AS components
          FROM (
            SELECT JSON_EXTRACT(ccd_json, '$["{urn:hl7-org:v3}ClinicalDocument"]') AS clinical_document
            FROM hive.ccds.dim_ccd_raw
          )
        ) CROSS JOIN UNNEST (
           COALESCE(TRY(CAST(documentation_of AS ARRAY<JSON>)), ARRAY[documentation_of])
        ) AS t(service_document)
        CROSS JOIN UNNEST (
           CAST(components AS ARRAY<JSON>)
        ) AS t1(component)
      )
    )
  )
)
""")
rows = cur.fetchall()
print(rows)
