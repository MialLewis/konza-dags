import xml.etree.ElementTree as ET
import pandas as pd
import csv
import random
import sys
from xmljson import badgerfish as bf
from json import dumps
import pyarrow.orc
from pyarrow.lib import Table
import os
from common import get_gender_code, get_dob

xml_path = sys.argv[1] # "ccd_xml_template1.xml"
names_path = sys.argv[2] # 'us-500.csv'
output_glob = sys.argv[3] # "output/ccd_%06d.xml"
nrows = int(sys.argv[4])

def ccd_path(path):
    return "/".join([
        ("{urn:hl7-org:v3}" if len(x) > 0 else "") + x
        for x in path.split("/")
    ])

def change_address(address_node, street, city, postal_code, country):

    for (path, new_value) in [
        ("streetAddressLine", street), 
        ("city", city), 
        ("postalCode", postal_code), 
        ("country", country),
    ]:
        address_node.find(ccd_path(path)).text = new_value


def change_name(name_node, given, family):

    for (path, new_value) in [
        ("given", given), 
        ("family", family), 
    ]:
        name_node.find(ccd_path(path)).text = new_value

def change_patient(node, given, family, street, city, postal_code, country, dob, gender):
    
    node.find(ccd_path("patient/birthTime")).text = dob
    gender_node = node.find(ccd_path("patient/administrativeGenderCode"))
    gender_node.attrib['code'] = gender
    gender_node.attrib['displayName'] = 'Female' if gender == 'F' else 'Male'

    address = patient_role.find(ccd_path("addr"))
    name = patient_role.find(ccd_path("patient/name"))

    change_address(address, street, city, postal_code, country)
    change_name(name, given, family)

tree = ET.parse(xml_path)
patient_role = tree.find(ccd_path("recordTarget/patientRole"))

csv.register_dialect('adhoc', lineterminator="\r")
patients = pd.read_csv(names_path, dialect='adhoc', nrows=nrows)
used_rows = list()

def write_to_orc(serialized, output_file):

    dt = pd.DataFrame({"ccd_json": serialized})
    with open(output_file, "wb") as f: 
        pyarrow.orc.write_table(Table.from_pandas(dt), f)


BATCH_SIZE = 180
serialized = []
for (i, row) in patients.iterrows():
   
    reuse_existing = random.random() <= 0.01
    if reuse_existing:
        row = random.choice(used_rows)
    else:
        used_rows.append(row)

    change_patient(
        patient_role, 
        given=row["first_name"], 
        family=row["last_name"],
        street=row["address"], 
        city="%s, %s" % (row["city"], row["state"]), 
        postal_code="%05d" % row["zip"], 
        country="U.S.",
        dob=get_dob(row["first_name"], row["last_name"]), 
        gender=get_gender_code(row["first_name"]),
    )
    serialized.append(dumps(bf.data(tree.getroot())))
    if i > 0 and i % BATCH_SIZE == 0:
        write_to_orc(serialized, output_glob % (i // BATCH_SIZE))
        serialized = []

if len(serialized) > 0:
    write_to_orc(serialized, output_glob % ((i // BATCH_SIZE) + 1))
