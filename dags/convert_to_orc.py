from xmljson import badgerfish as bf
import xml.etree.ElementTree as ET
from json import dumps
import pandas as pd
import pyarrow.orc
from pyarrow.lib import Table

NUM_XMLS = 500
PATH_GLOB = 'output/ccd_%06d.xml'

file_names = []
serialized = []

for i in range(NUM_XMLS):
    xml_path = PATH_GLOB % i
    tree = ET.parse(xml_path).getroot()
    file_names.append(xml_path)
    serialized.append(dumps(bf.data(tree)))

dt = pd.DataFrame({"xml_path": file_names, "ccd_json": serialized})
with open("output_orc/records.orc", "wb") as f: 
    pyarrow.orc.write_table(Table.from_pandas(dt), f)

