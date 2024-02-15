import csv
import pandas as pd
import sys
import pyarrow.orc
from pyarrow.lib import Table
from common import get_gender_code, get_dob

names_path = sys.argv[1]
num_needed = int(sys.argv[2])
output_file = sys.argv[3]

csv.register_dialect('adhoc', lineterminator="\r")
patients = pd.read_csv(names_path, dialect='adhoc')

NUM_NEEDED = 100

given, family, dob, gender = ([], [], [], [])

for (i, row) in patients.sample(n=num_needed).iterrows():
    given.append(row["first_name"])
    family.append(row["last_name"])
    dob.append(get_dob(row["first_name"], row["last_name"]))
    gender.append(get_gender_code(row["first_name"]))

dt = pd.DataFrame({
    "given": given,
    "family": family,
    "dob": dob,
    "gender": gender,
})

with open(output_file, "wb") as f: 
    pyarrow.orc.write_table(Table.from_pandas(dt), f)
