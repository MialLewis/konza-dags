#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example DAG demonstrating the usage of the BashOperator."""
from __future__ import annotations

import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="S-6__SFTP_Log_Retrieval",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
    params={"example_key": "example_value"},
) as dag:
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    # [START howto_operator_bash]

    import sqlite3
    import pandas as pd
    import shutil as sh

    #Copy the database off prior to connecting

    #dbfile = '//prd-az1-sqlw2/Security_logs/20240103/SFTP Logs/audit_database'
    dbfileOrig = '//PRD-AZ1-SFTP1/WingFTPLog/audit_database'
    sh.copy(dbfileOrig,'audit_database')

    dbfileCopy = 'audit_database'
    con = sqlite3.connect(dbfileCopy)

    # creating cursor
    #cur = con.cursor()
    #result = cur.execute("SELECT * FROM wftp_dblogs")
    #result.fetchall()
    #table_list = [a for a in cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]
    #print(table_list)
    #con.close()

    df = pd.read_sql_query("SELECT * from wftp_dblogs", con)
    con.close()

    dfWFTPdbLogs = pd.read_sql_query("SELECT * from wftp_dblogs", con)

    con.close()

    user = Login.formuser_user
    password = Login.formuser_password
    serverAddress = Login.formuser_server
    mssqlEngine = create_engine(
        f"mssql+pymssql://{user}:{password}@prd-az1-opssql.database.windows.net:1433/formoperations")

    df = dfWFTPdbLogs.astype(str)
    with mssqlEngine.connect() as conn:
        df.to_sql(name='wingftp_wftp_dblogs', con=conn, if_exists='replace')
    
    run_this = BashOperator(
        task_id="run_after_loop",
        bash_command="echo 1",
    )
    # [END howto_operator_bash]

    run_this >> run_this_last

    for i in range(3):
        task = BashOperator(
            task_id=f"runme_{i}",
            bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
        )
        task >> run_this

    # [START howto_operator_bash_template]
    also_run_this = BashOperator(
        task_id="also_run_this",
        bash_command='echo "ti_key={{ task_instance_key_str }}"',
    )
    # [END howto_operator_bash_template]
    also_run_this >> run_this_last

# [START howto_operator_bash_skip]
this_will_skip = BashOperator(
    task_id="this_will_skip",
    bash_command='echo "hello world"; exit 99;',
    dag=dag,
)
# [END howto_operator_bash_skip]
this_will_skip >> run_this_last

if __name__ == "__main__":
    dag.test()