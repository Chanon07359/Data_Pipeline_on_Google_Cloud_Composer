from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd

MYSQL_CONNECTION = "mysql_default" 
list_nafill=['City','Company Ownership','Type'];
raw_data_from_DB_path = "/home/airflow/gcs/data/raw_accident_table.csv"
accident_table_path = "/home/airflow/gcs/data/accident_table.csv"

def get_data_from_DB(raw_data_from_DB_path):
    mysqlserver = MySqlHook(MYSQL_CONNECTION)
    raw_data = mysqlserver.get_pandas_df(sql="SELECT * FROM work_accident")
    raw_data.to_csv(raw_data_from_DB_path, index=False)
    print(f"Output to {raw_data_from_DB_path}")


def clean_data(raw_data_from_DB_path,accident_table_path,list_nafill):
    accident_table=pd.read_csv(raw_data_from_DB_path)
    accident_table.drop(['Title','Sub Industry','Company Name','Number of Punished','Financial Penalty'], axis=1, inplace=True)
    accident_table['Date']=pd.to_datetime(accident_table['Date'])
    for i in list_nafill :
        accident_table[i].fillna("Unknow", inplace = True)
    accident_table.to_csv(accident_table_path, index=False)
    print(f"Output to {accident_table_path}")

with DAG(
    "Work_Accidents_in_China",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["Basic Data Pipeline"]
) as dag:

    t1 = PythonOperator(
        task_id="Get_raw_data_from_DB",
        python_callable=get_data_from_DB,
        op_kwargs={"raw_data_from_DB_path": raw_data_from_DB_path},
    )

    t2 =PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
        op_kwargs={"raw_data_from_DB_path":raw_data_from_DB_path ,"accident_table_path": accident_table_path ,"list_nafill":list_nafill}
    )

    t3 =BashOperator(
        task_id="Backup_raw_data_to_BQ",
        bash_command="bq load --source_format=CSV --autodetect [DATASET].[TABLE_NAME] gs://[GCS_BUCKET]/data/raw_accident_table.csv" #Upload to BQ by bq command
    )

    t4 =BashOperator(
        task_id="Backup_final_data_to_BQ",
        bash_command="bq load --source_format=CSV --autodetect [DATASET].[TABLE_NAME] gs://[GCS_BUCKET]/data/accident_table.csv" #Upload to BQ by bq command
    )                                                                                                                                

    t1 >> [t2,t3] >> t4 



