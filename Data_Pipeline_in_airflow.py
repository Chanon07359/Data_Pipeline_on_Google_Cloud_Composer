from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd

MYSQL_CONNECTION = "mysql_default" 
list_nafill=['City','Company Ownership','Type'];
mysql_output_path = "/home/airflow/gcs/data/audible_data_merged.csv"
conversion_rate_output_path = "/home/airflow/gcs/data/conversion_rate.csv"
final_output_path = "/home/airflow/gcs/data/output.csv"

def get_data_from_DB(transaction_path):
    mysqlserver = MySqlHook(MYSQL_CONNECTION)
    audible_data = mysqlserver.get_pandas_df(sql="SELECT * FROM ")
    df.to_csv(transaction_path, index=False)
    print(f"Output to {transaction_path}")


def clean_data(accident_table_path):
    global list_nafill
    accident_table=pd.read_csv(accident_table_path)
    accident_table.drop(['Title','Sub Industry','Company Name','Number of Punished','Financial Penalty'], axis=1, inplace=True)
    accident_table["City"] = accident_table.apply(lambda x: x["City"].replace(",","'"), axis=1)
    accident_table['Date']=pd.to_datetime(accident_table['Date'])
    for i in list_nafill :
        raw_table[i].fillna("Unknow", inplace = True)
    print(f"Output to {accident_table_path}")

with DAG(
    "Work_Accidents_in_China",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["Basic Data Pipeline"]
) as dag:

    t1 = PythonOperator(
        task_id="get_data_from_DB",
        python_callable=get_data_from_DB,
        op_kwargs={"transaction_path": mysql_output_path},
    )

    t2 =PythonOperator(
        task_id="",
        python_callable=,
        op_kwargs=
    )

    t3 =BashOperator(
        task_id="",
        bash_command=""
    )

    t4 =BashOperator(
        task_id="",
        bash_command=""
    )

    t5 =BashOperator(
        task_id="",
        bash_command=""
    )

    t1 >> [t2,t3] >> t4 >> t5

