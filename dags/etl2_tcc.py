from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator 
from airflow.operators.bash import BashOperator
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd


def trata_colunas(arquivo_num):
    colnames = ['Transaction_unique_identifier', 'price', 'Date_of_Transfer',
                'postcode', 'Property_Type', 'Old_New',
                'Duration', 'PAON', 'SAON',
                'Street', 'Locality', 'Town_City',
                'District', 'County', 'PPDCategory_Type',
                'Record_Status_monthly_file_only'
                ]

    df = pd.read_csv(f"/opt/airflow/dados/arquivo_saida_{arquivo_num}",
                     header=None,
                     names=colnames,
                     infer_datetime_format=True,
                     parse_dates=["Date_of_Transfer"],
                     dayfirst=False
                     )
    
    df['price'] = df['price'].astype(int)
    df['Date_of_Transfer'] = df['Date_of_Transfer'].astype(str)
    df['PAON'] = df['PAON'].astype(str)

    df = df.drop(['PPDCategory_Type', 'Record_Status_monthly_file_only'], axis=1)

    return df

def carga_bigquery(df):
    key_path = r"/opt/airflow/dados/GBQ.json"
    credentials = service_account.Credentials.from_service_account_file(key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    df.to_gbq(destination_table='base_tcc.UK_Property', if_exists='replace', credentials=credentials)

def executa_bigquery():
    for arquivo_num in range(1, 3):
        df = trata_colunas(arquivo_num)
        carga_bigquery(df)


with DAG('dag_etl_uk', start_date=datetime(2023, 10, 25),
         schedule_interval='30 * * * *', catchup=False) as dag:
    
    executa_bigquery_task = PythonOperator(
        task_id= 'executa_bigquery',
        python_callable = executa_bigquery 
    )

    executa_bigquery_task