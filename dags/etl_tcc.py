from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator 
from airflow.operators.bash import BashOperator
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# URL do CSV
url = 'https://raw.githubusercontent.com/Filipecdj/tcc2/main/data/Airline%20Dataset%20Updated%20-%20v2.csv'

def ajusta_dados(url, start_id=15):

    def adjust_date(date_str):
        parts = date_str.split('/')
        if len(parts) == 3:
            month, day, year = map(int, parts)
            return f'{day:02d}-{month:02d}-{year:02d}'
        else:
            return date_str

    df = pd.read_csv(url, sep=',', encoding='latin1')

    df['Departure Date'] = df['Departure Date'].apply(adjust_date)
    df['Departure Date'] = pd.to_datetime(df['Departure Date'], dayfirst=True)
    df['Departure Date'] = df['Departure Date'].astype(str)

    df['id'] = range(start_id, start_id + len(df))
    df = df[['id'] + [col for col in df.columns if col != 'id']]

    new_column_names = {col: col.replace(' ', '_') for col in df.columns}
    df.rename(columns=new_column_names, inplace=True)

    return df

def media_idade_por_nacionalidade():
    df = ajusta_dados(url)
    average_nationality = df.groupby('Nationality')['Age'].mean().reset_index()
    return average_nationality

def contagem_voos_por_pais():
    df = ajusta_dados(url)
    flight_counts = df.groupby('Country_Name').size().reset_index(name='Flight_Count')
    return flight_counts

def contagem_generos_por_pais():
    df = ajusta_dados(url)
    grouped = df.groupby(['Country_Name', 'Gender']).size().unstack(fill_value=0).reset_index()
    return grouped

def salva_carga_Full_GBQ():
    data = ajusta_dados(url)
    key_path = r"/opt/airflow/dados/GBQ.json"
    credentials = service_account.Credentials.from_service_account_file(key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    data.to_gbq(destination_table='base_tcc.Airline_Dataset_Full', if_exists='replace', credentials=credentials)

def salva_media_idade_por_nacionalidade_GBQ():
    data = media_idade_por_nacionalidade()
    key_path = r"/opt/airflow/dados/GBQ.json"
    credentials = service_account.Credentials.from_service_account_file(key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    data.to_gbq(destination_table='base_tcc.Avg_Airline', if_exists='replace', credentials=credentials)

def salva_contagem_voos_por_pais_GBQ():
    data = contagem_voos_por_pais()
    key_path = r"/opt/airflow/dados/GBQ.json"
    credentials = service_account.Credentials.from_service_account_file(key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    data.to_gbq(destination_table='base_tcc.Qtd_Voos_Airline', if_exists='replace', credentials=credentials)

def salva_contagem_generos_por_pais_GBQ():
    data = contagem_generos_por_pais()
    key_path = r"/opt/airflow/dados/GBQ.json"
    credentials = service_account.Credentials.from_service_account_file(key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    data.to_gbq(destination_table='base_tcc.Agrp_Nacionalidade_Airline', if_exists='replace', credentials=credentials)

with DAG('dag_etl', start_date=datetime(2023, 9, 22),
         schedule_interval='30 * * * *', catchup=False) as dag:
    
    salva_carga_Full_GBQ_task = PythonOperator(
        task_id='carga_Full_GBQ',
        python_callable=salva_carga_Full_GBQ 
    )
    
    salva_media_idade_por_nacionalidade_GBQ_task = PythonOperator(
        task_id='salva_media_idade_por_nacionalidade_GBQ',
        python_callable=salva_media_idade_por_nacionalidade_GBQ  
    )

    salva_contagem_voos_por_pais_GBQ_task = PythonOperator(
        task_id='salva_contagem_voos_por_pais_GBQ',
        python_callable=salva_contagem_voos_por_pais_GBQ  
    )

    salva_contagem_generos_por_pais_GBQ_task = PythonOperator(
        task_id='salva_contagem_generos_por_pais_GBQ',
        python_callable=salva_contagem_generos_por_pais_GBQ  
    )

    salva_carga_Full_GBQ_task >> [salva_media_idade_por_nacionalidade_GBQ_task, salva_contagem_voos_por_pais_GBQ_task, salva_contagem_generos_por_pais_GBQ_task]

