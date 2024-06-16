from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from datetime import datetime, timedelta

def get_data(**kwargs):
    url = 'https://raw.githubusercontent.com/plotly/datasets/master/sales_success.csv'
    response = requests.get(url)

    if(response.status_code == 200):
        df = pd.read_csv(url)

        # convert dataframe to json string and push to xCom
        json_data = df.to_json(orient='records')

        kwargs['ti'].xcom_push(key='data', value=json_data)
    
    else:
        raise Exception(f'Failed to get data, HTTP Status code: {response.status_code}')


def preview_data(**kwargs):
    output_data = kwargs['ti'].xcom_pull(key='data', task_ids='get_data')
    print(output_data)

    if output_data:
        output_data = json.loads(output_data)
    else:
        raise ValueError('No data received from XCom')

    df = pd.DataFrame(output_data)
    df['SalesRatio'] = df['sales'] / df['calls']
    df = df.groupby('region', as_index=False).agg({'sales': 'sum', 'SalesRatio': 'mean'})

    df = df.sort_values(by='SalesRatio', ascending=False)

    print(df.head(20))

default_args = {
    'owner': 'mohith',
    'start_date': datetime(2024, 6, 15),
    'catchup': False
}

dag = DAG(
    'fetch_and_preview',
    default_args = default_args,
    schedule=timedelta(days=1)
)

get_data_from_url = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    dag=dag
)

preview_data_from_url = PythonOperator(
    task_id='preview_data',
    python_callable=preview_data,
    dag=dag
)



get_data >> preview_data_from_url