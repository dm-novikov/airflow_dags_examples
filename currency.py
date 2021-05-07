from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import requests as req
import pandas as pd


def date_transform(date):
    dt = date.split('-')
    return f'{dt[2]}/{dt[1]}/{dt[0]}'


def download_xml_file(tmp_file, **kwargs) -> None:
    url = 'http://www.cbr.ru/scripts/XML_daily.asp'
    date = kwargs['yesterday_ds']

    request = req.get(f'{url}?date_req={date_transform(date)}')

    with open(f'{tmp_file}', 'w', encoding='utf-8') as tmp_file:
        tmp_file.write(request.text)


def transform_xml_file(tmp_file, csv_file, **kwargs) -> None:
    rows = list()
    parser = ET.XMLParser(encoding="utf-8")
    df_cols = ['num_code', 'char_code', 'nominal',
               'name', 'value', 'date']
    date = datetime.strptime(kwargs['yesterday_ds'],
                             '%Y-%m-%d')

    tree = ET.parse(tmp_file, parser=parser).getroot()

    for child in tree.findall('Valute'):
        num_code = child.find('NumCode').text
        char_code = child.find('CharCode').text
        nominal = child.find('Nominal').text
        name = child.find('Name').text
        value = float(child.find('Value').text.replace(',', '.'))

        rows.append((num_code, char_code, nominal,
                     name, value, date))

    out_df = pd.DataFrame(rows, columns=df_cols)
    out_df.to_csv(csv_file, sep=",", encoding='utf-8')


default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=180)
    }

with DAG('finance_centrobank',
         start_date=datetime(2021, 5, 6),
         max_active_runs=1,
         schedule_interval="0 6 * * *",
         default_args=default_args,) as dag:

    PythonOperator(
        task_id="download_xml_file",
        python_callable=download_xml_file,
        provide_context=True,
        op_kwargs={'tmp_file': '/tmp/file.xml'},
        dag=dag,
    ) >> PythonOperator(
        task_id="transform_xml_file",
        python_callable=transform_xml_file,
        provide_context=True,
        op_kwargs={'tmp_file': '/tmp/file.xml',
                   'csv_file': '/usr/local/airflow/dags/file.csv'},
        dag=dag,
    )
