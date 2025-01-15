import datetime
import os
import requests
import pendulum
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.models import Connection
from airflow import settings
from sqlalchemy import inspect,create_engine

os.environ["no_proxy"]="*"

@dag(
    dag_id="weather-telegram",
    schedule="@once",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)

def WetherETL():

    send_message_telegram_task = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='telegram_conn',
        token='7248934509:AAE4KtbM5wDjmrU-xFH8bMAwBwDZw-C_cMA',
        chat_id='839424934',
        text='Weather in Moscow \nYandex: ' + "{{ ti.xcom_pull(task_ids=['yandex_weather'],key='weather')[0]}}" + " degrees" +
        "\nOpen weather: " + "{{ ti.xcom_pull(task_ids=['open_weather'],key='open_weather')[0]}}" + " degrees",
    )

    @task(task_id='yandex_weather')
    def get_yandex_weather(**kwargs):
        ti = kwargs['ti']
        url = "https://api.weather.yandex.ru/v2/informers/?lat=55.75396&lon=37.620393"

        payload={}
        headers = {
        'X-Yandex-API-Key': '33f45b91-bcd4-46e4-adc2-33cfdbbdd88e'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        print("test")
        a=response.json()['fact']['temp']
        print(a)
        ti.xcom_push(key='weather', value=response.json()['fact']['temp'])
#        return str(a)
    @task(task_id='open_weather')
    def get_open_weather(**kwargs):
        ti = kwargs['ti']
        url = "https://api.openweathermap.org/data/2.5/weather?lat=55.749013596652574&lon=37.61622153253021&appid=2cd78e55c423fc81cebc1487134a6300"

        payload={}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)
        print("test")
        a=round(float(response.json()['main']['temp']) - 273.15, 2)
        print(a)
        ti.xcom_push(key='open_weather', value=round(float(response.json()['main']['temp']) - 273.15, 2))
#        return str(a)
    @task(task_id='python_weather')
    def get_weather(**kwargs):
        print("Yandex "+str(kwargs['ti'].xcom_pull(task_ids=['yandex_weather'],key='weather')[0])+" Open "+str(kwargs['ti'].xcom_pull(task_ids=['open_weather'],key='open_weather')[0]))
    @task(task_id='python_table')
    def get_weather_table(**kwargs):
        con=create_engine("mysql://Airflow:1@localhost:33061/spark")
        data = [[str(kwargs['ti'].xcom_pull(task_ids=['yandex_weather'],key='weather')[0]), str(kwargs['ti'].xcom_pull(task_ids=['open_weather'],key='open_weather')[0]), datetime.datetime.now()]]
        df = pd.DataFrame(data)
        df.to_sql('weather',con,schema='spark',if_exists='replace',index=False)
        
    get_yandex_weather() >> get_open_weather() >> get_weather()>> get_weather_table() >> send_message_telegram_task 



dag = WeatherETL()
