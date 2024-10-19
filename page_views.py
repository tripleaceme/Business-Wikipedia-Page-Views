from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta 
from airflow.utils.dates import days_ago
from urllib import request
import os


os.environ["no_proxy"]="*"

'''
The URL is constructed of various date and time components:
https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month}/pageviews-{year}{month}{day}-{hour}0000.gz

The output file is ordered in the format below:
ab 1427 2 0
ab - Domain Code
1427 - Page Title
2 - View Count
0 - Response in byte size

'''

default_args = {'owner': 'Ayoade',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        }

def _get_data(execution_date):

    year, month, day, hour, *_ = execution_date.timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
        )
    output_path = "/Users/mac/Documents/local_airflow/pageviews/wikipageviews.gz"
    request.urlretrieve(url, output_path)


def _fetch_pageviews(pagenames, execution_date, **remains):
    result = dict.fromkeys(pagenames, 0)
    with open("/Users/mac/Documents/local_airflow/pageviews/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, size = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts

    with open("/Users/mac/Documents/local_airflow/pageviews/postgres_query.sql", "w") as f:
        for pagename, viewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', {viewcount}, '{execution_date}'"
                ");\n"
            )

with DAG(
    dag_id="wikipedia_page_views",
    description="Building data company's hourly wikipedia page views. DAG fetching hourly Wikipedia pageviews and writing results to Postgres",
    default_args=default_args,start_date=days_ago(1),
    schedule_interval="@hourly",catchup=False,
    template_searchpath=[r"/Users/mac/Documents/local_airflow/pageviews/"]) as dag:

    
    # Download data
    '''get_data_bash = BashOperator(
        task_id="get_data_bash",
        bash_command=(
            "curl -o /Users/mac/Documents/local_airflow/pageviews/wikipageviews.gz " 
            "https://dumps.wikimedia.org/other/pageviews/"
            "{{ execution_date.year }}/"
            "{{ execution_date.year }}-{{ '{:02}'.format(execution_date.month) }}/"
            "pageviews-{{ execution_date.year }}"
            "{{ '{:02}'.format(execution_date.month) }}"
            "{{ '{:02}'.format(execution_date.day) }}-"
            "{{ '{:02}'.format(execution_date.hour) }}0000.gz" )
            )'''


    get_data_python = PythonOperator(
        task_id="get_data_python", 
        python_callable=_get_data
        )


    # extract downloaded file    
    extract_gz = BashOperator(
        task_id="extract_gz", 
        bash_command="gunzip --force /Users/mac/Documents/local_airflow/pageviews/wikipageviews.gz"
        )

    #fetch page views
    fetch_pageviews = PythonOperator(
        task_id="fetch_pageviews",
        python_callable=_fetch_pageviews,
        # businesses that we want to see thier wikipedia hourly page views
        op_kwargs={"pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook", "Youtube", "Twitter"}}
    )
    #write_to_db
    write_to_db = PostgresOperator(
        task_id="write_to_db",
        postgres_conn_id="postgres_db",
        sql="postgres_query.sql"
        )
    

get_data_python >> extract_gz >> fetch_pageviews >> write_to_db
