from airflow.sdk import DAG
from pendulum import datetime
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from wikipage.include.load import load_data_postgres
from wikipage.include.query import _fetch_top_company

output_path = "/opt/airflow/dags/wikipage/files"
wiki_url = 'https://dumps.wikimedia.org/other/pageviews/2025/2025-10/pageviews-20251016-050000.gz'
filename = wiki_url.split("/")[-1]
unzip_filepath = f"/opt/airflow/dags/wikipage/files/{filename.split(".")[0]}"

with DAG(
    dag_id="wiki_pageviews",
    start_date=datetime(2025, 10, 22),
    schedule=None
):
    
    download_zipfile = BashOperator(
        task_id="download_zipfile",
        bash_command=f"wget -P {output_path} {wiki_url}"
    )

    unzip_file = BashOperator(
        task_id="unzip_file",
        bash_command=f"gunzip {output_path}/{filename}"
    )

    create_dbtable = SQLExecuteQueryOperator(
        task_id="create_database_table",
        conn_id="postgres_conn",
        sql="sql/create_table.sql"
    )

    populate_dbtable = PythonOperator(
        task_id="populate_db_table",
        python_callable=load_data_postgres,
        op_kwargs={"file_path": unzip_filepath}
    )

    fetch_top_company = PythonOperator(
        task_id="fetch_top_company",
        python_callable=_fetch_top_company
    )

    notify_mail = EmailOperator(
        task_id="notify_mail",
        to=["orasaq377@stu.ui.edu.ng"],
        subject="Wikipedia Pageviews Data Load Complete for {{ ds }}",
        html_content="""
        <h3>Wikipedia Pageviews Data Load Update</h3><br>
        <p>Hi Manager, <br><br>
        The Airflow pipeline ran successfully and the Wikipedia pageviews data has been loaded into the database.<br>
        The company with the highest number of views is 
        {{ ti.xcom_pull(task_ids='fetch_top_company', key='top_company')[0] }} 
        with {{ ti.xcom_pull(task_ids='fetch_top_company', key='top_company')[1] }} views.<br><br>
        Kind regards, Olalekan.</p>
        """,
        conn_id="smtp_conn"
    )

download_zipfile >> unzip_file >> create_dbtable >> populate_dbtable >> fetch_top_company >> notify_mail