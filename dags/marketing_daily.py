from airflow import DAG
from airflow.operators.python import PythonOperator
from include.src.lngest_salesforce_data import fetch_salesforce_data
from include.src.Ingest_sales_order_erp_data import extract_erp_customer_data
from include.src.combine_data_forces import combine_data_for_leads_table
from include.src.data_validation import validate_leads_data
from include.src.load_to_my_sql_database import load_leads_to_warehouse
from datetime import datetime
from airflow.configuration import conf

# Set XCom pickling to True
conf.set('core', 'enable_xcom_pickling', 'True')

def pipeline(**context):
    date = context.get('execution_date', datetime.now())
    fetch_data_task = PythonOperator(
        task_id='fetch_salesforce_data',
        python_callable=fetch_salesforce_data,
        op_args=[
            "3MVG9ux34Ig8G5epaMPqbA1E25OpLuKGuGcWZixMzgV6myFvvKoIQnGrMY5mg9pTNHPBWj9GgJNuwD0TAIEIy",  # client_id
            "50D529BA45FA479E8FE492C5BC5CBE774452FF21597B1ED8D73352105428F3FC",  # client_secret
            "sfdcshared@gmail.com",  # username
            "InNn8li^AY27CRa8",  # password
            ['Id', 'FirstName', 'LastName', 'Name', 'Email', 'Phone','MailingStreet', 'MailingCity', 'MailingState', 'MailingPostalCode', 'MailingCountry',
            'Title', 'DoNotCall', 'LastModifiedDate']  # selected_columns
        ]
    )

    extract_erp_task = PythonOperator(
    task_id='extract_erp_data',
    python_callable=extract_erp_customer_data,
    op_args =[       
            date,
            "kinterview-db.cluster-cnawrkmxrmmc.us-west-2.rds.amazonaws.com",
            3306,
            "adventureworks",
            "adventureworks_sharjeel",
            "7Yj62TiQ9o3xHVR9"
    ],
    provide_context=True,
    dag=dag,
    )

    combine_data_task = PythonOperator(
        task_id='combine_data',
        python_callable=combine_data_for_leads_table,
        provide_context=True,
    )

    data_validation = PythonOperator(
        task_id='validate_data',
        python_callable=validate_leads_data,
        provide_context=True,
    )

    load_data = PythonOperator(
        task_id='load_data_to_warehouse',
        python_callable=load_leads_to_warehouse,
        provide_context=True,
    )

    # Task dependencies (example)
    [fetch_data_task >> extract_erp_task] >> combine_data_task >> data_validation >> load_data 

# Define the DAG
with DAG(
    dag_id='two_circles_data_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # No schedule
    catchup=False,
    tags=['ETL'],
) as dag:

    # Call the pipeline function to define tasks inside the DAG context
    pipeline()

