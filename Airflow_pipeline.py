from airflow.operators import bash_operator
from airflow import DAG
import datetime
import os
import logging
from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator 
from airflow.operators.dummy_operator import DummyOperator

today_date = datetime.datetime.now().strftime("%Y%m%d")
# yesterday ensures the DAG will run directly after implementation
yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())
table_nameP4 = 'Assignment3.ass3p4'
table_nameP5 = 'Assignment3.ass3p5'
table_nameP12 = 'Assignment3.ass3p12'

default_dag_args = {
    # set start date to today
    'start_date': yesterday,
     # Continue to run DAG once per day 
    'schedule_interval': '@daily',
    # To email on failure or retry set 'email' arg to your email and enable emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes 
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project')
}

with DAG(dag_id='assignment_3_dag', 
        default_args=default_dag_args
        # this is the start of the task
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    #logging.info('trying to bq_query: ')
    #logging.info('table name: ' + table_nameP4)
    sql_p4 = f"""  
                SELECT
                COUNT(DISTINCT fullVisitorId) AS unique_visitors,
                COUNT(v2ProductName) AS product_views,
                FROM `data-to-insights.ecommerce.all_sessions`  
                """
    bq_query_p4 = BigQueryOperator(
        task_id='bq_query_p4',
        sql=sql_p4, 
        destination_dataset_table=table_nameP4, 
        gcp_conn_id='bigquery_default', 
        use_legacy_sql=False, 
        write_disposition='WRITE_TRUNCATE', 
        create_disposition='CREATE_IF_NEEDED', 
        dag=dag
    )
    sql_p5 = f"""  
                SELECT 
                channelGrouping, 
                COUNT(DISTINCT fullVisitorId) AS UniqueVisitors
                FROM `data-to-insights.ecommerce.all_sessions`
                GROUP BY channelGrouping
                ORDER BY UniqueVisitors DESC  
                """
    bq_query_p5 = BigQueryOperator(
        task_id='bq_query_p5',
        sql=sql_p5, 
        destination_dataset_table=table_nameP5, 
        gcp_conn_id='bigquery_default', 
        use_legacy_sql=False, 
        write_disposition='WRITE_TRUNCATE', 
        create_disposition='CREATE_IF_NEEDED', 
        dag=dag
    )
    sql_p12 = f"""  
                SELECT
                v2ProductName,
                COUNT(*) AS product_views,
                COUNT(productQuantity) AS distinct_orders,
                SUM(productQuantity) AS total_units_ordered,
                ROUND(SUM(productQuantity)/COUNT(productQuantity), 2) AS average_amount_per_order
                FROM `data-to-insights.ecommerce.all_sessions`
                GROUP BY v2ProductName
                ORDER BY product_views DESC
                LIMIT 5 
                """
    bq_query_p12 = BigQueryOperator(
        task_id='bq_query_p12',
        sql=sql_p12, 
        destination_dataset_table=table_nameP12, 
        gcp_conn_id='bigquery_default', 
        use_legacy_sql=False, 
        write_disposition='WRITE_TRUNCATE', 
        create_disposition='CREATE_IF_NEEDED', 
        dag=dag
    )
    P12_export_bash = bash_operator.BashOperator(
        task_id='P12_export_bash',
        bash_command=   "bq extract --destination_format CSV \
                                    --field_delimiter=tab \
                                    --print_header=true \
                                    main-campaign-361703:Assignment3.p12 \
                                    gs://australia-southeast1-assign-cf82e8cb-bucket/p19_result.csv",
        dag=dag
    )
    start >> bq_query_p4 >> bq_query_p5 >> bq_query_p12 >> P12_export_bash >> end