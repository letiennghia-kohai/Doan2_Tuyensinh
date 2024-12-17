import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.wish_pipeline import process_wish_data
from pipelines.wish_pipeline import etl_to_mysql
from pipelines.notification_pipeline import crawl_notifications_task
from pipelines.notification_pipeline import process_notifications_task
from pipelines.qna_pipeline import process_qna_data
# from pipelines.mysql_pipeline import load_to_mysql
# from pipelines.mongodb_pipeline import load_to_mongodb

default_args = {
    'owner': 'NghiaLT',
    'start_date': datetime(2024, 12, 1),
}

dag = DAG(
    dag_id='tuyensinh_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['tuyensinh', 'etl', 'pipeline']
)

# Process Wish Data
process_wish_stage1 = PythonOperator(
    task_id='process_wish_stage1',
    python_callable=process_wish_data,
    op_kwargs={'stage': 1},
    dag=dag
)

process_wish_stage2 = PythonOperator(
    task_id='etl_to_mysql',
    python_callable=etl_to_mysql,
    dag=dag
)

# Process Notification Data
process_notification_stage1 = PythonOperator(
    task_id='crawl_notifications',
    python_callable=crawl_notifications_task,
    dag=dag
)

process_notification_stage2 = PythonOperator(
    task_id='process_notifications',
    python_callable=process_notifications_task,
    dag=dag
)

# Process Q&A Data
process_qna_stage1 = PythonOperator(
    task_id='process_qna_stage1',
    python_callable=process_qna_data,
    op_kwargs={'stage': 1},
    dag=dag
)

process_qna_stage2 = PythonOperator(
    task_id='process_qna_stage2',
    python_callable=process_qna_data,
    op_kwargs={'stage': 2},
    dag=dag
)

# # Load to MySQL Data Warehouse
# load_to_mysql = PythonOperator(
#     task_id='load_to_mysql',
#     python_callable=load_to_mysql,
#     op_kwargs={'database': 'data_warehouse'},
#     dag=dag
# )

# # Load to MongoDB for Chatbot
# load_to_mongodb = PythonOperator(
#     task_id='load_to_mongodb',
#     python_callable=load_to_mongodb,
#     op_kwargs={'database': 'chatbot_warehouse'},
#     dag=dag
# )

# Define dependencies
# [process_wish_stage1 >> process_wish_stage2,
#  process_notification_stage1 >> process_notification_stage2, process_qna_stage1 >> process_qna_stage2]
#  process_qna_stage1 >> process_qna_stage2] >> [load_to_mysql, load_to_mongodb]

# process_wish_stage1 >> process_wish_stage2 >> process_qna_stage1 >> process_qna_stage2

[process_wish_stage1 >> process_wish_stage2,
 process_notification_stage1 >> process_notification_stage2, process_qna_stage1 >> process_qna_stage2]