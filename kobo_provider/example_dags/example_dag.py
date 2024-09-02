from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from kobo_provider.operators.kobotoolbox import KoboToGeoNodeOperator


default_args = {
    'owner': 'KAN',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retry_delay': timedelta(minutes=3),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    "KoboToGeoNodeOperator-ExampleDAG",
    default_args=default_args,
    description='ETL Kobo-GeoNode',
    schedule_interval=None,
    tags=['KoboToolBox', 'GeoNode']
)

start = EmptyOperator(
    task_id='start',
    dag=dag
)

process_form = KoboToGeoNodeOperator(
    task_id="process_form",
    kobo_conn_id="kobo_connection_id_airflow",
    formid=39,                               # This or
    form_id_string="y4nMrHxChj3XZEQLVDwQIN", # This
    postgres_conn_id="postgres_connection_id_airflow",
    geonode_conn_id="geonode_connection_id_airflow",
    dataset_name= "dataset_name_in_geonode", # Optional
    columns=[                                # Optional
        "username",
        "column_1",
        "column_2",
        "column_3",
        "column_4",
        "column_5",
        "my_coords_column"
    ],
    mode="replace", # Optional
    dag=dag
)

end = EmptyOperator(
    task_id='end',
    dag=dag
)

start >> process_form >> end