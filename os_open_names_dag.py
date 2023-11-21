import logging
import datetime as dt
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import models
from airflow.models.baseoperator import chain
# from airflow.operators.python import ShortCircuitOperator
# from airflow.sensors.filesystem import FileSensor 
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

################################################################
# Functions

def query_metabase(sql):
    '''Connects to the meta database and pulls the top record from the query'''
    # conn made manually as the connection id within airflow creates it wrong
    conn = psycopg2.connect(database=models.Variable.get('airflow_db'), user=models.Variable.get('airflow_admin_user'), password=models.Variable.get('airflow_admin_passwd'), host=models.Variable.get('airflow_db_host'), port=models.Variable.get('airflow_db_port'))
    cur = conn.cursor()
    cur.execute(sql)
    record = cur.fetchone()
    cur.close()
    conn.close()
    #  extract the record from the tuple, remove the string quotation marks from the xcom
    clean_record=record[0][1:-1]
    return clean_record

# change the product to look for the specific product & time interval to look for
product="OpenNames"
interval="7 days" # can also be hours
# query looks at the metabase to see if an xcom has been generated for os open, product (within 1 week of the check)
search_query=f"SELECT encode(value,'escape') FROM airflow.xcom WHERE (dag_id ilike 'os_open_upload' or dag_id ilike 'os_open_pipeline') AND (key ilike '{product}' AND task_id ilike 'upload_shareddrive' AND timestamp > current_timestamp - INTERVAL '{interval}') ORDER BY timestamp DESC LIMIT 1;"

################################################################
# Airflow variables

# defining the logger
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2023,9,19,10,30,0),
    'email': ['user@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=5)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# creating the instance of the dag
dag = DAG(
    'OS_OpenNames', 
    default_args=default_args,
    description='Checks if OS OpenNames needs to be updated and writes it to the database',
    schedule='0 8 * * *', # checks it once a day at 8 (7am UTC)
    # 'minute hour day month dayOfWeek'
    catchup=False,
    max_active_runs=1,
    tags=['Ordnance Survey','OS Open','Production','Subprocess']
)

# SqlSensor task that checks if the database has an entry
sql_sensor = SqlSensor(
    task_id='check_for_openname_xcom',
    dag=dag,
    conn_id='airflow_db',
    sql=search_query,
    poke_interval=60*20, # checks the airflow metadatabase every 20 minutes
    timeout=60*60*4, # will count as a fail if the query takes longer than the timeout (4 hours)
    soft_fail=True # will count the task as skipped if the criteria is not met
)

# pulls the xcom into a variable to be used
pull_xcom = PythonOperator(
    task_id='pull_xcom',
    python_callable=query_metabase,
    dag=dag,
    op_kwargs={'sql': search_query}
)

# access bash script to write to database
write2database = BashOperator(
    task_id= 'write2database',
    dag=dag,
    depends_on_past=True,
    pool='postgres_pool',
    bash_command="models.Variable.get('open_names_airflow_script_path') {{ var.value.svc_passwd }} {{ ti.xcom_pull(task_ids=\'pull_xcom\') }}" 
)

# when the script is successful once, pause the dag as it doesn't have to be used again
pause_dag = BashOperator(
    task_id= 'pause_dag',
    dag=dag,
    depends_on_past=True,
    bash_command='airflow dags pause OS_OpenNames' 
)

# dependencies
chain(sql_sensor,pull_xcom,write2database,pause_dag)
