import datetime as dt
import subprocess
import pandas as pd
import numpy as np
import logging
import psycopg2
import os
import ast

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow import models
# from airflow.models.baseoperator import chain
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import ShortCircuitOperator
# from airflow.sensors.filesystem import FileSensor 
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.common.sql.sensors.sql import SqlSensor

################################################################
# Functions

class OSDagManager():
    '''Class that contains the functions necessary for managing OS dags by turning them on when required'''
    def __init__(self,range):
        self.product_range=range
        self.master_path = self.convert_path(os.path.abspath(__file__)) # assumes the file is in the correct directory
        self.default_path=self.convert_path(f"{self.master_path}/Ordnance_Survey/{self.product_range}")

        # defining the logger
        self.logger = logging.getLogger(__name__)

        # looks at the metabase for all the information on the dags
        search_query=f"SELECT dag_id, root_dag_id, is_paused FROM airflow.dag WHERE name dag_id ilike '%{self.product_range}%'"

        # format data
        with open(self.convert_path(f"{self.default_path}/{self.product_range}_formats.txt"), 'r') as file:
            formats_str = file.read()
        self.formats = ast.literal_eval(formats_str)

        # gets the dataframe from the database for use
        self.df = self.metabase_to_dataframe(search_query)

    def convert_path(self,path):
        '''Converts path to one that will work with the system being used'''
        return os.path.normpath(path)

    def metabase_to_dataframe(sql):
        '''Connects to the meta database, pulls records from the query and saves it to a pandas dataframe to be processed'''
        # conn made manually as the connection id within airflow creates it wrong
        conn = psycopg2.connect(database="airflow_db", user="airflow_admin", password=models.Variable.get('airflow_admin_passwd'), host="localhost", port="5432")
        cur = conn.cursor()
        cur.execute(sql)
        records = cur.fetchall()
        # saves the query as a dataframe
        df = pd.DataFrame(records, columns=[desc[0] for desc in cur.description])
        cur.close()
        conn.close()
        # replacing all the empty values with NaN
        df.replace('', np.nan, inplace=True)
        return df

    def ignore_products(*args):
        '''Creates a list of products to be ignored'''
        return list(args)

    def run_bash_command(self,item,item_dict,version="Only version"):
        '''Goes through dataframe and product to run the bash command required'''

        self.logger.info(f"Processing {item}: {version}")
        
        # looks for schedule and dag_id in the dictionary to determine if it needs to be operated on
        if "schedule" in item_dict and "dag_id" in item_dict:
            dag_id=item_dict['dag_id']
            # looks for the dag_id in the dictionary and checks if the dag is paused or not
            row = self.df[self.df['dag_id'].str.contains(dag_id, case=False)]
            # exits the function if more/less than 1 row is found - something is wrong and needs to be debugged
            if len(row) != 1:
                return self.logger.error(f"Incorrect number of matching dag_id's for {item}: {version}")
            elif self.df.loc[0, 'root_dag_id'] != None:
                # if the dag contains a root dag, skip it to not interfere with the root dag
                return self.logger.info(f"{dag_id} is a sub dag, skipping")
            else:
                # checks if the product is paused
                if self.df.loc[0, 'is_paused']:
                    # if paused, will check the calendar to determine if needs to be unpaused or not
                    current_month = dt.datetime.now().strftime("%B")
                    if current_month in item_dict['schedule']:
                        # runs bash command to unpause the dag and move onto the next one
                        process=subprocess.run(f"airflow dags unpause {dag_id}", shell=True)
                        return self.logger.info(f"{dag_id} unpaused, code: {process.returncode}")
                    else:
                        return self.logger.info(f"{current_month} not in {item}: {version} schedule, {dag_id} remaining unpaused")
                else:
                    return self.logger.info(f"{dag_id} is unpaused, skipping")
        else:
            return self.logger.info(f"No dag_id or schedule for {item}: {version}")
        
    def open_main(self,ignore_list=None):
        '''Checks if open products require multiple formats and runs the appropiate code'''

        for item in self.formats:
            # ignores the product if its on the ignore list
            if ignore_list!=None and item in ignore_list:
                pass
            # checks if the product has alternate formats and looks at the correct dictionary for it
            elif any("alternate" in key for key in self.formats[item].keys()) == True:
                for alternate in self.formats[item].keys():
                    item_dict=self.formats[item][alternate]
                    self.run_bash_command(item,item_dict,alternate)
            else:
                item_dict=self.formats[item]
                self.run_bash_command(item,item_dict)

def main():
    '''The main python function that runs through all products and dags'''
    OS_Open=OSDagManager("os_open")
    OS_Open.open_main()

################################################################
# Airflow variables

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
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
    'OS_Open_Manager', 
    default_args=default_args,
    description='Checks all OS Open dags that write to the database, compares their schedule to the current month and unpauses them when needed',
    schedule='0 6 1 * *', # runs the dag at the start of every month at 6am (5am UTC)
    # 'minute hour day month dayOfWeek'
    catchup=False,
    max_active_runs=1,
    tags=['Ordnance Survey','OS Open','Production','Main Pipeline']
)

# task than runs it all
configure_dags = PythonOperator(
    task_id='configure_dags',
    python_callable=main,
    dag=dag,
    depends_on_past=False
)
