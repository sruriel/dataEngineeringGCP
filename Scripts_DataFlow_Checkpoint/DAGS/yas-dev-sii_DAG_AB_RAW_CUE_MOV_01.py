import datetime
from datetime import timedelta
from airflow import models

from httplib2 import Http
from airflow import DAG
from airflow.models import Variable

from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.utils.dates import days_ago

bucket_temp_path="gs://yas-sii-int-des-dev/temp_dir/"
project_id="yas-dev-sii-pid"
gce_region="us-east1"
gce_zone="us-east1-c"
machineType_exe = 'n2d-standard-2'

default_args = {
    'owner': 'sii-dwh',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 8, 5, 17, 0),
    'email': 'e-rmontano@gentera.com.mx',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,   
    'retry_delay': timedelta(minutes=30),
    'dataflow_default_options': {
        'project' : project_id,
        'zone' : gce_zone,
        'tempLocation' : bucket_temp_path,
        'machineType' : machineType_exe,
        'serviceAccountEmail': 'yas-dev-sii-pid-sa@yas-dev-sii-pid.iam.gserviceaccount.com',
        'subnetwork' : "https://www.googleapis.com/compute/v1/projects/sha-net-dev-id/regions/us-east1/subnetworks/subnet-analytics-region-a"   
    }
    }

dag = DAG('dag-sii-bch-ing-ab-raw-cue-mov', default_args=default_args, schedule_interval='0 13,14,15,16,17,18,19,20,21,22,23,0,1 * * *', tags=['RAW','Movimientos','Cuentas'])

raw_cuentas = DataflowTemplateOperator(
    template='gs://yas-sii-int-des-dev/AB/templates/TPL_SII_BCH_ING_AB_RAW_CUENTAS',
    job_name='sii-bch-ing-ab-raw-cuenta',
    task_id='sii-bch-ing-ab-raw-cuenta',
    location=gce_region,
    gcp_conn_id='google_cloud_yas_sa_sii_de',
    dag=dag)

raw_movimientos = DataflowTemplateOperator(
    template='gs://yas-sii-int-des-dev/AB/templates/TPL_SII_BCH_ING_AB_RAW_MOVIMIENTOS',
    job_name='sii-bch-ing-ab-raw-movimientos',
    task_id='sii-bch-ing-ab-raw-movimientos',
    location=gce_region,
    gcp_conn_id='google_cloud_yas_sa_sii_de',
    dag=dag)
