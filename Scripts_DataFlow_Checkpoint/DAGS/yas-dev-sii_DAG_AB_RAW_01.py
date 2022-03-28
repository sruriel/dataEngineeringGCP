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
    'start_date': days_ago(1),
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

dag = DAG('dag-sii-bch-ing-ab-raw', default_args=default_args, schedule_interval='59 4 * * *', tags=['RAW'])

raw_estatus_cuentas = DataflowTemplateOperator(
    template='gs://yas-sii-int-des-dev/AB/templates/TPL_SII_BCH_ING_AB_RAW_ESTATUS_CUENTAS',
    job_name='sii-bch-ing-ab-raw-estatus-cuentas',
    task_id='sii-bch-ing-ab-raw-estatus-cuentas',
    location=gce_region,
    gcp_conn_id='google_cloud_yas_sa_sii_de',
    dag=dag)

raw_servicios_cuenta = DataflowTemplateOperator(
    template='gs://yas-sii-int-des-dev/AB/templates/TPL_SII_BCH_ING_AB_RAW_SERVICIOS_CUENTA',
    job_name='sii-bch-ing-ab-raw-servicios-cuenta',
    task_id='sii-bch-ing-ab-raw-servicios-cuenta',
    location=gce_region,
    gcp_conn_id='google_cloud_yas_sa_sii_de',
    dag=dag)
    
raw_tipos_cuentas = DataflowTemplateOperator(
    template='gs://yas-sii-int-des-dev/AB/templates/TPL_SII_BCH_ING_AB_RAW_TIPOS_CUENTAS',
    job_name='sii-bch-ing-ab-raw-tipos-cuentas',
    task_id='sii-bch-ing-ab-raw-tipos-cuentas',
    location=gce_region,
    gcp_conn_id='google_cloud_yas_sa_sii_de',
    dag=dag)

raw_tipos_transacciones = DataflowTemplateOperator(
    template='gs://yas-sii-int-des-dev/AB/templates/TPL_SII_BCH_ING_AB_RAW_TIPOS_TRANSACCIONES',
    job_name='sii-bch-ing-ab-raw-tipos-transacciones',
    task_id='sii-bch-ing-ab-raw-tipos-transacciones',
    location=gce_region,
    gcp_conn_id='google_cloud_yas_sa_sii_de',
    dag=dag)
    
raw_servicios = DataflowTemplateOperator(
    template='gs://yas-sii-int-des-dev/AB/templates/TPL_SII_BCH_ING_AB_RAW_SERVICIOS',
    job_name='sii-bch-ing-ab-raw-servicios',
    task_id='sii-bch-ing-ab-raw-servicios',
    location=gce_region,
    gcp_conn_id='google_cloud_yas_sa_sii_de',
    dag=dag)
