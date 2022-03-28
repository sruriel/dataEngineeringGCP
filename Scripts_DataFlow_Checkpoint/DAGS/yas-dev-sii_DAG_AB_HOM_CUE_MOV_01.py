from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.dates import days_ago

import datetime
from datetime import timedelta

from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from googleapiclient.discovery import build
import gcsfs
import json
import ast

from airflow.hooks.base_hook import BaseHook

def subdag(parent_dag_name, child_dag_name, args, json_gs):
    dag_subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        default_args=args,
        start_date=datetime.datetime(2021, 8, 5, 20, 0),
        schedule_interval='0 13,14,15,16,17,18,19,20,21,22,23,0,1 * * *',
    )

    connection_airflow_yas_sa_sii_de = BaseHook.get_connection('google_cloud_yas_sa_sii_de')
    service_account_yas_sa_sii_de = ast.literal_eval(connection_airflow_yas_sa_sii_de.extra_dejson["extra__google_cloud_platform__keyfile_dict"])

    with gcsfs.GCSFileSystem(project='yas-dev-sii-pid', token=service_account_yas_sa_sii_de).open(json_gs) as f:
            jd = json.load(f)

    # Variables para ejecucion desde JSON
    url_trn = jd['url_trn']

    # Datos de TRN
    job_name_hom = jd['job_name_hom']
    url_hom = jd['url_hom']
    file_name_hom = jd['file_name_hom']
    template_location_hom = jd['template_location_hom']

    # Datos Generales para la ejecucion
    temp_location = jd['temp_location']
    project = jd['project']
    region = jd['region']
    subnetwork = jd['subnetwork']
    service_account_email = jd['service_account_email']
    machine_type = jd['machine_type']
    max_num_workers = jd['max_num_workers']
    num_workers = jd['num_workers']

    folders = gcsfs.GCSFileSystem(project='yas-dev-sii-pid', token=service_account_yas_sa_sii_de).ls(url_trn)

    if len(folders)>0:
        for folder in folders:
            date_folder = folder.split('/')[3]

            if len(date_folder)>=10:
                url_source = 'gs://'+folder
                url_dest = url_hom+date_folder+'/'+file_name_hom

                parent_dag_name_for_id = parent_dag_name.lower()

                print('url_source: ' + url_source)
                print('url_dest: ' + url_dest)

                DataflowTemplateOperator(
                    template=template_location_hom,
                    job_name=f'{parent_dag_name_for_id}-{child_dag_name}-{date_folder}',
                    task_id=f'{parent_dag_name_for_id}-{child_dag_name}-{date_folder}',
                    location=region,
                    parameters={
                        'url_trn' : url_source,
                        'url_hom' : url_dest,
                    },
                    default_args=args,
                    dataflow_default_options={
                        'project' : project,
                        'zone' : 'us-east1-c',
                        'tempLocation' : temp_location,
                        'machineType' : machine_type,
                        'serviceAccountEmail': service_account_email,
                        'subnetwork' : subnetwork,  
                    },
                    gcp_conn_id='google_cloud_yas_sa_sii_de',
                    dag=dag_subdag,
                    )
    return dag_subdag

DAG_NAME = 'dag-sii-bch-ing-ab-hom-cue-mov'

args = {
    'owner': 'sii-dwh',
    'depends_on_past': False,
    # 'start_date': datetime.datetime(2021, 8, 2, 0, 0),
    'email': 'yas-dev-sii-pid-sa@yas-dev-sii-pid.iam.gserviceaccount.com',
    'email_on_failure': False,
    'email_on_retry': False,
    # 'schedule_interval': '@once',
    'retries': 0,
    'retry_delay': timedelta(minutes=30),
}
with DAG(
    dag_id=DAG_NAME, default_args=args, start_date=datetime.datetime(2021, 8, 5, 20, 0), schedule_interval='0 13,14,15,16,17,18,19,20,21,22,23,0,1 * * *', tags=['HOM','Movimientos','Cuentas']
) as dag:
    sensor_cuenta_trn = ExternalTaskSensor(
        task_id='sensor_trn_cuenta',
        external_dag_id='dag-sii-bch-ing-ab-trn-cue-mov',
        external_task_id='trn_cuenta',
    )
    sensor_movimientos_trn = ExternalTaskSensor(
        task_id='sensor_trn_movimientos',
        external_dag_id='dag-sii-bch-ing-ab-trn-cue-mov',
        external_task_id='trn_movimientos',
    )
    start = DummyOperator(
        task_id='start',
    )
    hom_cuenta = SubDagOperator(
        task_id='hom_cuenta',
        subdag=subdag(DAG_NAME, 'hom_cuenta', args, 'gs://yas-sii-int-des-dev/AB/config/PAR_SII_BCH_ELT_AB_TRN_HOM_CUENTA.json'),
    )
    hom_movimientos = SubDagOperator(
        task_id='hom_movimientos',
        subdag=subdag(DAG_NAME, 'hom_movimientos', args, 'gs://yas-sii-int-des-dev/AB/config/PAR_SII_BCH_ELT_AB_TRN_HOM_MOVIMIENTOS.json'),
    )
    end = DummyOperator(
        task_id='end',
    )

    start >> sensor_cuenta_trn >> hom_cuenta >> end
    start >> sensor_movimientos_trn >> hom_movimientos >> end

