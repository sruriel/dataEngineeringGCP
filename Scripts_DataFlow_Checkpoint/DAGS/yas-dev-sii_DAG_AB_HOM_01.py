from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.dates import days_ago

import datetime
from datetime import timedelta

from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.operators.python import PythonOperator

from googleapiclient.discovery import build
import gcsfs
import json
import ast

from airflow.hooks.base_hook import BaseHook

def subdag(parent_dag_name, child_dag_name, args, json_gs):
    dag_subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        default_args=args,
        start_date=days_ago(2),
        schedule_interval="@daily",
    )

    connection_airflow_yas_sa_sii_de = BaseHook.get_connection('google_cloud_yas_sa_sii_de')
    service_account_yas_sa_sii_de = ast.literal_eval(connection_airflow_yas_sa_sii_de.extra_dejson["extra__google_cloud_platform__keyfile_dict"])

    with gcsfs.GCSFileSystem(project='yas-dev-sii-pid', token=service_account_yas_sa_sii_de).open(json_gs) as f:
            jd = json.load(f)

    # Variables para ejecucion desde JSON

    # Datos de TRN
    url_trn = jd['url_trn']
    
    # Datos de HOM
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

DAG_NAME = 'dag-sii-bch-ing-ab-hom-TEST'

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
    dag_id=DAG_NAME, default_args=args, start_date=days_ago(2), schedule_interval="@once", tags=['HOM']
) as dag:
    start = DummyOperator(
        task_id='start',
    )
    hom_cuenta = SubDagOperator(
        task_id='hom_cuenta',
        subdag=subdag(DAG_NAME, 'hom_cuenta', args, 'gs://yas-sii-int-des-dev/AB/config/PAR_SII_BCH_ELT_AB_TRN_HOM_CUENTA.json'),
    )
    hom_estatus_cuenta = SubDagOperator(
        task_id='hom_estatus_cuenta',
        subdag=subdag(DAG_NAME, 'hom_estatus_cuenta', args, 'gs://yas-sii-int-des-dev/AB/config/PAR_SII_BCH_ELT_AB_TRN_HOM_ESTATUS_CUENTA.json'),
    )
    hom_movimientos = SubDagOperator(
        task_id='hom_movimientos',
        subdag=subdag(DAG_NAME, 'hom_movimientos', args, 'gs://yas-sii-int-des-dev/AB/config/PAR_SII_BCH_ELT_AB_TRN_HOM_MOVIMIENTOS.json'),
    )
    hom_servicios = SubDagOperator(
        task_id='hom_servicios',
        subdag=subdag(DAG_NAME, 'hom_servicios', args, 'gs://yas-sii-int-des-dev/AB/config/PAR_SII_BCH_ELT_AB_TRN_HOM_SERVICIOS.json'),
    )
    hom_servicios_cuenta = SubDagOperator(
        task_id='hom_servicios_cuenta',
        subdag=subdag(DAG_NAME, 'hom_servicios_cuenta', args, 'gs://yas-sii-int-des-dev/AB/config/PAR_SII_BCH_ELT_AB_TRN_HOM_SERVICIOS_CUENTA.json'),
    )
    hom_tipos_cuentas = SubDagOperator(
        task_id='hom_tipos_cuentas',
        subdag=subdag(DAG_NAME, 'hom_tipos_cuentas', args, 'gs://yas-sii-int-des-dev/AB/config/PAR_SII_BCH_ELT_AB_TRN_HOM_TIPOS_CUENTAS.json'),
    )
    hom_tipos_transacciones = SubDagOperator(
        task_id='hom_tipos_transacciones',
        subdag=subdag(DAG_NAME, 'hom_tipos_transacciones', args, 'gs://yas-sii-int-des-dev/AB/config/PAR_SII_BCH_ELT_AB_TRN_HOM_TIPOS_TRANSACCIONES.json'),
    )
    end = DummyOperator(
        task_id='end',
    )
    start >> hom_cuenta >> end
    start >> hom_estatus_cuenta >> end
    start >> hom_movimientos >> end
    start >> hom_servicios >> end
    start >> hom_servicios_cuenta >> end
    start >> hom_tipos_cuentas >> end
    start >> hom_tipos_transacciones >> end
