from googleapiclient.discovery import build
from google.cloud import storage
import gcsfs
import sys
import json
import datetime

# Recibe un JSON con los parametros
json_gs = sys.argv[2]
exe_param = sys.argv[1]

with gcsfs.GCSFileSystem().open(json_gs) as f:
            jd = json.load(f)

# Datos de RAW
url_raw = jd['url_raw']

# Datos de TRN
job_name_trn = jd['job_name_trn']
url_trn = jd['url_trn']
file_name_trn = jd['file_name_trn']
template_location_trn = jd['template_location_trn']

# Datos de HOM
job_name_hom = jd['job_name_hom']
url_hom = jd['url_hom']
file_name_hom = jd['file_name_hom']
template_location_hom = jd['template_location_hom']

# Datos de BQ
job_name_bq = jd['job_name_bq']
url_hom = jd['url_hom']
table_id = jd['table_id']
template_location_bq = jd['template_location_bq']

# Datos Generales para la ejecucion
temp_location = jd['temp_location']
project = jd['project']
region = jd['region']
subnetwork = jd['subnetwork']
service_account_email = jd['service_account_email']
machine_type = jd['machine_type']
max_num_workers = jd['max_num_workers']
num_workers = jd['num_workers']

### CAPA TRN ###
# Trata de leer la fecha de ayer en en origen RAW, calculada a partir de la fecha del sistema
if exe_param.lower()=='trn-delta':
#     Se captura la fecha a procesar
    date_execute = str(datetime.date.today()- datetime.timedelta(days=1))
#     Origen y Destino de la Operacion
    url_source = url_raw+date_execute
    url_dest = url_trn+date_execute+'/'+file_name_trn
    print('url_source: ' + url_source)
    print('url_dest: ' + url_dest)
    parameters = {
        'url_raw': url_source,
        'url_trn': url_dest,
    }
    environment_params = {
        'machineType': machine_type,
        'maxWorkers': max_num_workers,
        'numWorkers': num_workers,
        'serviceAccountEmail': service_account_email,
        'subnetwork': subnetwork,
        'tempLocation': temp_location,
        'workerRegion': region,
    }
    #request = dataflow.projects().templates().launch(
    dataflow = build('dataflow', 'v1b3')
    request = dataflow.projects().locations().templates().launch(
        projectId=project,
        gcsPath=template_location_trn,
        location=region,
        body={
            'jobName': job_name_trn+'-'+date_execute,
            'parameters': parameters,
            'environment': environment_params
        }
    )
    response = request.execute()
    print(response)
# itera por todas las fechas que encuentre en el origen RAW
elif exe_param.lower()=='trn-full':
    gs_raw_split = url_raw.split('/')
    bucket_name = gs_raw_split[2]
    prefix = gs_raw_split[3]+'/'+gs_raw_split[4]+'/'
    blobs = storage.Client().list_blobs(bucket_name, prefix=prefix, delimiter='/')
    blob_content = list(blobs)
    folders = list(blobs.prefixes)
    if len(folders)>0:
        for folder in folders:
            # Origen y Destino de la Operacion
            url_source = 'gs://'+bucket_name+'/'+folder
            url_dest = url_trn+folder.split('/')[2]+'/'+file_name_trn
            print('url_source: ' + url_source)
            print('url_dest: ' + url_dest)
            parameters = {
                'url_raw': url_source,
                'url_trn': url_dest,
            }
            environment_params = {
                'machineType': machine_type,
                'maxWorkers': max_num_workers,
                'numWorkers': num_workers,
                'serviceAccountEmail': service_account_email,
                'subnetwork': subnetwork,
                'tempLocation': temp_location,
                'workerRegion': region,
            }
            #request = dataflow.projects().templates().launch(
            dataflow = build('dataflow', 'v1b3')
            request = dataflow.projects().locations().templates().launch(
                projectId=project,
                gcsPath=template_location_trn,
                location=region,
                body={
                    'jobName': job_name_trn+'-'+folder.split('/')[2],
                    'parameters': parameters,
                    'environment': environment_params
                }
            )
            response = request.execute()
            print(response)

### CAPA HOM ###
# Trata de leer la fecha de ayer en en origen TRN, calculada a partir de la fecha del sistema
elif exe_param.lower()=='hom-delta':
#     Se captura la fecha a procesar
    date_execute = str(datetime.date.today()- datetime.timedelta(days=1))
#     Origen y Destino de la Operacion
    url_source = url_trn+date_execute
    url_dest = url_hom+date_execute+'/'+file_name_hom
    print('url_source: ' + url_source)
    print('url_dest: ' + url_dest)
    parameters = {
        'url_trn': url_source,
        'url_hom': url_dest,
    }
    environment_params = {
        'machineType': machine_type,
        'maxWorkers': max_num_workers,
        'numWorkers': num_workers,
        'serviceAccountEmail': service_account_email,
        'subnetwork': subnetwork,
        'tempLocation': temp_location,
        'workerRegion': region,
    }
    #request = dataflow.projects().templates().launch(
    dataflow = build('dataflow', 'v1b3')
    request = dataflow.projects().locations().templates().launch(
        projectId=project,
        gcsPath=template_location_hom,
        location=region,
        body={
            'jobName': job_name_hom+'-'+date_execute,
            'parameters': parameters,
            'environment': environment_params
        }
    )
    response = request.execute()
    print(response)
# itera por todas las fechas que encuentre en el origen TRN
elif exe_param.lower()=='hom-full':
    gs_trn_split = url_trn.split('/')
    bucket_name = gs_trn_split[2]
    prefix = gs_trn_split[3]+'/'+gs_trn_split[4]+'/'
    blobs = storage.Client().list_blobs(bucket_name, prefix=prefix, delimiter='/')
    blob_content = list(blobs)
    folders = list(blobs.prefixes)
    if len(folders)>0:
        for folder in folders:
            # Origen y Destino de la Operacion
            url_source = 'gs://'+bucket_name+'/'+folder
            url_dest = url_hom+folder.split('/')[2]+'/'+file_name_hom
            print('url_source: ' + url_source)
            print('url_dest: ' + url_dest)
            parameters = {
                'url_trn': url_source,
                'url_hom': url_dest,
            }
            environment_params = {
                'machineType': machine_type,
                'maxWorkers': max_num_workers,
                'numWorkers': num_workers,
                'serviceAccountEmail': service_account_email,
                'subnetwork': subnetwork,
                'tempLocation': temp_location,
                'workerRegion': region,
            }
            #request = dataflow.projects().templates().launch(
            dataflow = build('dataflow', 'v1b3')
            request = dataflow.projects().locations().templates().launch(
                projectId=project,
                gcsPath=template_location_hom,
                location=region,
                body={
                    'jobName': job_name_hom+'-'+folder.split('/')[2],
                    'parameters': parameters,
                    'environment': environment_params
                }
            )
            response = request.execute()
            print(response)
            
### CAPA BQ ###
# Trata de leer la fecha de ayer en en origen HOM, calculada a partir de la fecha del sistema
elif exe_param.lower()=='bq-delta':
#     Se captura la fecha a procesar
    date_execute = str(datetime.date.today()- datetime.timedelta(days=1))
#     Origen y Destino de la Operacion
    url_source = url_hom+date_execute
#     table_id = table_id
    print('url_source: ' + url_source)
    print('table_id: ' + table_id)
    parameters = {
        'url_hom': url_source,
        'table_id': table_id,
    }
    environment_params = {
        'machineType': machine_type,
        'maxWorkers': max_num_workers,
        'numWorkers': num_workers,
        'serviceAccountEmail': service_account_email,
        'subnetwork': subnetwork,
        'tempLocation': temp_location,
        'workerRegion': region,
    }
    #request = dataflow.projects().templates().launch(
    dataflow = build('dataflow', 'v1b3')
    request = dataflow.projects().locations().templates().launch(
        projectId=project,
        gcsPath=template_location_bq,
        location=region,
        body={
            'jobName': job_name_bq+'-'+date_execute,
            'parameters': parameters,
            'environment': environment_params
        }
    )
    response = request.execute()
    print(response)
# itera por todas las fechas que encuentre en el origen HOM
elif exe_param.lower()=='bq-full':
    gs_hom_split = url_hom.split('/')
    bucket_name = gs_hom_split[2]
    prefix = gs_hom_split[3]+'/'+gs_hom_split[4]+'/'
    blobs = storage.Client().list_blobs(bucket_name, prefix=prefix, delimiter='/')
    blob_content = list(blobs)
    folders = list(blobs.prefixes)
    if len(folders)>0:
        for folder in folders:
            # Origen y Destino de la Operacion
            url_source = 'gs://'+bucket_name+'/'+folder
#             url_dest = url_hom+folder.split('/')[2]+'/'+file_name_hom
            print('url_source: ' + url_source)
            print('table_id: ' + table_id)
            parameters = {
                'url_hom': url_source,
                'table_id': table_id,
            }
            environment_params = {
                'machineType': machine_type,
                'maxWorkers': max_num_workers,
                'numWorkers': num_workers,
                'serviceAccountEmail': service_account_email,
                'subnetwork': subnetwork,
                'tempLocation': temp_location,
                'workerRegion': region,
            }
            #request = dataflow.projects().templates().launch(
            dataflow = build('dataflow', 'v1b3')
            request = dataflow.projects().locations().templates().launch(
                projectId=project,
                gcsPath=template_location_bq,
                location=region,
                body={
                    'jobName': job_name_bq+'-'+folder.split('/')[2],
                    'parameters': parameters,
                    'environment': environment_params
                }
            )
            response = request.execute()
            print(response)
else:
    print('Parametro no reconocido')