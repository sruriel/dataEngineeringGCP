import gcsfs
import sys
import json
import datetime
import os

# Recibe un JSON con los parametros
json_gs = sys.argv[2]
exe_param = sys.argv[1]

with gcsfs.GCSFileSystem().open(json_gs) as f:
            jd = json.load(f)

# Datos de TRN
code_file_trn = jd['code_file_trn']
template_location_trn = jd['template_location_trn']
schema_source = jd['schema_source']
rename_columns = jd['rename_columns']

# Datos de HOM
code_file_hom = jd['code_file_hom']
template_location_hom = jd['template_location_hom']
schema_source_hom = jd['schema_source_hom']

# Datos de BQ
code_file_bq = jd['code_file_bq']
template_location_bq = jd['template_location_bq']
schema_source_bq = jd['schema_source_bq']
custom_bq_temp_loc = jd['temp_location']

# Datos generales 
temp_location = jd['temp_location']
staging_location = jd['staging_location']
setup_file = jd['setup_file']
project = jd['project']
runner = jd['runner']
region = jd['region']

try:
    if exe_param.lower()=='trn':
        comand_string = 'python3 '+str(code_file_trn)+' --setup_file '+str(setup_file)+' --project '+str(project)+' --save_main_session --runner '+str(runner)+' --region '+str(region)+' --temp_location '+str(temp_location)+' --staging_location '+str(staging_location)+' --template_location '+str(template_location_trn)+' --schema_source '+str(schema_source)+' --rename_columns '+'"{'+str(rename_columns)+'}"'
        os.system(comand_string)
        
    elif exe_param.lower()=='hom':
        comand_string = 'python3 '+str(code_file_hom)+' --setup_file '+str(setup_file)+' --project '+str(project)+' --save_main_session --runner '+str(runner)+' --region '+str(region)+' --temp_location '+str(temp_location)+' --staging_location '+str(staging_location)+' --template_location '+str(template_location_hom)+' --schema_source_hom '+str(schema_source_hom)
        os.system(comand_string)
        
    elif exe_param.lower()=='bq':
        comand_string = 'python3 '+str(code_file_bq)+' --setup_file '+str(setup_file)+' --project '+str(project)+' --save_main_session --runner '+str(runner)+' --region '+str(region)+' --temp_location '+str(temp_location)+' --staging_location '+str(staging_location)+' --template_location '+str(template_location_bq)+' --schema_source_bq '+str(schema_source_bq)+' --custom_bq_temp_loc '+str(custom_bq_temp_loc)
        os.system(comand_string)
    else:
        print('Parametro invalido, solo admite trn, hom y bq')
except Exception as e:
    print(e)
    print('Comando ejecutado: '+str(comand_string))
    print('¡Error creando template!')
print('¡Template Creado con Exito!')
    





