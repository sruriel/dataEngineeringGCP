import pyarrow
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.parquetio import WriteToParquet, ReadFromParquet
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import json
import gcsfs
import argparse
import subprocess
from io import StringIO
import ast

class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument("--url_hom", type=str)
        parser.add_value_provider_argument("--table_id", type=str)
        parser.add_value_provider_argument("--schema_source_bq", type=str)
        parser.add_value_provider_argument("--custom_bq_temp_loc", type=str)

pipeline_options = PipelineOptions()

with beam.Pipeline(options=pipeline_options) as p:
    print("Start Pipeline")
    user_options = pipeline_options.view_as(UserOptions)
    metadata_table_bq = subprocess.run(["bq","show","--format=prettyjson", user_options.schema_source_bq.get()], capture_output=True).stdout
    metadata_table_bq = ast.literal_eval(metadata_table_bq.decode('UTF-8'))
#     print(metadata_table_bq)
#     print(metadata_table_bq.keys())
    if "timePartitioning" in metadata_table_bq.keys() or "rangePartitioning" in metadata_table_bq.keys():
        if "timePartitioning" in metadata_table_bq.keys():
            print("time Partitioning")
            bq_param = metadata_table_bq["timePartitioning"]
#             bq_param = {"timePartitioning": {"type": metadata_table_bq["timePartitioning"]["type"],"field": metadata_table_bq["timePartitioning"]["field"]}}
        elif "rangePartitioning" in metadata_table_bq.keys():
            print("range Partitioning")
            bq_param2 = metadata_table_bq["rangePartitioning"]
            bq_param = {"rangePartitioning": {"field": metadata_table_bq["rangePartitioning"]["field"],"range": metadata_table_bq["rangePartitioning"]["range"]}}
        data = p | "Read from storage HOM" >> ReadFromParquet(user_options.url_hom)
        print(bq_param2)
        print(bq_param)
#         data | beam.Map(print)
        data | "Write to BQ HOM" >> WriteToBigQuery(
#                                             table=user_options.table_id,
                                            table='yas-dev-sii-pid:sii_yas_de_hom.hom_cuentas_test',
#                                             project=metadata_table_bq['tableReference']['projectId'],
                                            additional_bq_parameters=bq_param,
                                            schema=metadata_table_bq['schema'],
#                                             method='FILE_LOADS',
                                            create_disposition='CREATE_IF_NEEDED',
#                                             create_disposition='CREATE_NEVER',
                                            write_disposition='WRITE_TRUNCATE',
#                                             write_disposition='WRITE_APPEND',
#                                             custom_gcs_temp_location=user_options.custom_bq_temp_loc.get()
                                            )
#     else:
#         print("append Table")
#         data = p | "Read from storage HOM" >> ReadFromParquet(user_options.url_hom)
#         data | "Write to BQ HOM" >> WriteToBigQuery(
#                                             table=user_options.table_id,
#                                             schema=metadata_table_bq['schema'],
#                                             method='FILE_LOADS',
# #                                             create_disposition='CREATE_IF_NEEDED',
#                                             create_disposition='CREATE_NEVER',
#                                             write_disposition='WRITE_APPEND',
#                                             custom_gcs_temp_location=user_options.custom_bq_temp_loc.get()
#                                             )
    
print("End Pipeline")



            
