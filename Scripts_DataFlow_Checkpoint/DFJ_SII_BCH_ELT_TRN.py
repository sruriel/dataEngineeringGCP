import ast
import pyarrow
import pandas as pd
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.parquetio import WriteToParquet, ReadFromParquet
from apache_beam.pvalue import AsList

# Se manejan los parametros para la creacion y ejecucion de la plantilla
class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Estos parametros se usan para la ejecucion de la plantilla y no se deben indicar cuando se crea la plantilla
        parser.add_value_provider_argument("--url_raw", type=str)
        parser.add_value_provider_argument("--url_trn", type=str)
        # Estos parametros se usan para la creacion de la plantilla y corresponden a datos que quedan estaticos dentro de la misma
        parser.add_value_provider_argument("--rename_columns", type=str)
        parser.add_value_provider_argument("--schema_source", type=str)

pipeline_options = PipelineOptions()

with beam.Pipeline(options=pipeline_options) as p:
    print("Start Pipeline")
    user_options = pipeline_options.view_as(UserOptions)
    # Esta funcion renombra las "columnas" y regresa los datos renombrados
    def reColumns(row, rename_cols=None):
        for col in rename_cols:
            dict_rename = {value: row[key] for (key, value) in ast.literal_eval(col).items()}
        return dict_rename
    # Esta funcion recibe el parametro rename_columns y calcula un diccionario con las parejas de nombres de columnas
    def mapRenameCols(row, rename_cols=ast.literal_eval(user_options.rename_columns.get())):
        cols_before = list(row)
        cols_after = list(row)
        for (key, value) in rename_cols.items():
            index_col = cols_after.index(key)
            cols_after[index_col] = value
        map_rename_cols = dict(zip(cols_before,cols_after))
        return map_rename_cols
    # Esta funcion calcula el schema del parquet a escribir, aplicando el renombre de columnas al schema original
    def getSchema():
        df_schema  = pyarrow.Schema.from_pandas(pd.read_parquet(user_options.schema_source.get()))
        for (key, value) in ast.literal_eval(user_options.rename_columns.get()).items():
            df_schema = df_schema.set(df_schema.get_field_index(key),pyarrow.field(value,df_schema.types[df_schema.get_field_index(key)]))
        return df_schema
    # Este lee los archivos parquet fuente y calcula el diccionario con el mapeo de las columnas a renombrar 
    map_rename_cols = (p | "Read for rename cols" >> ReadFromParquet(user_options.url_raw)
                         | "Map rename cols" >> beam.Map(mapRenameCols)
                         | "Rename cols to string" >> beam.Map(str)
                         | "Deduplicate elements" >> beam.Distinct())
    # Este lee los datos desde los archivos fuente
    data = (p | "Read parquet for data" >> ReadFromParquet(user_options.url_raw))
    # Este aplica la funcion para renombarar las columnas y recibe el resultado del paso anterior como diccionario
    rename_data = (data | "Rename columns" >> beam.Map(reColumns, rename_cols=AsList(map_rename_cols) ))
    # Este escribe los datos en la ruta destino, obteniendo el schema desde la funcion getSchema
    _ = (rename_data | "Write to storage TRN" >> WriteToParquet(user_options.url_trn, schema=getSchema(), file_name_suffix=".parquet"))
    
print("End Pipeline")
