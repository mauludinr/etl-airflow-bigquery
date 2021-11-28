import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv
from apache_beam.dataframe import convert
import os
from beam_mysql.connector import splitters
from beam_mysql.connector.io import ReadFromMySQL
from apache_beam.options import pipeline_options

def run(argv=None):

  parser = argparse.ArgumentParser()

  parser.add_argument(
        '--dataset_id', dest='dataset_id', required=True,
        help='Insert Bigquery Dataset')
  parser.add_argument('--temp_location', dest='gcs_temp_location', required=True,
                        help='GCS Temp Directory to store Temp data before write to BQ')
  parser.add_argument('--staging_location', dest='gcs_stg_location', required=True,
                        help='GCS Staging Directory to store staging data before write to BQ')
  parser.add_argument('--project', dest='project_id', required=True,
                        help='Project ID which GCP linked on.')

  known_args, pipeline_args = parser.parse_known_args(argv)

  p_options = pipeline_options.PipelineOptions(
                pipeline_args,
                temp_location=known_args.gcs_temp_location,
                staging_location=known_args.gcs_stg_location,
                project=known_args.project_id)

  schema_table = [
       'account_id:INTEGER,district_id:INTEGER,frequency:STRING,date:DATE',
       'card_id:INTEGER,disp_id:INTEGER,type:STRING,issued:STRING',
       'client_id:INTEGER,gender:STRING,birth_date:DATE,district_id:INTEGER',
       'disp_id:INTEGER,client_id:INTEGER,account_id:INTEGER,type:STRING',
       'district_id:INTEGER,A2:STRING,A3:STRING,A4:INTEGER,A5:INTEGER,A6:INTEGER,A7:INTEGER,A8:INTEGER,A9:INTEGER,A10:FLOAT,A11:INTEGER,A12:FLOAT,A13:FLOAT,A14:INTEGER,<NTEGER,A15:FLOAT,A16:INTEGER',   
       'order_id:INTEGER,account_id:INTEGER,bank_to:STRING,account_to:INTEGER,amount:FLOAT,k_symbol:STRING',
       'trans_id:INTEGER,account_id:INTEGER,date:DATE,type:STRING,operation:STRING,amount:INTEGER,balance:INTEGER,k_symbol:STRING,bank:STRING,account:FLOAT'
      ]

  kolom = ['account','card','client','disp','district','loan','order','trans']

  with beam.Pipeline(options=PipelineOptions()) as pipeline:
    for i in range(len(kolom)):
      (
        pipeline
        | f'Read SQL{i+1}' >> ReadFromMySQL(host="relational.fit.cvut.cz",
                         port='3306',
                         user='guest',
                         password='relational',
                         database='financial',
                         query = f"""SELECT * FROM `{kolom[i]}`;""",
                         splitter=splitters.NoSplitter()
            )
        | f"Write data to BigQuery{i+1}" >> beam.io.WriteToBigQuery(
           table= f'{known_args.project_id}:{known_args.dataset_id}.{kolom[i]}',
           schema=schema_table[i],
                 custom_gcs_temp_location=known_args.gcs_temp_location,
                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
           write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
          )
       )

if __name__ == '__main__':
    run()
    
