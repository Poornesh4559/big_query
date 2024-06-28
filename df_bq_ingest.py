import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToBigQuery
from apache_beam.io import filesystems
import logging
import yaml
import argparse
from datetime import datetime
import re
import ast


data = [
    {'name': 'Alice', 'age': 28, 'salary': 7000.25, 'is_active': True, 'created_at': '2023-07-01T08:15:30Z', 'join_date': '2023-07-01' },
    {'name': 'Bob', 'age': 32, 'salary': 8000.50, 'is_active': False, 'created_at': '2023-07-02T10:20:45+02:00', 'join_date': '2023-07-02'},
    {'name': 'Carol', 'age': 40, 'salary': None, 'is_active': True, 'created_at': '2023-07-03T12:30:50-04:00', 'join_date': '2023-07-03'}
]
data3 = {'data':[
    ['David', '22', '5500.75', 'false', '2023-07-04T14:45:55Z', '2023-07-04'],
    ['Eva', '29', '6700.80', 'true', '2023-07-05T16:50:00+09:00', '2023-07-05'],
    ['Frank', '35', '9100.60', 'false', '2023-07-06T18:55:05-07:00', '2023-07-06']
]}


class FormatToBqRow(beam.DoFn):
    def __init__(self,column_names):
        self.column_names = column_names

    def process(self,element):
        data = element['parsed_data']
        print('converting to bq format')
        dict_data = []
        for row in data:
            dict_row = {col_name: value for col_name, value in zip(self.column_names, row)}
            dict_data.append(dict_row)
            yield dict_row

class SchemaValidationException(Exception):
    pass
class SchemaValidation(beam.DoFn):
        def __init__(self, schema):
            self.schema = schema

        def process(self, element):
            data = element['data']
            try:
                parsed_data = self.parse_data(data)
                self.validate_data(parsed_data, self.schema)
                print("Data is valid according to the schema.")
                element['parsed_data'] = parsed_data
                yield element
            except SchemaValidationException as e:
                print(f"Schema validation error: {e}")

        def validate_data(self, data, schema):

            for row in data:
                #print("length of row and schema",len(row),len(schema))
                if len(row) != len(schema):
                    raise SchemaValidationException("Row length does not match schema length.")
                
                for value, field in zip(row, schema):
                    field_name = field['name']
                    field_type = field['type']
                    field_mode = field.get('mode', 'NULLABLE')
                    
                    if field_mode == 'REQUIRED' and value is None:
                        raise SchemaValidationException(f"Field '{field_name}' is required but is missing.")
                    
                    if value is not None:
                        if not self.validate_type(value, field_type):
                            raise SchemaValidationException(f"Field '{field_name}' with value '{value}' cannot be converted to type '{field_type}'.")

        def validate_type(self, value, field_type):

            try:
                if field_type in ['STRING', 'BYTES', 'JSON', 'GEOGRAPHY', 'INTERVAL']:
                    str(value)
                elif field_type in ['INT64', 'INTEGER', 'INT', 'SMALLINT', 'BIGINT', 'TINYINT', 'BYTEINT']:
                    int(value)
                elif field_type in ['NUMERIC', 'DECIMAL', 'BIGNUMERIC', 'BIGDECIMAL', 'FLOAT64']:
                    float(value)
                elif field_type == 'BOOL':
                    if str(value).lower() not in ['true', 'false', '1', '0']:
                        return False
                elif field_type == 'DATE':
                    datetime.strptime(value, '%Y-%m-%d')
                elif field_type == 'DATETIME':
                    datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                elif field_type == 'TIME':
                    datetime.strptime(value, '%H:%M:%S')
                elif field_type == 'TIMESTAMP':
                    try:
                        if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$", value):
                            datetime.fromisoformat(value.replace('Z', '+00:00'))
                        else:
                            datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f %Z')
                    except ValueError:
                        try:
                            datetime.strptime(value, '%Y-%m-%d %H:%M:%S %Z')
                        except ValueError:
                            try:
                                datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
                            except ValueError:
                                datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                else:
                    str(value)
                return True
            except (ValueError, TypeError):
                return False
            
        def parse_data(self,data):
            parsed_data = []
            for row in data:
                parsed_row = []
                for item in row:
                    try:
                        parsed_item = ast.literal_eval(item)
                    except (ValueError, SyntaxError):
                        parsed_item = item  # If literal_eval fails, keep it as a string
                    parsed_row.append(parsed_item)
                parsed_data.append(parsed_row)
            return parsed_data
def run_pipeline(table,argv=None):
    # Create an argument parser
    parser = argparse.ArgumentParser()
    
    # Parse the command-line arguments
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Create PipelineOptions using the parsed arguments
    pipeline_options = PipelineOptions(pipeline_args)
    

    schema = [
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "age", "type": "INT64", "mode": "REQUIRED"},
    {"name": "salary", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "is_active", "type": "BOOL", "mode": "REQUIRED"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "join_date", "type": "DATE", "mode": "REQUIRED"},
]
    
    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        column_names=[col['name'] for col in schema]
        (
        p
        | 'Read data' >> beam.Create([data3])
        | 'Validate data with schema' >> beam.ParDo(SchemaValidation(schema))
        | 'Convert to bq format' >> beam.ParDo(FormatToBqRow(column_names))
        | 'Filter out null values' >> beam.Filter(lambda row: row is not None)
        # | beam.Map(print)
        | 'Write to BigQuery' >> WriteToBigQuery(
            table=table,
            schema = {'fields':schema},
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
    )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    table = ['dataflow-d.dif_sink.diftest1','dataflow-d.dif_sink.diftest2','dataflow-d.dif_sink.test_partition']
    run_pipeline(table[2])
    # for i in range(len(table)):
    #     run_pipeline(table[i])
