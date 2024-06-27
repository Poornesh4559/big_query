from datetime import datetime
import re
import ast

def parse_data(data):
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

def convert_to_dict_format(data, column_names):
    """
    Convert nested list data into list of dictionaries format.
    
    Args:
        data (list): Nested list where each sublist represents a row of data.
        column_names (list): List of column names corresponding to each sublist element.
        
    Returns:
        list: List of dictionaries where each dictionary represents a row of data with keys as column names.
    """
    dict_data = []
    for row in data:
        dict_row = {col_name: value for col_name, value in zip(column_names, row)}
        dict_data.append(dict_row)
    return dict_data


class SchemaValidationException(Exception):
    pass

def validate_data(data, schema):
    """
    Validate data against BigQuery schema.
    
    Parameters:
    data (list): The nested list data to validate.
    schema (list): The BigQuery table schema as a list of dictionaries.
    
    Raises:
    SchemaValidationException: If the data does not match the schema.
    """
    for row in data:
        if len(row) != len(schema):
            raise SchemaValidationException("Row length does not match schema length.")
        
        for value, field in zip(row, schema):
            field_name = field['name']
            field_type = field['type']
            field_mode = field.get('mode', 'NULLABLE')
            
            if field_mode == 'REQUIRED' and value is None:
                raise SchemaValidationException(f"Field '{field_name}' is required but is missing.")
            
            if value is not None:
                if not validate_type(value, field_type):
                    raise SchemaValidationException(f"Field '{field_name}' with value '{value}' cannot be converted to type '{field_type}'.")

def validate_type(value, field_type):
    """
    Validate if a value can be converted to a specific BigQuery field type.
    
    Parameters:
    value: The value to validate.
    field_type (str): The BigQuery field type.
    
    Returns:
    bool: True if the value can be converted to the field type, False otherwise.
    """
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
        # elif field_type == 'ARRAY':
        #     if not isinstance(value, list):
        #         return False
        #     for item in value:
        #         if not validate_type(item, 'STRING'):
        #             return False
        # elif field_type == 'STRUCT':
        #     if not isinstance(value, dict):
        #         return False
        # elif field_type == 'RANGE':
        #     if not isinstance(value, list) or len(value) != 2:
        #         return False
        #     for item in value:
        #         if not validate_type(item, 'DATE'):
        #             return False
        else:
            return False
        return True
    except (ValueError, TypeError):
        return False

# Example usage
data = [
    ["John", 30, 5000.50, "true", "2023-06-24T12:34:56Z", "2023-06-24", ["elem1", "elem2"], {"field1": "value1"}, ["2023-06-24", "2023-07-24"]],
    ["Jane", 25, 6000.75, "false", "2023-06-25T15:45:30+05:30", "2023-06-25", ["elem3", "elem4"], {"field2": "value2"}, ["2023-06-25", "2023-07-25"]]
]
data2 = [
    ['David', '22', '5500.75', 'false', '2023-07-04T14:45:55Z', '2023-07-04', "['item1', 'item2']", "{'fieldD': 'valueD'}", "['2023-07-04', '2023-08-04']"],
    ['Eva', '29', '6700.80', 'true', '2023-07-05T16:50:00+09:00', '2023-07-05', "['item3', 'item4']", "{'fieldE': 'valueE'}", "['2023-07-05', '2023-08-05']"],
    ['Frank', '35', '9100.60', 'false', '2023-07-06T18:55:05-07:00', '2023-07-06', "['item5']", "{'fieldF': 'valueF'}", "['2023-07-06', '2023-08-06']"]
]
data3 = [
    ['David', '22', '5500.75', 'false', '2023-07-04T14:45:55Z', '2023-07-04', ['item1', 'item2'], {'fieldD': 'valueD'}, ['2023-07-04', '2023-08-04']],
    ['Eva', '29', '6700.80', 'true', '2023-07-05T16:50:00+09:00', '2023-07-05', ['item3', 'item4'], {'fieldE': 'valueE'}, ['2023-07-05', '2023-08-05']],
    ['Frank', '35', '9100.60', 'false', '2023-07-06T18:55:05-07:00', '2023-07-06', ['item5'], {'fieldF': 'valueF'}, ['2023-07-06', '2023-08-06']]
]

data4 = [
    ["Jack", 39, 8500.00, "true", "2023-07-10 02:15:25 UTC", "2023-07-10", ["phi", "chi"], {"fieldJ": "valueJ"}, ["2023-07-10", "2023-08-10"]],
    ["Kara", 26, 9200.45, "false", "2023-07-11 01:22:55", "2023-07-11", ["psi"], {"fieldK": "valueK"}, ["2023-07-11", "2023-08-11"]],
    ["Leo", 33, 7800.65, "true", "2023-07-12T06:25:35-06:00", "2023-07-12", ["omega"], {"fieldL": "valueL"}, ["2023-07-12", "2023-08-12"]],
]

schema = [
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "age", "type": "INT64", "mode": "REQUIRED"},
    {"name": "salary", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "is_active", "type": "BOOL", "mode": "REQUIRED"},
    {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "join_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "tags", "type": "ARRAY", "mode": "NULLABLE"},
    {"name": "metadata", "type": "STRUCT", "mode": "NULLABLE"},
    {"name": "validity_period", "type": "RANGE", "mode": "NULLABLE"}
]
parsed_data1 = parse_data(data4)
print(parsed_data1)
try:
    validate_data(parsed_data1, schema)
    print("Data is valid according to the schema.")
except SchemaValidationException as e:
    print(f"Schema validation error: {e}")
