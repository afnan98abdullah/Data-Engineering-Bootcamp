import os
import boto3  # Consider using boto3 only if interacting with AWS services
import requests
import snowflake.connector as sf


def lambda_handler(event, context):
    """
    Downloads a file from a URL, uploads it to Snowflake, and loads it into a table.

    Args:
        event: The event object passed to the Lambda function.
        context: The context object passed to the Lambda function.

    Returns:
        A dictionary with a statusCode of 200 and a body message indicating success or failure.
    """

    url = 'https://de-materials-tpcds.s3.ca-central-1.amazonaws.com/inventory.csv'
    destination_folder = '/tmp'
    file_name = 'inventory.csv'
    local_file_path = os.path.join(destination_folder, file_name)

    snowflake_config = {
        'account': 'TCNREJL-IKB46695',
        'warehouse': 'COMPUTE_WH',
        'database': 'TPCDS',
        'schema': 'RAW',
        'table': 'INVENTORY',
        'user': 'TPCDS_user',
        'password': 'REPLACE_WITH_SECURE_PASSWORD',  # Replace with a secure password retrieval method
        'role': 'accountadmin',
        'stage_name': 'inv_Stage',
    }

    try:
        # Download data
        download_data(url, destination_folder, file_name)

        # Establish Snowflake connection
        conn = establish_snowflake_connection(snowflake_config)

        # Create Snowflake objects (stage and CSV format)
        create_snowflake_objects(conn, snowflake_config['stage_name'], snowflake_config['schema'])

        # Copy data to Snowflake
        copy_data_to_snowflake(conn, local_file_path, snowflake_config,file_name)
       
        return {
            'statusCode': 200,
            'body': 'File downloaded and uploaded to Snowflake successfully.'
        }
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': f"An error occurred while processing the file: {e}"
        }


def download_data(url, destination_folder, file_name):
    """
    Downloads a file from a URL and saves it to a local file.

    Args:
        url: The URL of the file to download.
        destination_folder: The folder where the file should be saved.
        file_name: The name of the file to save.
    """

    response = requests.get(url)
    response.raise_for_status()

    file_path = os.path.join(destination_folder, file_name)
    with open(file_path, 'wb') as file:
        file.write(response.content)

    # Optional: Print the contents of the downloaded file for debugging purposes
    # with open(file_path, 'r') as file:
    #     file_content = file.read()
    #     print("File Content:")
    #     print(file_content)
    


def establish_snowflake_connection(snowflake_config):
    """
    Establishes a connection to Snowflake using the provided configuration.

    Args:
        snowflake_config: A dictionary containing Snowflake connection parameters.

    Returns:
        A Snowflake connection object.
    """

    conn = sf.connect(**snowflake_config)
    return conn


def create_snowflake_objects(conn, stage_name, schema):
    """
    Creates a Snowflake stage and CSV format for loading data.

    Args:
        conn: The Snowflake connection object.
        stage_name: The name of the stage to create.
        schema: The schema where the stage will be created.
    """

    cursor = conn.cursor()

    # Use the specified schema
    use_schema_query = f"USE SCHEMA {schema};"
    cursor.execute(use_schema_query)

    # Create a CSV format (consider adding error handling)
    create_csv_format_query = f"CREATE OR REPLACE FILE FORMAT COMMA_CSV TYPE = 'CSV' FIELD_DELIMITER = ',';"
    cursor.execute(create_csv_format_query)

    # Create the stage (consider adding error handling)
    create_stage_query = f"CREATE OR REPLACE STAGE {stage_name} FILE_FORMAT =COMMA_CSV"
    cursor.execute(create_stage_query)

    conn.commit()


def copy_data_to_snowflake(conn, local_file_path, snowflake_config,file_name):
    """
    Copies data from a local file to Snowflake stage and then loads it into a table.

    Args:
        conn: The Snowflake connection object.
        local_file_path: The path to the local file.
        snowflake_config: A dictionary containing Snowflake configuration.
        file_name: The filename of the data file.
    """

    cursor = conn.cursor()

    # Copy the file from local to the stage
    copy_into_stage_query = f"PUT 'file://{local_file_path}' @{snowflake_config['stage_name']}"
    cursor.execute(copy_into_stage_query)

    # Truncate table (consider adding logic to avoid unnecessary truncates)
    truncate_table = f"TRUNCATE TABLE {snowflake_config['schema']}.{snowflake_config['table']};"
    cursor.execute(truncate_table)

    # Load the data from the stage into a table
    copy_into_query = f"COPY INTO {snowflake_config['schema']}.{snowflake_config['table']} FROM @{snowflake_config['stage_name']}/{file_name} FILE_FORMAT =COMMA_CSV on_error= 'continue';"
    cursor.execute(copy_into_query)

    conn.commit()
   