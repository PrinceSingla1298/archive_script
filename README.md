#!/usr/bin/env python3
# coding: utf-8
## added dynamic sleep
# for TacobellUS_Prod_RewardsDataArchival.py with AWS Secrets Manager integration

import numpy as np
import boto3  # pip install boto3
from botocore.config import Config
import configparser
import os
import sys
import pandas as pd
from io import BytesIO
from io import BytesIO, StringIO
from pathlib import Path
import gzip
from datetime import datetime
from pandasql import sqldf
import base64
import signal
import re
from io import StringIO
from mysql.connector import Error
from mysql.connector import pooling
import mysql.connector
import time
import math
import json
import logging

#### ENV VARIABLES :
current_datetime = datetime.now()
get_data_export_dir = current_datetime.strftime("%Y-%m-%d")

# Global variables for monitoring
process_log_id = None
current_processing_file = None
current_processing_batch = None
process_start_time = None

# AWS Secrets Manager client
secrets_client = None
monitoring_connection_config = {}
db_credentials = {}

def setup_aws_clients(aws_profile='default'):
    """Setup AWS clients using configured profile"""
    global secrets_client
    try:
        session = boto3.Session(profile_name=aws_profile)
        secrets_client = session.client('secretsmanager')
        return session.client('s3')
    except Exception as e:
        print(f"Failed to setup AWS clients: {str(e)}")
        sys.exit(1)

def get_secret(secret_name):
    """Retrieve secret from AWS Secrets Manager"""
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        print(f"Failed to retrieve secret {secret_name}: {str(e)}")
        sys.exit(1)

def load_secrets_and_config(config_file):
    """Load configuration and secrets"""
    global monitoring_connection_config, db_credentials
    
    if not os.path.exists(config_file):
        print(f"Configuration file not found: {config_file}")
        sys.exit(1)
    
    config = configparser.ConfigParser()
    config.read(config_file)
    
    # Load database credentials from secrets manager
    db_secret_name = config.get('secrets', 'database_secret')
    db_credentials = get_secret(db_secret_name)
    
    # Load monitoring database config from secrets manager
    monitoring_secret_name = config.get('secrets', 'monitoring_db_secret')
    monitoring_connection_config = get_secret(monitoring_secret_name)
    
    return config


def execute_monitoring_query(query, params=None, fetch=False):
    """Execute query on monitoring database"""
    import numpy as np  # only inside function to avoid global dependency
    
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(
            host=monitoring_connection_config['host'],
            user=monitoring_connection_config['username'],
            password=monitoring_connection_config['password'],
            database=monitoring_connection_config['database'],
            port=monitoring_connection_config.get('port', 3306)
        )
        cursor = connection.cursor()

        # Fix: sanitize params so MySQL never sees numpy.int64
        if params:
            safe_params = []
            for p in params:
                if isinstance(p, (np.integer,)):
                    safe_params.append(int(p))
                else:
                    safe_params.append(p)
            cursor.execute(query, tuple(safe_params))
        else:
            cursor.execute(query)

        if fetch:
            return cursor.fetchall()

        connection.commit()
        return cursor.lastrowid if cursor.lastrowid else None

    except Exception as e:
        if connection:
            connection.rollback()
        print(f"Monitoring database error: {str(e)}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def create_monitoring_tables():
    """Create monitoring tables if they don't exist"""
    create_tables_sql = [
        """
        CREATE TABLE IF NOT EXISTS process_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            table_name VARCHAR(255) NOT NULL,
            process_type VARCHAR(100) NOT NULL,
            start_time DATETIME NOT NULL,
            end_time DATETIME NULL,
            status ENUM('RUNNING', 'COMPLETED', 'FAILED', 'PARTIAL') DEFAULT 'RUNNING',
            total_files INT DEFAULT 0,
            processed_files INT DEFAULT 0,
            current_file VARCHAR(500) NULL,
            current_batch INT NULL,
            replica_lag INT DEFAULT 0,
            hll_value INT DEFAULT 0,
            error_message TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS file_processing_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            process_log_id INT NOT NULL,
            file_name VARCHAR(500) NOT NULL,
            file_size BIGINT NOT NULL,
            start_time DATETIME NOT NULL,
            end_time DATETIME NULL,
            status ENUM('PROCESSING', 'COMPLETED', 'FAILED') DEFAULT 'PROCESSING',
            total_batches INT DEFAULT 0,
            processed_batches INT DEFAULT 0,
            error_message TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (process_log_id) REFERENCES process_logs(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS batch_processing_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            process_log_id INT NOT NULL,
            file_name VARCHAR(500) NOT NULL,
            batch_number INT NOT NULL,
            min_id BIGINT NOT NULL,
            max_id BIGINT NOT NULL,
            record_count INT NOT NULL,
            processing_time_seconds DECIMAL(10,3) NOT NULL,
            replica_lag INT DEFAULT 0,
            hll_value INT DEFAULT 0,
            sleep_time INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (process_log_id) REFERENCES process_logs(id)
        )
        """
    ]
    
    for sql in create_tables_sql:
        execute_monitoring_query(sql)

def create_process_log_entry(table_name, process_type='DATA_ARCHIVAL'):
    """Create initial process log entry"""
    global process_log_id, process_start_time
    
    query = """
    INSERT INTO process_logs (table_name, process_type, start_time, status)
    VALUES (%s, %s, %s, 'RUNNING')
    """
    
    process_start_time = datetime.now()
    process_log_id = execute_monitoring_query(query, (table_name, process_type, process_start_time))
    return process_log_id

def update_process_log(status, error_message=None, replica_lag=0, hll_value=0):
    """Update process log entry"""
    query = """
    UPDATE process_logs 
    SET end_time = %s, status = %s, error_message = %s, current_file = %s, 
        current_batch = %s, replica_lag = %s, hll_value = %s
    WHERE id = %s
    """
    
    params = (
        datetime.now(),
        status,
        error_message,
        current_processing_file,
        current_processing_batch,
        replica_lag,
        hll_value,
        process_log_id
    )
    
    execute_monitoring_query(query, params)

def log_batch_processing(file_name, batch_num, min_id, max_id, record_count, processing_time, replica_lag=0, hll_value=0, sleep_time=0):
    """Log batch processing details"""
    query = """
    INSERT INTO batch_processing_logs 
    (process_log_id, file_name, batch_number, min_id, max_id, record_count, 
     processing_time_seconds, replica_lag, hll_value, sleep_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    params = (process_log_id, file_name, batch_num, min_id, max_id, record_count, 
             processing_time, replica_lag, hll_value, sleep_time)
    execute_monitoring_query(query, params)

# Load configuration
if len(sys.argv) != 2:
    print("Usage: python script.py <config_file>")
    sys.exit(1)

config_file = sys.argv[1]

# First read the config file to get AWS profile
config = configparser.ConfigParser()
config.read(config_file)

# Setup AWS clients FIRST
s3 = setup_aws_clients(config.get('general', 'aws_profile', fallback='default'))

# NOW load the secrets
db_secret_name = config.get('secrets', 'database_secret')
db_credentials = get_secret(db_secret_name)

monitoring_secret_name = config.get('secrets', 'monitoring_db_secret') 
monitoring_connection_config = get_secret(monitoring_secret_name)

# Setup monitoring
create_monitoring_tables()
table_name = config.get('database', 'table_name')
create_process_log_entry(table_name)

# Get configuration values
bucket_name = config.get('s3', 'bucket_name')
file_path = config.get('s3', 'file_path').replace('{date}', get_data_export_dir)
log_directory_path = config.get('general', 'log_directory')

# Database configuration from config file and secrets
host = config.get('database', 'host')
user = db_credentials['username']
password = db_credentials['password']
database = config.get('database', 'database')

# Replica configurations
replica_hosts = []
if config.has_section('replica'):
    print("Replica section found in config")
    for key, value in config.items('replica'):
        if key.startswith('host'):
            replica_hosts.append(value)
            print(f"Added replica host: {value}")
else:
    print("No replica section found in config - will skip replica lag monitoring")

replica_Lag_all = []
HLL_all = []
DBHEALTH_INTERVAL = config.getint('database', 'health_check_interval', fallback=5)
DB_BatchSize = config.getint('database', 'batch_size', fallback=2200)

# SQL queries from config
reward_archival_insert_QL = config.get('database', 'archival_insert_query', fallback="""
INSERT INTO reward_archives(id ,business_id,redeemable_tag_id,user_id,redeemable_id,free_punchh_campaign_id,redemption_id,checkin_id,admin_id,from_balance,to_balance,created_at,updated_at,start_time,end_time,read_at,gaming_level_id,gifted_for_type,gift_reason,redeemable_location_id,event_type,campaign_type,app_device_id,recurring_reward_id,gifted_for_id,status) 
SELECT id ,business_id,redeemable_tag_id,user_id,redeemable_id,free_punchh_campaign_id,redemption_id,checkin_id,admin_id,from_balance,to_balance,created_at,updated_at,start_time,end_time,read_at,gaming_level_id,gifted_for_type,gift_reason,redeemable_location_id,event_type,campaign_type,app_device_id,recurring_reward_id,gifted_for_id,'perished' 
FROM rewards WHERE id IN ({}) ;
""")

delete_rewards_QL = config.get('database', 'delete_query', fallback="DELETE FROM rewards WHERE id IN ({}) ;")

datewithtime = datetime.today().strftime('%d_%m_%Y_%H_%M_%S')
datenotime = datetime.today().strftime('%d_%m_%Y')
logfileoutput = log_directory_path + '/DataProcessingAllDetails_' + datenotime + '.txt'
filedetails = log_directory_path + '/DataProcessingAllFilesDetails_' + datenotime + '.txt'
fileProcesseddetails = log_directory_path + '/DataProcessedFilesDetails_' + datenotime + '.txt'
fileIntProcdetails = log_directory_path + '/DataIntProcDetails_' + datenotime + '.txt'

### FUNCTIONS (keeping original functionality):
def run_sql_queries(host, user, password, database, queries, ifselect):
    connection = None
    cursor = None
    try:
        # Create a MySQL database connection
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        # Create a cursor object to interact with the database
        cursor = connection.cursor()

        # Execute each query one by one
        cursor.execute(queries)

        if ifselect == 1:
            select_results = cursor.fetchone()
            return select_results

        # Commit the changes (for INSERT, UPDATE, DELETE queries)
        connection.commit()

    except mysql.connector.Error as err:
        # Handle any errors that occur during query execution
        if connection is not None:
            connection.rollback()
        print("MySQL Error: {}".format(err))

    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()


def create_directory(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created.")
    else:
        print(f"Directory '{directory_path}' already exists.")

def Get_Required_Sleep_Time():
    global replica_Lag_all 
    global HLL_all
    # clear old values
    replica_Lag_all.clear()
    HLL_all.clear()

    # Check if replica hosts are configured
    if not replica_hosts:
        print("No replica hosts configured - skipping replica lag check")
    else:
        print(f"Checking {len(replica_hosts)} replica hosts for lag")
        # Check replica hosts
        for replica_host in replica_hosts:
            try:
                slave_status = "show replica status "
                slavest = run_sql_queries(replica_host, user, password, database, slave_status, 1)
                replication_lag_sec = slavest[32] if slavest and len(slavest) > 32 else 0
                replica_Lag_all.append(replication_lag_sec)
                print(f"Replica {replica_host} lag: {replication_lag_sec} seconds")
                
                hll_query = "select count from information_schema.INNODB_METRICS where name like '%trx_rseg_history_len%'; "
                get_current_hll = run_sql_queries(replica_host, user, password, database, hll_query, 1)
                if get_current_hll and len(get_current_hll) > 0:
                    HLL_all.append(get_current_hll[0])
                    print(f"Replica {replica_host} HLL: {get_current_hll[0]}")
            except Exception as e:
                print(f"Error checking replica {replica_host}: {str(e)}")
        
    # Check master HLL
    try:
        hll_query = "select count from information_schema.INNODB_METRICS where name like '%trx_rseg_history_len%'; "
        get_current_hll = run_sql_queries(host, user, password, database, hll_query, 1)
        if get_current_hll and len(get_current_hll) > 0:
            HLL_all.append(get_current_hll[0])
            print(f"Master {host} HLL: {get_current_hll[0]}")
    except Exception as e:
        print(f"Error checking master HLL: {str(e)}")

def print_elapsed_time(start_time, get_latest_updatetime):
    get_now = datetime.now()
    elapsed_time = get_latest_updatetime - start_time 
    elapsed_minutes = elapsed_time.total_seconds() / 60
    getprocessingtime_min = elapsed_minutes
    getprocessingtime_min = math.floor(getprocessingtime_min)
    return getprocessingtime_min

def get_sleep_time_replication(get_replication_lag):
    if get_replication_lag >= 3000:
        return 10
    elif get_replication_lag >= 900 and get_replication_lag < 3000:
        return 5
    elif get_replication_lag >= 100 and get_replication_lag < 900:
        return 3
    else:
        return 0

def get_sleep_time_hll(get_hll_value):
    if get_hll_value >= 90000:
        return 10
    elif get_hll_value >= 10000 and get_hll_value < 90000:
        return 5
    elif get_hll_value >= 1000 and get_hll_value < 10000:
        return 1
    else:
        return 0

def get_dynamic_sleep_values():
    global replica_Lag_all
    global HLL_all

    Get_Required_Sleep_Time()
    # get max values for sleep time:
    if not replica_Lag_all:
        print(f"no replica found")
        max_replicalag = 0
        print(f"replication lag: {max_replicalag}")
    else:    
        max_replicalag = max(replica_Lag_all)
        print(f"replication lag: {max_replicalag}")

    if not HLL_all:
        print(f"no hll found")
        max_hllvalue = 0
    else:    
        max_hllvalue = max(HLL_all)
        print(f"hll : {max_hllvalue}")
      
    # get sleep
    get_dynamic_sleep_final = []
    getsleep_repli = get_sleep_time_replication(max_replicalag)
    getsleep_hll = get_sleep_time_hll(max_hllvalue)
    get_dynamic_sleep_final.append(getsleep_repli)
    get_dynamic_sleep_final.append(getsleep_hll)
    max_sleep = max(get_dynamic_sleep_final)

    return max_sleep, max_replicalag, max_hllvalue

##### END OF FUNCTIONS

create_directory(log_directory_path)

startatewithtime = datetime.today().strftime('%d_%m_%Y_%H_%M_%S')   
startscr_output_text = "Script Starts at:" + startatewithtime

filenames = []
fileoutput_text = "File Details Captured at :" + datewithtime

# Open the file for writing (creates the file if it doesn't exist)
with open(logfileoutput, 'a') as file:
    # Write the output to the file
    file.write(startscr_output_text + '\n')

s3filelist = s3.list_objects_v2(Bucket=bucket_name, Prefix=file_path)


with open(logfileoutput, 'a') as file:
    # Write the output to the file
    file.write(fileoutput_text + '\n')

print(s3filelist)

#if 'Contents' not in s3filelist:
#    print(f"No files found in bucket '{bucket_name}' with prefix '{file_path}'")      
for item in s3filelist['Contents']:
    files = item['Key']    
    print(files)
    head, tail = os.path.split(files)

    with open(filedetails, 'a') as file:
        _, file_extension = os.path.splitext(files)
        
        if (file_extension.lower() == '.csv'):
            file.write(files + '\n')
            filenames.append(files)   
            
        else:
            pass

filenames2 = [filename for filename in filenames if filename.lower().endswith('.csv')]
filenames = filenames2

def processing_file(filename, batch_size):
    global current_processing_file, current_processing_batch
    current_processing_file = filename
    
    output_text = "Script Started at:" + datewithtime

    start_time = datetime.now()
    lastupdated_time = datetime.now()
    print(f"calculated start time: {start_time}")
    print(f"calculated last updated time: {lastupdated_time}")
    start_time_text_megg = f"calculated first time: {start_time}"
    update_time_text_megg = f"calculated last updated time: {lastupdated_time}"

    with open(logfileoutput, 'a') as file:
        # Write the output to the file
        file.write(output_text + '\n')
        file.write(start_time_text_megg + '\n')
        file.write(update_time_text_megg + '\n')

    s3_response = s3.get_object(Bucket=bucket_name, Key=filename)
    decompressed_data = s3_response['Body'].read()

    # Initialize a generator to read the CSV file in chunks
    chunks = pd.read_csv(BytesIO(decompressed_data), header=None, encoding='utf-8', chunksize=batch_size)

    # Process each batch
    for i, chunk in enumerate(chunks):
        batch_start_time = datetime.now()
        current_processing_batch = i + 1
        
        headers = ['id']
        # Assign the headers to the DataFrame
        chunk.columns = headers
        # Do your data processing here for each batch
        newlinec = "---------------"
        batchprocessingdetails = "Processing Batch:" + str(i + 1)
        
        #fileIntProcdetails. override on every file processing begins
        with open(fileIntProcdetails, 'w') as file:
            file.write('Processed File:' + filename + '\n')
            file.write(newlinec + '\n')
            file.write(batchprocessingdetails + '\n')
            
        buffer = StringIO()
        filebatch_minid = chunk['id'].min()
        filebatch_maxid = chunk['id'].max()
        record_count = len(chunk)

        chunk['id'].to_csv(buffer, index=False, header=False)
        comma_separated_values = buffer.getvalue()
        comma_separated_values = comma_separated_values.replace("\n", ",")

        ids_groupby = ' '
        lastc = comma_separated_values[len(comma_separated_values)-1]
        if lastc == ',':
            ids_groupby = comma_separated_values[:len(comma_separated_values)-1]
        else:
            lastcharacterst = "not removing last character"
        
        with open(logfileoutput, 'a') as file:
            file.write(newlinec + '\n')
        with open(fileIntProcdetails, 'a') as file:
            file.write(newlinec + '\n')            

        lastupdated_time = datetime.now()
        print(f"calculated last updated time: {lastupdated_time}")

        start_time_text_megg = f"saving start time: {start_time}"
        update_time_text_megg = f"calculated last updated time: {lastupdated_time}"

        with open(logfileoutput, 'a') as file:
            # Write the output to the file
            file.write(start_time_text_megg + '\n')
            file.write(update_time_text_megg + '\n')

        getprocesstime = print_elapsed_time(start_time, lastupdated_time)
        elapsed_time_text_megg = f"calculated elapsed time: {getprocesstime}"

        with open(logfileoutput, 'a') as file:
            # Write the output to the file
            file.write(elapsed_time_text_megg + '\n')

        print('test1')
        # Execute archival and delete queries
        if config.has_option('database', 'archival_insert_query'):
            run_sql_queries(host, user, password, database, reward_archival_insert_QL.format(ids_groupby), 0)
        
        run_sql_queries(host, user, password, database, delete_rewards_QL.format(ids_groupby), 0)

        # Calculate processing time
        processing_time = (datetime.now() - batch_start_time).total_seconds()
        print(processing_time)

        # Health check and sleep calculation
        sleep_time = 0
        max_replicalag = 0
        max_hllvalue = 0
        
        if getprocesstime >= DBHEALTH_INTERVAL:
            print(f"total processing time exceed {DBHEALTH_INTERVAL} min window!")

            getprocessedsleeptime, max_replicalag, max_hllvalue = get_dynamic_sleep_values()
            sleep_time = getprocessedsleeptime

            sleep_values_txt = f"Calculated Current Sleep: ,last updated time {lastupdated_time} ,start time: {start_time},Current Sleep: {getprocessedsleeptime} "

            # Open the file for writing (creates the file if it doesn't exist)
            with open(logfileoutput, 'a') as file:
                # Write the output to the file
                file.write(sleep_values_txt + '\n')

            time.sleep(getprocessedsleeptime) 

            update_start_time_txt2 = f"updating start time to fix db health check stats:"
            with open(logfileoutput, 'a') as file:
                # Write the output to the file
                file.write(update_start_time_txt2 + '\n')

            start_time = lastupdated_time
            lastupdated_time = datetime.now()

            start_time_text_megg = f"saving UPDATED start time: {start_time}"
            update_time_text_megg = f"calculated last updated time: {lastupdated_time}"

            with open(logfileoutput, 'a') as file:
                # Write the output to the file
                file.write(start_time_text_megg + '\n')
                file.write(update_time_text_megg + '\n')
        else:
            print(f"Using Previous Computed Sleep value, No Calculation.")

        # Log batch processing to monitoring database
        log_batch_processing(filename, i + 1, filebatch_minid, filebatch_maxid, 
                           record_count, processing_time, max_replicalag, max_hllvalue, sleep_time)
            
        with open(fileIntProcdetails, 'a') as file:
            file.write(str(filebatch_minid) + '\n')
            file.write(str(filebatch_maxid) + '\n')
        with open(fileIntProcdetails, 'a') as file:
            file.write(delete_rewards_QL.format(ids_groupby) + '\n')
            file.write(newlinec + '\n')

        time.sleep(1) 
        i = i + 1

# convert file content to a list
def file_to_list(filename):
    # Open the file in read mode
    with open(filename, 'r') as file:
        # Create an empty list to store the lines
        desired_list = []

        # Iterate over the lines of the file
        for line in file:
            # Remove the newline character at the end of the line
            line = line.strip()
            # Append the line to the list
            desired_list.append(line)
    return desired_list            

def batch_processing_file(filename, batch_size, batchno):
    global current_processing_file, current_processing_batch
    current_processing_file = filename
    
    output_text = "Script Started at:" + datewithtime
    start_time = datetime.now()
    lastupdated_time = datetime.now()
    print(f"calculated start time: {start_time}")
    print(f"calculated last updated time: {lastupdated_time}")
    start_time_text_megg = f"calculated first time: {start_time}"
    update_time_text_megg = f"calculated last updated time: {lastupdated_time}"
    with open(logfileoutput, 'a') as file:
        file.write(output_text + '\n')
        file.write(start_time_text_megg + '\n')
        file.write(update_time_text_megg + '\n')

    s3_response = s3.get_object(Bucket=bucket_name, Key=filename)
    decompressed_data = s3_response['Body'].read()

    # Initialize a generator to read the CSV file in chunks
    chunks = pd.read_csv(BytesIO(decompressed_data), header=None, encoding='utf-8', chunksize=batch_size)
    
    for i, chunk in enumerate(chunks):
        batch_start_time = datetime.now()
        current_processing_batch = i + 1
        
        headers = ['id']
        chunk.columns = headers

        newlinec = "---------------"
        batchprocessingdetails = "Processing Batch:" + str(i + 1)

        with open(logfileoutput, 'a') as file:
            file.write(newlinec + '\n')
            file.write(batchprocessingdetails + '\n')

        with open(fileIntProcdetails, 'w') as file:
            file.write('Processed File:' + filename + '\n')
            file.write(newlinec + '\n')
            file.write(batchprocessingdetails + '\n')

        getcurrbatch = i + 1
        batchno = int(batchno)

        if getcurrbatch >= batchno:
            buffer = StringIO()
            filebatch_minid = chunk['id'].min()
            filebatch_maxid = chunk['id'].max()
            record_count = len(chunk)
            print(f"Batch min ID: {filebatch_minid}, max ID: {filebatch_maxid}")
            # chunk.columns = headers
            # chunk = chunk.astype(str).replace("nan", "")  
            # chunk['id'] = chunk['id'].astype(int)

            chunk['id'].to_csv(buffer, index=False, header=False)
            comma_separated_values = buffer.getvalue().replace("\n", ",")
            ids_groupby = comma_separated_values.rstrip(',')

            lastupdated_time = datetime.now()
            print(f"calculated last updated time: {lastupdated_time}")

            with open(logfileoutput, 'a') as file:
                file.write(f"saving start time: {start_time}\n")
                file.write(f"calculated last updated time: {lastupdated_time}\n")

            getprocesstime = print_elapsed_time(start_time, lastupdated_time)

            # Execute queries
            if config.has_option('database', 'archival_insert_query'):
                run_sql_queries(host, user, password, database, reward_archival_insert_QL.format(ids_groupby), 0)
            
            run_sql_queries(host, user, password, database, delete_rewards_QL.format(ids_groupby), 0)

            # Calculate processing time
            processing_time = (datetime.now() - batch_start_time).total_seconds()
            
            # Health check
            sleep_time = 0
            max_replicalag = 0
            max_hllvalue = 0
            
            if getprocesstime >= DBHEALTH_INTERVAL:
                print(f"total processing time exceed {DBHEALTH_INTERVAL} min window!")
                getprocessedsleeptime, max_replicalag, max_hllvalue = get_dynamic_sleep_values()
                sleep_time = getprocessedsleeptime
                with open(logfileoutput, 'a') as file:
                    file.write(f"Calculated Current Sleep: {getprocessedsleeptime}\n")
                time.sleep(getprocessedsleeptime)
                start_time = lastupdated_time
                lastupdated_time = datetime.now()

            # Log batch processing
            log_batch_processing(filename, getcurrbatch, filebatch_minid, filebatch_maxid, 
                               record_count, processing_time, max_replicalag, max_hllvalue, sleep_time)

            with open(fileIntProcdetails, 'a') as file:
                file.write(str(filebatch_minid) + '\n')
                file.write(str(filebatch_maxid) + '\n')
                file.write(delete_rewards_QL.format(ids_groupby) + '\n')
                file.write(newlinec + '\n')

            time.sleep(1)

        else:
            print(f"Skipping batch {getcurrbatch} before target batch {batchno}")

    else:
        print("no more batch remaining to process")

        i = i + 1

def find_and_display_string_in_file(file_path, search_string):
    matching_lines = []  # To store lines containing the search string

    try:
        with open(file_path, 'r') as file:
            for line in file:
                if search_string in line:
                    matching_lines.append(line)  # Append matching line to the list
        return matching_lines  # Return list of matching lines
    except FileNotFoundError:
        return None  # File not found

#code for last error handling:

file_path = fileIntProcdetails  
search_string = 'FILE PROCESSED SUCCESSFUL' 

#check for file: fileIntProcdetails

if not os.path.exists(fileIntProcdetails):
    with open(fileIntProcdetails, 'w'):
        pass

if os.path.getsize(fileIntProcdetails) == 0:
    print("The file is empty.")
    # no action required.
else:
    print("The file is not empty.")
    # further processing file for remaining file id

    matching_lines = find_and_display_string_in_file(file_path, search_string)

    if matching_lines:
        print(f"'{search_string}' found in the file in the following lines:")
        for line in matching_lines:
            desiredfilename = line.split(':')[1]
    else:
        print(f"'{search_string}' not found in the file.")

        findfilename = 'Processed File:'
        findfilename = find_and_display_string_in_file(file_path, findfilename)
        for line in findfilename:
            desirefilename = line.split(':')[1]
            desirefilename = desirefilename.strip('\n')

        findbatch = 'Processing Batch:'
        findbatchno = find_and_display_string_in_file(file_path, findbatch)
        for line in findbatchno:
            desirebatchno = line.split(':')[1]
            batch_processing_file(desirefilename, DB_BatchSize, desirebatchno)

            EOProcessing = "FILE PROCESSED SUCCESSFUL.:" + desirefilename

            with open(fileProcesseddetails, 'a') as file:
                # Write the output to the file
                file.write(desirefilename + '\n')

            with open(logfileoutput, 'a') as file:
                file.write(EOProcessing + '\n')

            with open(fileIntProcdetails, 'a') as file:
                file.write(EOProcessing + '\n')

# saving 
all_list = file_to_list(filedetails)
all_list2 = [filename for filename in all_list if filename.lower().endswith('.csv')]
all_list = all_list2

# Check if the file exists
if not os.path.exists(fileProcesseddetails):
    with open(fileProcesseddetails, 'w'):
        print("File creation or existence check complete.")

processed_list = file_to_list(fileProcesseddetails)
processed_list2 = [filename for filename in processed_list if filename.lower().endswith('.csv')]
processed_list = processed_list2

working_list = [x for x in all_list if x not in processed_list]

with open(logfileoutput, 'a') as file:
    # Loop through the list and write each element to the file
    for filename in working_list:
        file.write(filename + '\n')

# Update total files count in monitoring
execute_monitoring_query(
    "UPDATE process_logs SET total_files = %s WHERE id = %s",
    (len(working_list), process_log_id)
)

processed_count = 0
failed_count = 0

for item in working_list:
    current_processing_file = item
    print(f"Processed File Name: {item}")

    try:
        filesize_response = s3.head_object(Bucket=bucket_name, Key=item)
        get_file_size = filesize_response['ContentLength']
        print(f"FileSize:{get_file_size}")

        if get_file_size > 0:
            CheckFileProcessing = "FILE has Rows ,Start Processing .:" + item
        
            with open(logfileoutput, 'a') as file:
                # Write the output to the file
                file.write(CheckFileProcessing + '\n')
    
            processing_file(item, DB_BatchSize)
            
            EOProcessing = "FILE PROCESSED SUCCESSFUL.:" + item
            
            with open(fileProcesseddetails, 'a') as file:
                # Write the output to the file
                file.write(item + '\n')
            
            with open(logfileoutput, 'a') as file:
                file.write(EOProcessing + '\n')
                
            with open(fileIntProcdetails, 'a') as file:
                file.write(EOProcessing + '\n')    

            processed_count += 1
            
        else:
            print(f"File {item} is empty, skipping")
            processed_count += 1

    except Exception as e:
        error_msg = f"Failed to process file {item}: {str(e)}"
        print(error_msg)
        with open(logfileoutput, 'a') as file:
            file.write(f"ERROR: {error_msg}\n")
        failed_count += 1

    # Update processed files count
    execute_monitoring_query(
        "UPDATE process_logs SET processed_files = %s WHERE id = %s",
        (processed_count, process_log_id)
    )

enddatewithtime = datetime.today().strftime('%d_%m_%Y_%H_%M_%S')    
endofscr_output_text = "Script Ends at:" + enddatewithtime

# Open the file for writing (creates the file if it doesn't exist)
with open(logfileoutput, 'a') as file:
    # Write the output to the file
    file.write(endofscr_output_text + '\n')

# Determine final status and update process log
if failed_count == 0:
    final_status = 'COMPLETED'
elif processed_count > 0:
    final_status = 'PARTIAL'
else:
    final_status = 'FAILED'

# Get final health metrics
try:
    final_sleep, final_replica_lag, final_hll = get_dynamic_sleep_values()
except:
    final_replica_lag, final_hll = 0, 0

update_process_log(final_status, None, final_replica_lag, final_hll)

print("PROCESS COMPLETED!")
print(f"Processed: {processed_count}, Failed: {failed_count}")