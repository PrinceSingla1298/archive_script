#!/usr/bin/env python3
# coding: utf-8
## Enhanced TacobellUS_Prod_RewardsDataArchival.py with batch-level resume functionality
## Fixed error logging, duplicate key handling, and comprehensive resume logic

import numpy as np
import boto3
from botocore.config import Config
import configparser
import os
import sys
import pandas as pd
from io import BytesIO, StringIO
from pathlib import Path
import gzip
from datetime import datetime
from pandasql import sqldf
import base64
import signal
import re
import mysql.connector
from mysql.connector import Error, pooling
import time
import math
import json
import logging

# Global variables for monitoring
process_log_id = None
current_processing_file = None
current_processing_batch = None
process_start_time = None
file_processing_log_id = None

# AWS and database clients
secrets_client = None
monitoring_connection_config = {}
db_credentials = {}

class ProcessingError(Exception):
    """Custom exception for processing errors"""
    pass

class DuplicateKeyError(ProcessingError):
    """Exception for duplicate key errors"""
    pass

def setup_aws_clients(aws_profile='default'):
    """Setup AWS clients using configured profile"""
    global secrets_client
    try:
        session = boto3.Session(profile_name=aws_profile)
        secrets_client = session.client('secretsmanager')
        return session.client('s3')
    except Exception as e:
        raise ProcessingError(f"Failed to setup AWS clients: {str(e)}")

def get_secret(secret_name):
    """Retrieve secret from AWS Secrets Manager"""
    if secrets_client is None:
        raise ProcessingError(f"AWS secrets client not initialized. Call setup_aws_clients() first.")
    
    try:
        print(f"Attempting to retrieve secret: {secret_name}")
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response['SecretString'])
        print(f"Successfully retrieved secret: {secret_name}")
        return secret_data
    except Exception as e:
        raise ProcessingError(f"Failed to retrieve secret {secret_name}: {str(e)}")

def load_configuration(config_file):
    """Load configuration file (without secrets)"""
    if not os.path.exists(config_file):
        raise ProcessingError(f"Configuration file not found: {config_file}")
    
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

def load_secrets(config):
    """Load secrets from AWS Secrets Manager after AWS client is initialized"""
    global monitoring_connection_config, db_credentials
    
    try:
        # Load database credentials from secrets manager
        db_secret_name = config.get('secrets', 'database_secret')
        print(f"Loading database secret: {db_secret_name}")
        db_credentials = get_secret(db_secret_name)
        
        # Load monitoring database config from secrets manager
        monitoring_secret_name = config.get('secrets', 'monitoring_db_secret')
        print(f"Loading monitoring database secret: {monitoring_secret_name}")
        monitoring_connection_config = get_secret(monitoring_secret_name)
        
        print("Successfully loaded both secrets")
        
    except Exception as e:
        raise ProcessingError(f"Failed to load secrets: {str(e)}")

def execute_monitoring_query(query, params=None, fetch=False):
    """Execute query on monitoring database with improved error handling"""
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

        # Sanitize numpy types for MySQL compatibility
        if params:
            safe_params = []
            for p in params:
                if isinstance(p, (np.integer,)):
                    safe_params.append(int(p))
                elif isinstance(p, (np.floating,)):
                    safe_params.append(float(p))
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
            last_processed_batch INT DEFAULT 0,
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
            FOREIGN KEY (process_log_id) REFERENCES process_logs(id),
            UNIQUE KEY unique_batch (process_log_id, file_name, batch_number)
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

def create_file_processing_log(file_name, file_size):
    """Create file processing log entry"""
    global file_processing_log_id
    
    query = """
    INSERT INTO file_processing_logs (process_log_id, file_name, file_size, start_time, status)
    VALUES (%s, %s, %s, %s, 'PROCESSING')
    """
    
    file_processing_log_id = execute_monitoring_query(
        query, (process_log_id, file_name, file_size, datetime.now())
    )
    return file_processing_log_id

def update_file_processing_log(status, error_message=None, total_batches=0, processed_batches=0, last_processed_batch=0):
    """Update file processing log entry"""
    query = """
    UPDATE file_processing_logs 
    SET end_time = %s, status = %s, error_message = %s, total_batches = %s, 
        processed_batches = %s, last_processed_batch = %s
    WHERE id = %s
    """
    
    execute_monitoring_query(query, (
        datetime.now(), status, error_message, total_batches, 
        processed_batches, last_processed_batch, file_processing_log_id
    ))

def update_process_log(status, error_message=None, replica_lag=0, hll_value=0):
    """Update process log entry with proper error logging"""
    query = """
    UPDATE process_logs 
    SET end_time = %s, status = %s, error_message = %s, current_file = %s, 
        current_batch = %s, replica_lag = %s, hll_value = %s
    WHERE id = %s
    """
    
    params = (
        datetime.now(), status, error_message, current_processing_file,
        current_processing_batch, replica_lag, hll_value, process_log_id
    )
    
    result = execute_monitoring_query(query, params)
    if result is None and error_message:
        print(f"WARNING: Failed to log error message to database: {error_message}")
    return result

def update_process_file_counts(total_files=None, processed_files=None):
    """Update file counts in process log"""
    if total_files is not None:
        execute_monitoring_query(
            "UPDATE process_logs SET total_files = %s WHERE id = %s",
            (total_files, process_log_id)
        )
    
    if processed_files is not None:
        execute_monitoring_query(
            "UPDATE process_logs SET processed_files = %s WHERE id = %s",
            (processed_files, process_log_id)
        )

def log_batch_processing(file_name, batch_num, min_id, max_id, record_count, processing_time, replica_lag=0, hll_value=0, sleep_time=0):
    """Log batch processing details with duplicate handling"""
    query = """
    INSERT INTO batch_processing_logs 
    (process_log_id, file_name, batch_number, min_id, max_id, record_count, 
     processing_time_seconds, replica_lag, hll_value, sleep_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    processing_time_seconds = VALUES(processing_time_seconds),
    replica_lag = VALUES(replica_lag),
    hll_value = VALUES(hll_value),
    sleep_time = VALUES(sleep_time)
    """
    
    params = (process_log_id, file_name, batch_num, min_id, max_id, record_count, 
             processing_time, replica_lag, hll_value, sleep_time)
    execute_monitoring_query(query, params)

def run_sql_queries(host, user, password, database, queries, fetch_one=False):
    """Execute SQL queries with improved error handling"""
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        cursor = connection.cursor()
        cursor.execute(queries)

        if fetch_one:
            return cursor.fetchone()

        connection.commit()
        return True

    except mysql.connector.Error as err:
        if connection:
            connection.rollback()
        error_msg = f"MySQL Error: {err}"
        print(error_msg)
        
        # Check for duplicate key error
        if "Duplicate entry" in str(err):
            raise DuplicateKeyError(error_msg)
        else:
            raise ProcessingError(error_msg)

    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

def get_health_metrics(host, user, password, database, replica_hosts):
    """Get database health metrics (replica lag and HLL)"""
    replica_lag_all = []
    hll_all = []

    # Check replica hosts if configured
    if replica_hosts:
        print(f"Checking {len(replica_hosts)} replica hosts for lag")
        for replica_host in replica_hosts:
            try:
                slave_status = "SHOW REPLICA STATUS"
                slavest = run_sql_queries(replica_host, user, password, database, slave_status, fetch_one=True)
                replication_lag_sec = slavest[32] if slavest and len(slavest) > 32 else 0
                replica_lag_all.append(replication_lag_sec)
                print(f"Replica {replica_host} lag: {replication_lag_sec} seconds")
                
                hll_query = "SELECT count FROM information_schema.INNODB_METRICS WHERE name LIKE '%trx_rseg_history_len%'"
                get_current_hll = run_sql_queries(replica_host, user, password, database, hll_query, fetch_one=True)
                if get_current_hll and len(get_current_hll) > 0:
                    hll_all.append(get_current_hll[0])
                    print(f"Replica {replica_host} HLL: {get_current_hll[0]}")
            except Exception as e:
                print(f"Error checking replica {replica_host}: {str(e)}")
    
    # Check master HLL
    try:
        hll_query = "SELECT count FROM information_schema.INNODB_METRICS WHERE name LIKE '%trx_rseg_history_len%'"
        get_current_hll = run_sql_queries(host, user, password, database, hll_query, fetch_one=True)
        if get_current_hll and len(get_current_hll) > 0:
            hll_all.append(get_current_hll[0])
            print(f"Master {host} HLL: {get_current_hll[0]}")
    except Exception as e:
        print(f"Error checking master HLL: {str(e)}")

    return replica_lag_all, hll_all

def calculate_sleep_time(replica_lag_all, hll_all):
    """Calculate dynamic sleep time based on health metrics"""
    max_replicalag = max(replica_lag_all) if replica_lag_all else 0
    max_hllvalue = max(hll_all) if hll_all else 0
    
    print(f"Max replication lag: {max_replicalag}")
    print(f"Max HLL: {max_hllvalue}")

    # Calculate sleep time based on replication lag
    if max_replicalag >= 3000:
        sleep_repli = 10
    elif max_replicalag >= 900:
        sleep_repli = 5
    elif max_replicalag >= 100:
        sleep_repli = 3
    else:
        sleep_repli = 0

    # Calculate sleep time based on HLL
    if max_hllvalue >= 90000:
        sleep_hll = 10
    elif max_hllvalue >= 10000:
        sleep_hll = 5
    elif max_hllvalue >= 1000:
        sleep_hll = 1
    else:
        sleep_hll = 0

    return max(sleep_repli, sleep_hll), max_replicalag, max_hllvalue

def create_directory(directory_path):
    """Create directory if it doesn't exist"""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created.")

def get_csv_files_from_s3(s3, bucket_name, file_path):
    """Get list of CSV files from S3"""
    try:
        s3filelist = s3.list_objects_v2(Bucket=bucket_name, Prefix=file_path)
        filenames = []
        
        if 'Contents' in s3filelist:
            for item in s3filelist['Contents']:
                file_key = item['Key']
                if file_key.lower().endswith('.csv'):
                    filenames.append(file_key)
        
        return filenames
    except Exception as e:
        raise ProcessingError(f"Failed to list S3 files: {str(e)}")

def get_last_processed_batch(filename):
    """Get the last successfully processed batch for a file from database"""
    query = """
    SELECT COALESCE(MAX(batch_number), 0) as last_batch
    FROM batch_processing_logs 
    WHERE process_log_id = %s AND file_name = %s
    """
    
    result = execute_monitoring_query(query, (process_log_id, filename), fetch=True)
    if result and len(result) > 0:
        return result[0][0]
    return 0

def write_batch_to_log_file(log_file, filename, batch_num, status="PROCESSING"):
    """Write batch processing info to log file for resume functionality"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] FILE: {filename} | BATCH: {batch_num} | STATUS: {status}\n"
    
    with open(log_file, 'a') as f:
        f.write(log_entry)

def get_last_batch_from_log_file(log_file, filename):
    """Get the last batch number from log file for a specific file"""
    if not os.path.exists(log_file):
        return 0
    
    last_batch = 0
    try:
        with open(log_file, 'r') as f:
            lines = f.readlines()
            
        # Read from bottom to top to find the last entry for this file
        for line in reversed(lines):
            if f"FILE: {filename}" in line and "STATUS: COMPLETED" in line:
                # Extract batch number
                batch_part = line.split("BATCH: ")[1].split(" |")[0]
                last_batch = int(batch_part)
                break
                
    except Exception as e:
        print(f"Error reading log file: {str(e)}")
        
    return last_batch

def process_file_batches(s3, bucket_name, filename, batch_size, config, db_config, replica_hosts, log_files):
    """Process file in batches with batch-level resume functionality"""
    global current_processing_file, current_processing_batch
    current_processing_file = filename
    
    try:
        # Get file size and create file processing log
        filesize_response = s3.head_object(Bucket=bucket_name, Key=filename)
        file_size = filesize_response['ContentLength']
        
        if file_size == 0:
            print(f"File {filename} is empty, skipping")
            return True
        
        create_file_processing_log(filename, file_size)
        
        # Get last processed batch (check both database and log file)
        last_batch_db = get_last_processed_batch(filename)
        last_batch_log = get_last_batch_from_log_file(log_files['main'], filename)
        start_batch = max(last_batch_db, last_batch_log)
        
        if start_batch > 0:
            print(f"Resuming file {filename} from batch {start_batch + 1}")
            with open(log_files['main'], 'a') as f:
                f.write(f"RESUMING: {filename} from batch {start_batch + 1}\n")
        else:
            print(f"Starting fresh processing for file: {filename}")
            with open(log_files['main'], 'a') as f:
                f.write(f"STARTING: {filename} (Size: {file_size} bytes)\n")

        # Get file content from S3
        s3_response = s3.get_object(Bucket=bucket_name, Key=filename)
        decompressed_data = s3_response['Body'].read()
        
        # Process in chunks
        chunks = pd.read_csv(BytesIO(decompressed_data), header=None, encoding='utf-8', chunksize=batch_size)
        
        start_time = datetime.now()
        total_batches = 0
        processed_batches = 0
        
        # Count total batches first
        temp_chunks = pd.read_csv(BytesIO(decompressed_data), header=None, encoding='utf-8', chunksize=batch_size)
        total_batches = sum(1 for _ in temp_chunks)
        
        # Process each batch
        for i, chunk in enumerate(chunks):
            current_processing_batch = i + 1
            
            # Skip already processed batches
            if current_processing_batch <= start_batch:
                print(f"Skipping already processed batch {current_processing_batch}")
                continue
                
            batch_start_time = datetime.now()
            
            try:
                # Write batch start to log file
                write_batch_to_log_file(log_files['main'], filename, current_processing_batch, "PROCESSING")
                
                # Prepare data
                chunk.columns = ['id']
                buffer = StringIO()
                chunk['id'].to_csv(buffer, index=False, header=False)
                comma_separated_values = buffer.getvalue().replace("\n", ",").rstrip(',')
                
                filebatch_minid = chunk['id'].min()
                filebatch_maxid = chunk['id'].max()
                record_count = len(chunk)
                
                print(f"Processing batch {current_processing_batch}/{total_batches} - IDs: {filebatch_minid} to {filebatch_maxid}")
                
                # Execute archival insert (if configured)
                if config.has_option('database', 'archival_insert_query'):
                    try:
                        archival_query = config.get('database', 'archival_insert_query').format(comma_separated_values)
                        run_sql_queries(db_config['host'], db_config['user'], db_config['password'], 
                                       db_config['database'], archival_query)
                        print(f"Archival insert completed for batch {current_processing_batch}")
                    except DuplicateKeyError as e:
                        print(f"Duplicate key in archival for batch {current_processing_batch}: {str(e)}")
                        # Log the duplicate key error but continue with deletion
                        with open(log_files['main'], 'a') as f:
                            f.write(f"DUPLICATE_KEY_ERROR: Batch {current_processing_batch} - {str(e)}\n")
                
                # Execute delete query
                delete_query = config.get('database', 'delete_query').format(comma_separated_values)
                run_sql_queries(db_config['host'], db_config['user'], db_config['password'], 
                               db_config['database'], delete_query)
                print(f"Delete completed for batch {current_processing_batch}")
                
                processed_batches += 1
                processing_time = (datetime.now() - batch_start_time).total_seconds()
                
                # Health check and sleep if needed
                sleep_time = 0
                max_replicalag = 0
                max_hllvalue = 0
                
                elapsed_minutes = (datetime.now() - start_time).total_seconds() / 60
                health_check_interval = config.getint('database', 'health_check_interval', fallback=5)
                
                if elapsed_minutes >= health_check_interval:
                    replica_lag_all, hll_all = get_health_metrics(
                        db_config['host'], db_config['user'], db_config['password'], 
                        db_config['database'], replica_hosts
                    )
                    sleep_time, max_replicalag, max_hllvalue = calculate_sleep_time(replica_lag_all, hll_all)
                    
                    if sleep_time > 0:
                        print(f"Sleeping for {sleep_time} seconds due to health metrics")
                        with open(log_files['main'], 'a') as f:
                            f.write(f"SLEEP: {sleep_time}s for batch {current_processing_batch}\n")
                        time.sleep(sleep_time)
                    
                    start_time = datetime.now()  # Reset timer
                
                # Log batch processing
                log_batch_processing(filename, current_processing_batch, filebatch_minid, 
                                   filebatch_maxid, record_count, processing_time, 
                                   max_replicalag, max_hllvalue, sleep_time)
                
                # Write batch completion to log file
                write_batch_to_log_file(log_files['main'], filename, current_processing_batch, "COMPLETED")
                
                time.sleep(1)  # Small delay between batches
                
            except DuplicateKeyError as e:
                # Handle duplicate key in archival gracefully
                error_msg = f"Duplicate key error in batch {current_processing_batch}: {str(e)}"
                print(error_msg)
                with open(log_files['main'], 'a') as f:
                    f.write(f"ERROR: {error_msg}\n")
                
                # Still mark batch as completed since we can continue
                write_batch_to_log_file(log_files['main'], filename, current_processing_batch, "COMPLETED_WITH_DUPLICATES")
                processed_batches += 1
                
            except Exception as e:
                error_msg = f"Error processing batch {current_processing_batch} in file {filename}: {str(e)}"
                print(error_msg)
                write_batch_to_log_file(log_files['main'], filename, current_processing_batch, "FAILED")
                update_file_processing_log('FAILED', error_msg, total_batches, processed_batches, current_processing_batch - 1)
                raise ProcessingError(error_msg)
        
        # Update file processing log as completed
        update_file_processing_log('COMPLETED', None, total_batches, processed_batches, current_processing_batch)
        
        with open(log_files['processed'], 'a') as f:
            f.write(f"{filename}\n")
        
        with open(log_files['main'], 'a') as f:
            f.write(f"FILE PROCESSED SUCCESSFUL: {filename} (Batches: {processed_batches}/{total_batches})\n")
        
        return True
        
    except Exception as e:
        error_msg = f"Failed to process file {filename}: {str(e)}"
        print(error_msg)
        update_file_processing_log('FAILED', error_msg)
        with open(log_files['main'], 'a') as f:
            f.write(f"ERROR: {error_msg}\n")
        raise ProcessingError(error_msg)

def main():
    """Main execution function"""
    global current_processing_file, current_processing_batch
    
    try:
        # Validate arguments
        if len(sys.argv) != 2:
            raise ProcessingError("Usage: python script.py <config_file>")
        
        config_file = sys.argv[1]
        
        # Load configuration file (without secrets)
        config = load_configuration(config_file)
        
        # Setup AWS clients FIRST
        s3 = setup_aws_clients(config.get('general', 'aws_profile', fallback='default'))
        
        # NOW load secrets after AWS client is initialized
        load_secrets(config)
        
        # Setup monitoring
        create_monitoring_tables()
        table_name = config.get('database', 'table_name')
        create_process_log_entry(table_name)
        
        # Get configuration values
        current_datetime = datetime.now()
        get_data_export_dir = current_datetime.strftime("%Y-%m-%d")
        
        bucket_name = config.get('s3', 'bucket_name')
        file_path = config.get('s3', 'file_path').replace('{date}', get_data_export_dir)
        log_directory_path = config.get('general', 'log_directory')
        
        # Database configuration
        db_config = {
            'host': config.get('database', 'host'),
            'user': db_credentials['username'],
            'password': db_credentials['password'],
            'database': config.get('database', 'database')
        }
        
        # Replica configurations
        replica_hosts = []
        if config.has_section('replica'):
            for key, value in config.items('replica'):
                if key.startswith('host'):
                    replica_hosts.append(value)
        
        batch_size = config.getint('database', 'batch_size', fallback=2200)
        
        # Create log directory
        create_directory(log_directory_path)
        
        # Setup log files
        datenotime = datetime.today().strftime('%d_%m_%Y')
        log_files = {
            'main': f"{log_directory_path}/DataProcessingAllDetails_{datenotime}.txt",
            'processed': f"{log_directory_path}/DataProcessedFilesDetails_{datenotime}.txt"
        }
        
        # Write start time
        start_time_str = datetime.today().strftime('%d_%m_%Y_%H_%M_%S')
        with open(log_files['main'], 'a') as f:
            f.write(f"=== Script Starts at: {start_time_str} ===\n")
        
        # Get files from S3
        all_files = get_csv_files_from_s3(s3, bucket_name, file_path)
        
        if not all_files:
            print(f"No CSV files found in bucket '{bucket_name}' with prefix '{file_path}'")
            update_process_log('COMPLETED', "No files to process")
            return
        
        # Get already processed files
        processed_files = []
        if os.path.exists(log_files['processed']):
            with open(log_files['processed'], 'r') as f:
                processed_files = [line.strip() for line in f if line.strip().endswith('.csv')]
        
        # Determine files to process
        files_to_process = [f for f in all_files if f not in processed_files]
        
        print(f"Total files: {len(all_files)}, Already processed: {len(processed_files)}, To process: {len(files_to_process)}")
        
        # Update total files count
        update_process_file_counts(total_files=len(files_to_process))
        
        if not files_to_process:
            print("All files already processed")
            update_process_log('COMPLETED', "All files already processed")
            return
        
        # Process files
        processed_count = 0
        failed_count = 0
        
        for filename in files_to_process:
            try:
                print(f"Processing file: {filename}")
                process_file_batches(s3, bucket_name, filename, batch_size, config, db_config, replica_hosts, log_files)
                processed_count += 1
                update_process_file_counts(processed_files=processed_count)
                
            except ProcessingError as e:
                print(f"Failed to process file {filename}: {str(e)}")
                failed_count += 1
                
                # Log the error properly to process_logs
                update_process_log('FAILED', f"File processing error: {str(e)}")
                
                # Don't continue processing if we have a critical error like missing table
                if "doesn't exist" in str(e).lower() or "table" in str(e).lower():
                    print("Critical error detected - stopping all processing")
                    raise e
        
        # Determine final status
        final_error_msg = None
        if failed_count == 0:
            final_status = 'COMPLETED'
        elif processed_count > 0:
            final_status = 'PARTIAL'
            final_error_msg = f"Processed {processed_count} files successfully, {failed_count} files failed"
        else:
            final_status = 'FAILED'
            final_error_msg = f"All {failed_count} files failed to process"
        
        # Get final health metrics
        try:
            replica_lag_all, hll_all = get_health_metrics(
                db_config['host'], db_config['user'], db_config['password'], 
                db_config['database'], replica_hosts
            )
            _, final_replica_lag, final_hll = calculate_sleep_time(replica_lag_all, hll_all)
        except:
            final_replica_lag, final_hll = 0, 0
        
        # Update process log with final status and any error message
        update_process_log(final_status, final_error_msg, final_replica_lag, final_hll)
        
        # Write end time
        end_time_str = datetime.today().strftime('%d_%m_%Y_%H_%M_%S')
        with open(log_files['main'], 'a') as f:
            f.write(f"=== Script Ends at: {end_time_str} ===\n")
            f.write(f"Final Status: {final_status}\n")
            f.write(f"Processed: {processed_count}, Failed: {failed_count}\n")
        
        print("PROCESS COMPLETED!")
        print(f"Final Status: {final_status}")
        print(f"Processed: {processed_count}, Failed: {failed_count}")
        
    except ProcessingError as e:
        error_msg = f"Processing error: {str(e)}"
        print(error_msg)
        update_process_log('FAILED', error_msg)
        sys.exit(1)
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(error_msg)
        update_process_log('FAILED', error_msg)
        sys.exit(1)

if __name__ == "__main__":
    main()