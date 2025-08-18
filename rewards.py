import os
import os.path
import streamlit as st
from datetime import datetime
import sys
import configparser
import re
import mysql.connector
from mysql.connector import Error
import boto3
import json
import signal

#generic_archivaljob_status_template.py


def default():
    #enable_display_button=0
    ###ENV
    archivaljob_stack = "Mothership Rewards"

    # Read new config file
    config_file_path = "/opt/Script/prince_test_archive_script/rewards.ini"
    config = configparser.ConfigParser()
    if not os.path.exists(config_file_path):
        st.title("CONFIG FILE MISSING")
        st.write(config_file_path)
        sys.exit(1)
    config.read(config_file_path)

    # General settings
    aws_profile_raw = config.get('general', 'aws_profile', fallback='').strip()
    aws_profile = aws_profile_raw.strip().strip('[]').strip('"').strip("'")

    # Keep original variable name used throughout the file
    log_direcotry = config.get('general', 'log_directory', fallback='/opt/Logs/DataProcessed/prod/mothership/rewards/')

    # Database and secrets
    database_secret_name = config.get('secrets', 'database_secret', fallback='').strip()
    if not database_secret_name:
        st.title('CONFIG ERROR: database_secret missing in [secrets]')
        sys.exit(1)

    db_host = config.get('database', 'host', fallback='').strip()
    database = config.get('database', 'database', fallback='').strip()
    if not db_host or not database:
        st.title('CONFIG ERROR: host or database missing in [database]')
        sys.exit(1)

    # Fetch DB credentials from AWS Secrets Manager
    try:
        if aws_profile:
            session = boto3.session.Session(profile_name=aws_profile)
        else:
            session = boto3.session.Session()
        secrets_client = session.client('secretsmanager')
        secret_value_response = secrets_client.get_secret_value(SecretId=database_secret_name)
        secret_string = secret_value_response.get('SecretString')
        secret_dict = json.loads(secret_string) if secret_string else {}
        user = secret_dict.get('username') or secret_dict.get('user')
        password = secret_dict.get('password')
        if not user or not password:
            st.title('SECRETS ERROR: username/password not found in database_secret')
            sys.exit(1)
    except Exception as e:
        st.title('FAILED TO RETRIEVE DB SECRETS')
        st.write(str(e))
        sys.exit(1)

    ## Process detection (search multiple patterns)
    def detect_archival_processes():
        pattern = r"(rewards\\.ini|Mothership_Prod_RewardsDataArchival\\.py|RewardsDataArchival\\.py|DataArchival\\.py)"
        cmd = f"ps -eo pid,cmd --no-headers | grep -E \"{pattern}\" | grep -v grep"
        lines = os.popen(cmd).read().strip().splitlines()
        processes = []
        for line in lines:
            parts = line.strip().split(maxsplit=1)
            if len(parts) >= 1:
                pid = parts[0]
                cmdline = parts[1] if len(parts) > 1 else ""
                processes.append((pid, cmdline))
        return processes

    detected_processes = detect_archival_processes()
    pkill_output = [p[0] for p in detected_processes]
    d_output = [f"{pid} {cmdline}" for pid, cmdline in detected_processes]

    # Utilities
    def check_file_exists(fileexists):
        return os.path.isfile(fileexists)

    def get_date_from_details_filename(filename):
        match = re.search(r'DataProcessingAllDetails_(.*?)\\.txt', filename)
        if match:
            return match.group(1)
        else:
            return None

    # Dates
    getlogsdate = datetime.today().strftime('%d_%m_%Y')

    # Primary log files for today
    filename_all_details = log_direcotry + "DataProcessingAllDetails_" + getlogsdate + ".txt"
    filename_processed_files = log_direcotry + "DataProcessedFilesDetails_" + getlogsdate + ".txt"

    # If today's log not present, fallback to latest available and adjust date
    if not os.path.exists(filename_all_details):
        latest_details_cmd = f"ls -1t {log_direcotry}DataProcessingAllDetails_* 2>/dev/null | head -1 | xargs -n1 basename"
        latest_details_name = os.popen(latest_details_cmd).read().strip()
        latest_date = get_date_from_details_filename(latest_details_name) if latest_details_name else None
        if latest_date:
            getlogsdate = latest_date
            filename_all_details = log_direcotry + "DataProcessingAllDetails_" + getlogsdate + ".txt"
            filename_processed_files = log_direcotry + "DataProcessedFilesDetails_" + getlogsdate + ".txt"

    # Row processed file
    stream_file_cmd = log_direcotry + 'rowprocessed'
    cmd_stream = f'cat {stream_file_cmd} 2>/dev/null | head -1'
    stream = os.popen(cmd_stream)
    f_output = stream.readlines()
    output = f_output[0].strip() if f_output else "0"

    # Process info already computed above (pkill_output, d_output)

    st.markdown(f"## {archivaljob_stack} Archival Process Status!")

    if not os.path.exists(filename_all_details):
        st.title('PROCESS NOT STARTED, Please Contact DBA')
        st.write(filename_all_details)
        sys.exit(1)

    # DB helper
    def run_sql_queries(host, user, password, database, queries, ifselect):
        try:
            connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            cursor = connection.cursor()
            cursor.execute(queries)
            if (ifselect == 1):
                select_results = cursor.fetchone()
                return select_results
            connection.commit()
        except mysql.connector.Error as err:
            try:
                connection.rollback()
            except Exception:
                pass
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            try:
                connection.close()
            except Exception:
                pass

    def get_serverstats_details(replica_hosts_dict, master_host, database, user, password):
        replica_Lag_all = []
        replica_Lag_all.append('RDS_Alias:RDS_Endpoint:Replica_Lag\n')
        hll_Lag_all = []
        hll_Lag_all.append('RDS_Alias:RDS_Endpoint:HLL_Value\n')

        # Process replicas
        if replica_hosts_dict:
            for key, value in replica_hosts_dict.items():
                server = value
                slave_status = "show replica status ;"
                slavest = run_sql_queries(server, user, password, database, slave_status, 1)
                replication_lag_sec = slavest[32] if slavest else 0
                resultappend = f"{key}:{value}:{replication_lag_sec}\n"
                replica_Lag_all.append(resultappend)

                hll_query = "select count from information_schema.INNODB_METRICS where name like '%trx_rseg_history_len%'; "
                get_current_hll = run_sql_queries(server, user, password, database, hll_query, 1)
                hll_val = get_current_hll[0] if get_current_hll is not None else 0
                hll_Lag_all.append(f"{key}:{value}:{hll_val}\n")

        # Master HLL
        if master_host:
            hll_query = "select count from information_schema.INNODB_METRICS where name like '%trx_rseg_history_len%'; "
            get_current_hll = run_sql_queries(master_host, user, password, database, hll_query, 1)
            hll_val = get_current_hll[0] if get_current_hll is not None else 0
            hll_Lag_all.append(f"master:{master_host}:{hll_val}\n")

        return hll_Lag_all, replica_Lag_all

    # Parse replicas section into a dict
    replica_hosts = {}
    if config.has_section('replica'):
        for k, v in config.items('replica'):
            if v.strip():
                replica_hosts[k] = v.strip()

    col1, col2, col3 = st.columns([1,1,1])

    with col1:
        button_processid_c = st.button("SHOW PROCESSID", key='button1')
    with col2:
        button_killpid_c = st.button("KILL PROCESSID", key='button2')
    with col3:
        button_serverstats = st.button("Server Stats", key='button3')

    if button_processid_c:
        st.write("Process ID Details")
        st.write(d_output)

    if button_killpid_c:
        st.write("Process ID Details")
        st.write(d_output)
        if len(d_output) == 0:
            pass
        else:
            try:
                pkillid = int(pkill_output[0])
                st.write("ProcessedID Going to Kill:", pkillid)
                os.kill(pkillid, signal.SIGTERM)
                st.write('PROCESSID KILLED', pkillid)
            except Exception as e:
                st.write(f"Failed to kill process: {e}")

    if button_serverstats:
        st.write("Server Stats!")
        hll_values_all, replica_Lag_all = get_serverstats_details(replica_hosts, db_host, database, user, password)
        st.write(replica_Lag_all)
        st.write(hll_values_all)

        # Parse sleep seconds from archive logs like: "SLEEP: 5s for batch ..."
        get_current_sleep_command = f"grep -E 'SLEEP: *[0-9]+s' {filename_all_details} | tail -1 | awk '{{print $2}}' | tr -d 's'"
        get_current_sleep_value = os.popen(get_current_sleep_command).read().strip()
        try:
            get_currsleep = int(get_current_sleep_value) if get_current_sleep_value else 0
        except Exception:
            get_currsleep = 0
        st.write(f"Script Taking Pause:, {get_currsleep} Seconds, After Every batch exection.")

    st.text(f"Number Of Row Processing: {output}")

    starttime_command = f"head -10 {filename_all_details} | grep -i 'Script Starts at:' "
    get_starttime = os.popen(starttime_command).read()
    st.text(f"STARTED: {get_starttime}")

    endtime_command = f"tail -10 {filename_all_details} | grep -i 'Script Ends at:' "
    get_endtime = os.popen(endtime_command).read()
    if get_endtime.strip():
        st.text(f"ENDTIME: {get_endtime}")
    else:
        check_last_update_time = f'find {filename_all_details} -mmin -2 -exec stat -c "%y" {{}} \\;'
        get_last_updatetime = os.popen(check_last_update_time).read()
        if get_last_updatetime:
            st.text(f"ENDTIME: {get_endtime}")
        else:
            terminated_time = f"stat -c %y {filename_all_details}"
            get_terminated_time = os.popen(terminated_time).read()
            st.text(f"PROCESS TERMINATED AT : {get_terminated_time}")