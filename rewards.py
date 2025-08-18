import os
import os.path
import streamlit as st
from datetime import datetime
import pandas as pd
import pandasql as ps
import sys
from pandasql import sqldf
import base64
import signal
import configparser
import re
import mysql.connector
from mysql.connector import Error
import boto3
import json

#generic_archivaljob_status_template.py




def default():

    #enable_display_button=0
    ###ENV
    archivaljob_stack="Mothership Rewards"

    # Read new config file
    config_file_path = "/opt/Scripts/prince_test_archive_script/rewards.ini"
    config = configparser.ConfigParser()
    if not os.path.exists(config_file_path):
        st.title("CONFIG FILE MISSING")
        st.write(config_file_path)
        sys.exit(1)
    config.read(config_file_path)

    # General settings
    aws_profile_raw = config.get('general', 'aws_profile', fallback='').strip()
    # sanitize possible surrounding brackets or quotes
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

    ## kill threadid


    pkill_stream = os.popen("ps -ef | grep Mothership_Prod_RewardsDataArchival.py |grep -v grep  | awk '{print $2}' ")
    d_stream = os.popen('ps -ef | grep Mothership_Prod_RewardsDataArchival.py |grep -v grep  ')


    ### END OF VARIABLES :

    #define Function:
    def check_file_exists(fileexists):
        if os.path.isfile(fileexists):
            #print(f"The file '{fileexists}' exists.")
            return True

        else:
            #print(f"The file '{fileexists}' does not exist.")
            return False

    def get_substring(filename):

        # Use regular expression to find the substring between '_' and '.'
        match = re.search(r'DataProcessingAllFilesDetails_(.*?)\.txt', filename)
        if match:
            return match.group(1)
        else:
            return None




    current_date = datetime.now()

    ## get dd_mm_yy  
    getlogsdate=datetime.today().strftime('%d_%m_%Y')



    # Determine logs date based on available files. Prefer AllDetails, then ProcessedFilesDetails.
    filename_all_details=log_direcotry+"DataProcessingAllDetails_"+getlogsdate+".txt"
    filename_processed_files=log_direcotry+"DataProcessedFilesDetails_"+getlogsdate+".txt"

    if not check_file_exists(filename_all_details) and not check_file_exists(filename_processed_files):
        # Try to infer date from latest AllDetails
        filecheckcommand_all = (
            f"ls -1rt {log_direcotry} 2>/dev/null | grep -E '^DataProcessingAllDetails_.*\\.txt$' | tail -1"
        )
        latest_all = os.popen(filecheckcommand_all).read().strip()
        if latest_all:
            m = re.search(r'DataProcessingAllDetails_(.*?)\.txt', latest_all)
            if m:
                getlogsdate = m.group(1)
                filename_all_details=log_direcotry+"DataProcessingAllDetails_"+getlogsdate+".txt"
                filename_processed_files=log_direcotry+"DataProcessedFilesDetails_"+getlogsdate+".txt"
        
        # If still not found, try ProcessedFilesDetails
        if not check_file_exists(filename_all_details) and not check_file_exists(filename_processed_files):
            filecheckcommand_proc = (
                f"ls -1rt {log_direcotry} 2>/dev/null | grep -E '^DataProcessedFilesDetails_.*\\.txt$' | tail -1"
            )
            latest_proc = os.popen(filecheckcommand_proc).read().strip()
            if latest_proc:
                m2 = re.search(r'DataProcessedFilesDetails_(.*?)\.txt', latest_proc)
                if m2:
                    getlogsdate = m2.group(1)
                    filename_all_details=log_direcotry+"DataProcessingAllDetails_"+getlogsdate+".txt"
                    filename_processed_files=log_direcotry+"DataProcessedFilesDetails_"+getlogsdate+".txt"

    # Read row processed count if exists
    stream_file_cmd=log_direcotry+'rowprocessed'
    output = "0"
    if os.path.exists(stream_file_cmd):
        cmd_stream= f'head -1 {stream_file_cmd}'
        stream = os.popen(cmd_stream)
        f_output = stream.readlines()
        output=f_output[0].strip() if f_output else "0"



    #pkill_stream = os.popen(get_pid_ql)
    pkill_output = pkill_stream.readlines()



    #d_stream = os.popen(get_pid_ql2)
    d_output = d_stream.readlines()



    ### EOVARIABLES

    fixed_height = 75


    st.markdown(f"## {archivaljob_stack} Archival Process Status!")




    if os.path.exists(filename_all_details) or os.path.exists(filename_processed_files):
        pass
    else:
        st.title('PROCESS NOT STARTED, Please Contact DBA')
        st.write(log_direcotry)
        return




    # No AllFilesDetails file dependency; proceed with available logs



    def is_file_not_empty(file_path):
        return os.path.isfile(file_path) and os.path.getsize(file_path) > 0


        processed_files = pd.read_csv(filename_processed_files,header=None)

        processed_files.columns = headers




    # MySQL Config:
    def run_sql_queries(host, user, password, database, queries,ifselect):
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
            # If you have SELECT queries and want to fetch results, you can do so here
            if (ifselect==1):
                select_results = cursor.fetchone()
                #print(select_results)
                return select_results

            # Commit the changes (for INSERT, UPDATE, DELETE queries)
            connection.commit()

        except mysql.connector.Error as err:
            # Handle any errors that occur during query execution
            #print("MySQL Error: {}".format(err))
            connection.rollback()

        finally:
            # Close the cursor and the database connection
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
        initiallist='RDS_Alias'+':'+ 'RDS_Endpoint'+':'+ 'Replica_Lag'+'\n'
        replica_Lag_all.append(initiallist)
        hll_Lag_all=[]
        initialhlllist='RDS_Alias'+':'+ 'RDS_Endpoint'+':'+ 'HLL_Value'+'\n'
        hll_Lag_all.append(initialhlllist)

        # Process replicas if defined
        if replica_hosts_dict:
            for key, value in replica_hosts_dict.items():
                server=value
                slave_status="show replica status ;"
                slavest=run_sql_queries(server, user, password, database, slave_status,1)
                replication_lag_sec  =  slavest[32] if slavest else 0
                resultappend=str(key)+':'+str(value)+':'+str(replication_lag_sec)+'\n'
                replica_Lag_all.append(resultappend)

                hll_query="select count from information_schema.INNODB_METRICS where name like '%trx_rseg_history_len%'; "
                get_current_hll=run_sql_queries(server, user, password, database, hll_query,1)
                if get_current_hll is not None:
                    hllappendres=str(key)+':'+str(value)+':'+str(get_current_hll[0])+'\n'
                else:
                    hllappendres=str(key)+':'+str(value)+':0\n'
                hll_Lag_all.append(hllappendres)

        # Master HLL value
        if master_host:
            hll_query="select count from information_schema.INNODB_METRICS where name like '%trx_rseg_history_len%'; "
            get_current_hll=run_sql_queries(master_host, user, password, database, hll_query,1)
            if get_current_hll is not None:
                hllappendres='master'+':'+str(master_host)+':'+str(get_current_hll[0])+'\n'
            else:
                hllappendres='master'+':'+str(master_host)+':0\n'
            hll_Lag_all.append(hllappendres)

        return hll_Lag_all,replica_Lag_all



    # Parse replicas section into a dict
    replica_hosts = {}
    if config.has_section('replica'):
        for k, v in config.items('replica'):
            if v.strip():
                replica_hosts[k] = v.strip()

    col1, col2, col3 = st.columns([1,1,1])
    
    with col1:
        button_processid_c=st.button("SHOW PROCESSID",key='button1')
    with col2:
        button_killpid_c=st.button("KILL PROCESSID",key='button2') 
    with col3:
        button_serverstats=st.button("Server Stats",key='button3')         



    if button_processid_c:

        st.write("Process ID Details")
        st.write(d_output)
        #pkillid=int(pkill_output[0])
        #st.write(pkillid)

        if len(d_output) == 0:
            displaytext="NO PROCESS RUNNING"
        else:
            displaytext="PROCESS IS RUNNING"


    if button_killpid_c:
        st.write("Process ID Details")
        st.write(d_output)        

        if len(d_output) == 0:
            displaytext="NO PROCESS RUNNING"

        else:
            pkillid=int(pkill_output[0])
            st.write("ProcessedID Going to Kill:",pkillid)
            st.write('PROCESSID KILLED',pkillid)
            os.kill(pkillid, signal.SIGTERM) 


    if button_serverstats:
        st.write("Server Stats!")
        hll_values_all,replica_Lag_all=get_serverstats_details(replica_hosts, db_host, database, user, password)

        st.write(replica_Lag_all)
        st.write(hll_values_all)        

        get_current_sleep_command=f"cat {filename_all_details}  | grep -i 'Calculated Current Sleep' | tail -1"

        get_current_sleep_value = os.popen(get_current_sleep_command).read()

        def get_last_word(s):
            words = s.split()
            if words:
                return words[-1]
            else:
                return None         

        get_currsleep = get_last_word(get_current_sleep_value)
        if get_currsleep is None:
            get_currsleep=10


        st.write(f"Script Taking Pause:, {get_currsleep} Seconds, After Every batch exection.") 


    # timings first, then row count (per dashboard requirement)





    starttime_command = f"head -10 {filename_all_details} | grep -i 'Script Starts at:' "

    get_starttime = os.popen(starttime_command).read()

    st.text(f"Script Starts at: {get_starttime.strip()}")

    endtime_command = f"tail -10 {filename_all_details} | grep -i 'Script Ends at:' "
    
    get_endtime = os.popen(endtime_command).read()
    if get_endtime.strip():  # strip removes leading/trailing whitespace
        st.text(f"Script Ends at: {get_endtime.strip()}")

    else:
        check_last_update_time = f'find {filename_all_details} -mmin -2 -exec stat -c "%y" {{}} \\;'
        get_last_updatetime = os.popen(check_last_update_time).read() 

        if get_last_updatetime:
            st.text(f"Script Ends at: {get_endtime.strip()}")

        else:
            terminated_time=f"stat -c %y {filename_all_details}"
            get_terminated_time=os.popen(terminated_time).read() 
            st.text(f"Terminate at: {get_terminated_time.strip()}")

    # finally show number of rows processed
    st.text(f"Number Of Row Processing: {output}")

