from glob import glob
import os, time
import sys
import csv
import re
import subprocess
import pandas as pd
import pypyodbc
from multiprocessing import Process, Queue, Event, Pool, current_process, freeze_support


def check_files(path, files_dict_):
    """
    arguments -- root path to monitor
    returns   -- dictionary of {file: timestamp, ...}
    """
    sql_query_dirs = glob(path + "/*/IDABox/")
 
    files_dict = {}
    try:
        for sql_query_dir in sql_query_dirs:
            for root, dirs, filenames in os.walk(sql_query_dir):
                [files_dict.update({(root + filename): os.path.getmtime(root + filename)}) for 
                         filename in filenames if filename.endswith('.jsl')]
    except:
        files_dict = files_dict_ # for rare case the deleting files takes over while iteration. Try will envoke error.
        # pass

    return files_dict


def get_sql_string(jsl_file):
    """
    Reads in jsl (JMP script) file and extracts IDA credential and SQL statement.
    """
    fi  = open(jsl_file, 'r')
    
    jsl_script = fi.read()
    jsl_script_ = re.search('\((.*)\)', jsl_script, re.M | re.DOTALL).group()[1:-1]
    jsl_script_list = jsl_script_.strip().split('"')
    ora_string = "driver={Oracle in OraClient11g_home1};" + jsl_script_list[1].replace('DSN', 'DBQ') 
    sql_string = jsl_script_list[3].strip()
    if sql_string[-1] == ';': sql_string = sql_string[:-1]
    # sql_string_splits = re.split("where", sql_string, flags=re.IGNORECASE)
    # if sql_string_splits: 
    #     sql_string_preview = 'WHERE'.join( sql_string_splits[:-1] ) + "WHERE ROWNUM <=10 AND " + sql_string_splits[-1]
    # else:
    #     sql_string_preview = sql_string
 
    sql_string_ = "SELECT * FROM (" + sql_string + ") WHERE ROWNUM <= 10 "
    return ora_string, sql_string, sql_string_


def query_ida(jsl_file):
    """
    Connects to IDA database, and query.
    """
    # print "check8"
    file_surfix = os.path.splitext(jsl_file)[0]

    try:
        ora_string, sql_string, sql_string_ = get_sql_string(jsl_file)
        conn = pypyodbc.connect(ora_string)
        cur = conn.cursor()
        print "{0} is excuted.".format(jsl_file)
        print "Querying IDA with {0}...\n".format(sql_string) 

        print sql_string_
        data = cur.execute(sql_string_)
        df = pd.DataFrame(data.fetchall())
        df.columns = [column[0].upper() for column in cur.description]
        fo_name_ = file_surfix + '_preview.csv'
        df.to_csv(fo_name_, index=False)      
        
        data = cur.execute(sql_string)
        df = pd.DataFrame(data.fetchall())
        df.columns = [column[0].upper() for column in cur.description]
        fo_name = file_surfix + '.csv'
        df.to_csv(fo_name, index=False)

        print "IDA data saved to " + fo_name
    except:
        fo_error = file_surfix + '.err'
        fo = open(fo_error, 'w')
        print fo.write("{0} failed with error {1}. \n".format(jsl_file, str(sys.exc_info()[0])))


def worker_main(queue):
    print os.getpid(),"working"
    while True:
        item = queue.get(True)
        query_ida(item)

def main():
    the_queue = Queue()
    the_pool = Pool(8, worker_main,(the_queue,))

    path = "Y:/"
    before = check_files(path, {}) # get the current dictionary of sql_files
    while True:         #while loop to check the changes of the files
        time.sleep(5)
        sql_queue  = [] 
        after = check_files(path, before)
        print after
        added = [f for f in after if not f in before]
        deleted = [f for f in before if not f in after]
        overlapped = list(set(list(after)) & set(list(before)))
        updated = [f for f in overlapped if before[f] < after[f]]  
        
        before = after
        sql_queue = added + updated   
        print sql_queue
        if sql_queue:
            for jsl_file in sql_queue:
                try:
                    the_queue.put(jsl_file)
                except:
                    print "{0} failed with error {1}. \n".format(jsl_file, str(sys.exc_info()[0]))
                    pass
        else:
            pass



if __name__ == "__main__":
    main() 
