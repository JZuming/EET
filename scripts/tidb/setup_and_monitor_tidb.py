import argparse
import os
import time
import psutil
import re
import sys
import datetime

server_setup_cmd = "nohup /root/tidb/bin/tidb-server >> server_log 2>&1 &"
server_log = 'server_log'
tester_log = 'tester_log'
def tester_setup_cmd(testdb):
    return "nohup ../qit/qcn --tidb-db=" + testdb + " --tidb-port=4000 >> tester_log 2>&1 &"

def check_server():
    pattern = re.compile(".*tidb-server.*", re.IGNORECASE)
    matching_processes = [p for p in psutil.process_iter() if pattern.search(p.name())]
    if matching_processes:
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for p in matching_processes:
            print("Time " + str(current_time) + " Process " + str(p.name()) + " with ID " + str(p.pid) + " is running.")
        return 1
    else:
        print("No matching processes found.")
        return 0

def check_log(log_name):
    f = open(log_name)
    last_30_lines = f.readlines()[-30:]
    f.close()
    return last_30_lines


def main(test_process_num):
    print("Test process number: " + str(test_process_num))
    print("Setting up the TiDB server")
    os.system(server_setup_cmd)
    time.sleep(5)
    server_exists = check_server()
    if server_exists != 1:
        sys.exit("server cannot be setted up")
    
    # set up the tester
    for i in range(0, test_process_num):
        new_dir = "test" + str(i)
        if not os.path.exists(new_dir):
            os.makedirs(new_dir)
        os.chdir(new_dir)
        print(os.getcwd())
        testdb = "testdb" + str(i)
        os.system(tester_setup_cmd(testdb))
        time.sleep(1)
        os.chdir("..")
    
    # monitor the server
    while True:
        server_exists = check_server()
        if server_exists == 1:
            time.sleep(60)
            continue
        
        # crash by expected error, set up the server again
        os.remove(server_log)
        time.sleep(5)
        os.system(server_setup_cmd)
        time.sleep(5)
        server_exists = check_server()
        if server_exists != 1:
            sys.exit("server cannot be setted up in 77")
        
        # only restart the tester stopped by lost connection
        for i in range(0, test_process_num):
            new_dir = "test" + str(i)
            os.chdir(new_dir)
            if not os.path.exists("origin"): # not logic bug
                # os.remove(tester_log)
                print(os.getcwd())
                testdb = "testdb" + str(i)
                os.system(tester_setup_cmd(testdb))
                time.sleep(1)
            os.chdir("..")

        continue


# use it with "nohup python3 -u setup_and_monitor_tidb.py 16 &"
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A script that set up and monitor TiDB server')
    parser.add_argument('test_process_num', type=int, help='the number of test processes.')
    args = parser.parse_args()

    main(args.test_process_num)
