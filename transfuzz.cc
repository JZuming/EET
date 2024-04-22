#include "config.h"

#include <iostream>
#include <chrono>

#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

#include <thread>
#include <typeinfo>

#include "random.hh"
#include "grammar.hh"
#include "relmodel.hh"
#include "schema.hh"
#include "gitrev.h"

#include "log.hh"
#include "dump.hh"
#include "impedance.hh"
#include "dut.hh"

#include "postgres.hh"

#include <sys/time.h>
#include <sys/wait.h>

using namespace std;

using namespace std::chrono;

extern "C" {
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
}

#include "transaction_test.hh"

#define NORMAL_EXIT 0
#define FIND_BUG_EXIT 7
#define MAX_TIMEOUT_TIME 3
#define MAX_SETUP_TRY_TIME 3

int child_pid = 0;
bool child_timed_out = false;

extern int write_op_id;

void kill_process_signal(int signal)  
{  
    if(signal != SIGALRM) {  
        printf("unexpect signal %d\n", signal);  
        exit(1);  
    }

    if (child_pid > 0) {
        printf("child pid timeout, kill it\n"); 
        child_timed_out = true;
		kill(child_pid, SIGKILL);
        // also kill server process to restart
        while (try_to_kill_server() == false) {}
	}

    cerr << "get SIGALRM, stop the process" << endl;
    return;  
}

int fork_for_generating_database(dbms_info& d_info)
{
    static itimerval itimer;
    fork_if_server_closed(d_info);
    
    write_op_id = 0;
    child_pid = fork();
    if (child_pid == 0) { // in child process
        generate_database(d_info);
        ofstream output_wkey("wkey.txt");
        output_wkey << write_op_id << endl;
        output_wkey.close();
        exit(NORMAL_EXIT);
    }

    itimer.it_value.tv_sec = TRANSACTION_TIMEOUT;
    itimer.it_value.tv_usec = 0; // us limit
    setitimer(ITIMER_REAL, &itimer, NULL);

    int status;
    auto res = waitpid(child_pid, &status, 0);
    if (res <= 0) {
        cerr << "waitpid() fail: " <<  res << endl;
        throw runtime_error(string("waitpid() fail"));
    }

    if (!WIFSTOPPED(status)) 
        child_pid = 0;
    
    itimer.it_value.tv_sec = 0;
    itimer.it_value.tv_usec = 0;
    setitimer(ITIMER_REAL, &itimer, NULL);
    
    if (WIFEXITED(status)) {
        auto exit_code =  WEXITSTATUS(status); // only low 8 bit (max 255)
        // cerr << "exit code: " << exit_code << endl;
        if (exit_code == FIND_BUG_EXIT) {
            cerr << RED << "a bug is found in fork process" << RESET << endl;
            transaction_test::record_bug_num++;
            exit(-1);
        }
        if (exit_code == 255)
            exit(-1);
    }

    if (WIFSIGNALED(status)) {
        auto killSignal = WTERMSIG(status);
        if (child_timed_out && killSignal == SIGKILL) {
            cerr << "timeout in generating stmt, reset the seed" << endl;
            // transaction_test::try_to_kill_server();
            // auto just_check_server = make_shared<transaction_test>(d_info);
            // auto restart = just_check_server->fork_if_server_closed();
            // if (restart)
            //     throw runtime_error(string("restart server")); // need to generate database again
            
            smith::rng.seed(time(NULL));
            throw runtime_error(string("transaction test timeout"));
        }
        else {
            cerr << RED << "find memory bug" << RESET << endl;
            cerr << "killSignal: " << killSignal << endl;
            throw runtime_error(string("memory bug"));
        }
    }

    ifstream input_wkey("wkey.txt");
    input_wkey >> write_op_id;
    input_wkey.close();

    write_op_id++;
    // cerr << "updating write_op_id: "<< write_op_id << endl;

    return 0;
}

int fork_for_transaction_test(dbms_info& d_info)
{
    static itimerval itimer;

    fork_if_server_closed(d_info);
    
    child_pid = fork();
    if (child_pid == 0) { // in child process
        try {
            // cerr << "write_op_id: " << write_op_id << endl;
            transaction_test tt(d_info);
            auto ret = tt.oracle_test();
            if (ret == 1) {
                cerr << RED << "Find a bug !!!" << RESET << endl;
                exit(FIND_BUG_EXIT);
            }
        } catch(std::exception &e) { // ignore runtime error
            cerr << "in test: " << e.what() << endl;
        }
        exit(NORMAL_EXIT);
    }

    itimer.it_value.tv_sec = TRANSACTION_TIMEOUT;
    itimer.it_value.tv_usec = 0; // us limit
    setitimer(ITIMER_REAL, &itimer, NULL);

    int status;
    auto res = waitpid(child_pid, &status, 0);
    if (res <= 0) {
        cerr << "waitpid() fail: " <<  res << endl;
        throw runtime_error(string("waitpid() fail"));
    }

    if (!WIFSTOPPED(status)) 
        child_pid = 0;
    
    itimer.it_value.tv_sec = 0;
    itimer.it_value.tv_usec = 0;
    setitimer(ITIMER_REAL, &itimer, NULL);
    
    if (WIFEXITED(status)) {
        auto exit_code =  WEXITSTATUS(status); // only low 8 bit (max 255)
        // cerr << "exit code: " << exit_code << endl;
        if (exit_code == FIND_BUG_EXIT) {
            cerr << RED << "a bug is found in fork process" << RESET << endl;
            transaction_test::record_bug_num++;
            // exit(-1);
        }
        if (exit_code == 255)
            exit(-1);
    }

    if (WIFSIGNALED(status)) {
        auto killSignal = WTERMSIG(status);
        if (child_timed_out && killSignal == SIGKILL) {
            cerr << "timeout in generating stmt, reset the seed" << endl;
            smith::rng.seed(time(NULL));
            throw runtime_error(string("transaction test timeout"));
        }
        else {
            cerr << RED << "find memory bug" << RESET << endl;
            cerr << "killSignal: " << killSignal << endl;
            exit(-1);
            throw runtime_error(string("memory bug"));
        }
    }

    return 0;
}

int random_test(dbms_info& d_info)
{   
    cerr << YELLOW << "initial seed as time(NULL)" << RESET << endl;
    smith::rng.seed(time(NULL));
    
    // reset the target DBMS to initial state
    int setup_try_time = 0;
    while (1) {
        if (setup_try_time > MAX_SETUP_TRY_TIME) {
            kill_server_process_with_SIGTERM();
            setup_try_time = 0;
        }

        try {
            // donot fork, so that the static schema can be used in each test case
            fork_if_server_closed(d_info);
            generate_database(d_info);
            
            // fork_for_generating_database(d_info);
            break;
        } catch(std::exception &e) {
            cerr << e.what() << " in setup stage" << endl;
            setup_try_time++;
        }
    } 

    int i = TEST_TIME_FOR_EACH_DB;
    while (i--) {  
        try {
            fork_for_transaction_test(d_info);
        } catch (exception &e) {
            string err = e.what();
            cerr << "ERROR in random_test: " << err << endl;
            if (err == "restart server")
                break;
            else if (err == "transaction test timeout") {
                break; // break the test and begin a new test
                // after killing and starting a new server, created tables might be lost
                // so it needs to begin a new test to generate tables
            }
            else {
                cerr << "the exception cannot be handled" << endl;
                throw e;
            }
        }
    }
    
    return 0;
}


bool reproduce_routine(dbms_info& d_info,
                        vector<shared_ptr<prod>>& stmt_queue, 
                        vector<int>& tid_queue,
                        vector<stmt_usage> usage_queue,
                        string& err_info)
{
    fork_if_server_closed(d_info);
    
    transaction_test re_test(d_info);
    re_test.stmt_queue = stmt_queue;
    re_test.tid_queue = tid_queue;
    re_test.stmt_num = re_test.tid_queue.size();
    re_test.stmt_use = usage_queue;

    int max_tid = -1;
    for (auto tid:tid_queue) {
        if (tid > max_tid)
            max_tid = tid;
    }

    re_test.trans_num = max_tid + 1;
    delete[] re_test.trans_arr;
    re_test.trans_arr = new transaction[re_test.trans_num];

    cerr << re_test.trans_num << " " << re_test.tid_queue.size() << " " << re_test.stmt_queue.size() << endl;
    if (re_test.tid_queue.size() != re_test.stmt_queue.size()) {
        cerr << "tid queue size should equal to stmt queue size" << endl;
        return 0;
    }

    // init each transaction stmt
    for (int i = 0; i < re_test.stmt_num; i++) {
        auto tid = re_test.tid_queue[i];
        re_test.trans_arr[tid].stmts.push_back(re_test.stmt_queue[i]);
    }

    for (int tid = 0; tid < re_test.trans_num; tid++) {
        re_test.trans_arr[tid].dut = dut_setup(d_info);
        re_test.trans_arr[tid].stmt_num = re_test.trans_arr[tid].stmts.size();
        if (re_test.trans_arr[tid].stmts.empty()) {
            re_test.trans_arr[tid].status = TXN_ABORT;
            continue;
        }

        auto stmt_str = print_stmt_to_string(re_test.trans_arr[tid].stmts.back());
        if (stmt_str.find("COMMIT") != string::npos)
            re_test.trans_arr[tid].status = TXN_COMMIT;
        else
            re_test.trans_arr[tid].status = TXN_ABORT;
    }
    
    try {
        re_test.trans_test();
        // /* only check memory bugs
        shared_ptr<dependency_analyzer> tmp_da;
        if (re_test.analyze_txn_dependency(tmp_da)) {
            string bug_str = "Find bugs in analyze_txn_dependency";
            cerr << RED << bug_str << RESET << endl;
            if (err_info != "" && err_info != bug_str) {
                cerr << "not same as the original bug" << endl;
                return false;
            }
            err_info = bug_str;
            return true;
        }
        set<stmt_id> empty_deleted_nodes;
        bool delete_flag = false;
        auto longest_stmt_path = tmp_da->topological_sort_path(empty_deleted_nodes, &delete_flag);
        if (delete_flag == true) {
            cerr << "the test case contains cycle and cannot be properly sorted" << endl;
            return false;
        }
        cerr << RED << "stmt path for normal test: " << RESET;
        print_stmt_path(longest_stmt_path, tmp_da->stmt_dependency_graph);

        re_test.normal_stmt_test(longest_stmt_path);
        if (re_test.check_normal_stmt_result(longest_stmt_path, false) == false) {
            string bug_str = "Find bugs in check_normal_stmt_result";
            cerr << RED << bug_str << RESET << endl;
            if (err_info != "" && err_info != bug_str) {
                cerr << "not same as the original bug" << endl;
                return false;
            }
            err_info = bug_str;
            return true;
        }
        // */
    } catch (exception &e) {
        string cur_err_info = e.what();
        cerr << "exception captured by test: " << cur_err_info << endl;
        if (cur_err_info.find("INSTRUMENT_ERR") != string::npos) // it is cause by: after instrumented, the scheduling change and error in txn_test happens
            return false;
        if (err_info != "" && err_info != cur_err_info) {
            cerr << "not same as the original bug" << endl;
            return false;
        }
        err_info = cur_err_info;
        return true;
    }

    return false;
}

bool check_txn_cycle(dbms_info& d_info,
                        vector<shared_ptr<prod>>& stmt_queue, 
                        vector<int>& tid_queue,
                        vector<stmt_usage>& usage_queue)
{
    fork_if_server_closed(d_info);

    transaction_test re_test(d_info);
    re_test.stmt_queue = stmt_queue;
    re_test.tid_queue = tid_queue;
    re_test.stmt_num = re_test.tid_queue.size();
    re_test.stmt_use = usage_queue;

    int max_tid = -1;
    for (auto tid:tid_queue) {
        if (tid > max_tid)
            max_tid = tid;
    }

    re_test.trans_num = max_tid + 1;
    delete[] re_test.trans_arr;
    re_test.trans_arr = new transaction[re_test.trans_num];

    cerr << "txn num: " << re_test.trans_num << endl
        << "tid_queue size: " << re_test.tid_queue.size() << endl
        << "stmt_queue size: " << re_test.stmt_queue.size() << endl;
    if (re_test.tid_queue.size() != re_test.stmt_queue.size()) {
        cerr << "tid queue size should equal to stmt queue size" << endl;
        return false;
    }

    // init each transaction stmt
    for (int i = 0; i < re_test.stmt_num; i++) {
        auto tid = re_test.tid_queue[i];
        re_test.trans_arr[tid].stmts.push_back(re_test.stmt_queue[i]);
    }

    for (int tid = 0; tid < re_test.trans_num; tid++) {
        re_test.trans_arr[tid].dut = dut_setup(d_info);
        re_test.trans_arr[tid].stmt_num = re_test.trans_arr[tid].stmts.size();
        if (re_test.trans_arr[tid].stmts.empty()) {
            re_test.trans_arr[tid].status = TXN_ABORT;
            continue;
        }

        auto stmt_str = print_stmt_to_string(re_test.trans_arr[tid].stmts.back());
        if (stmt_str.find("COMMIT") != string::npos)
            re_test.trans_arr[tid].status = TXN_COMMIT;
        else
            re_test.trans_arr[tid].status = TXN_ABORT;
    }
    
    try {
        re_test.trans_test();
        shared_ptr<dependency_analyzer> tmp_da;
        re_test.analyze_txn_dependency(tmp_da);
        set<int> cycle_nodes;
        vector<int> sorted_nodes;
        tmp_da->check_txn_graph_cycle(cycle_nodes, sorted_nodes);
        if (!cycle_nodes.empty()) {
            cerr << "Has transactional cycles" << endl;
            return true;
        }
        else {
            cerr << "No transactional cycle" << endl;
            return false;
        }
    } catch (exception &e) {
        string cur_err_info = e.what();
        cerr << "exception captured by test: " << cur_err_info << endl;
    }

    return false;
}

void txn_decycle_test(dbms_info& d_info,
                    vector<shared_ptr<prod>>& stmt_queue, 
                    vector<int>& tid_queue,
                    vector<stmt_usage>& usage_queue,
                    int& succeed_time,
                    int& all_time,
                    vector<int> delete_nodes)
{
    fork_if_server_closed(d_info);

    transaction_test re_test(d_info);
    re_test.stmt_queue = stmt_queue;
    re_test.tid_queue = tid_queue;
    re_test.stmt_num = re_test.tid_queue.size();
    re_test.stmt_use = usage_queue;

    int max_tid = -1;
    for (auto tid:tid_queue) {
        if (tid > max_tid)
            max_tid = tid;
    }

    re_test.trans_num = max_tid + 1;
    delete[] re_test.trans_arr;
    re_test.trans_arr = new transaction[re_test.trans_num];

    cerr << "txn num: " << re_test.trans_num << endl
        << "tid_queue size: " << re_test.tid_queue.size() << endl
        << "stmt_queue size: " << re_test.stmt_queue.size() << endl;
    if (re_test.tid_queue.size() != re_test.stmt_queue.size()) {
        cerr << "tid queue size should equal to stmt queue size" << endl;
        return;
    }

    // init each transaction stmt
    for (int i = 0; i < re_test.stmt_num; i++) {
        auto tid = re_test.tid_queue[i];
        re_test.trans_arr[tid].stmts.push_back(re_test.stmt_queue[i]);
    }

    for (int tid = 0; tid < re_test.trans_num; tid++) {
        re_test.trans_arr[tid].dut = dut_setup(d_info);
        re_test.trans_arr[tid].stmt_num = re_test.trans_arr[tid].stmts.size();
        if (re_test.trans_arr[tid].stmts.empty()) {
            re_test.trans_arr[tid].status = TXN_ABORT;
            continue;
        }

        auto stmt_str = print_stmt_to_string(re_test.trans_arr[tid].stmts.back());
        if (stmt_str.find("COMMIT") != string::npos)
            re_test.trans_arr[tid].status = TXN_COMMIT;
        else
            re_test.trans_arr[tid].status = TXN_ABORT;
    }
    
    try {
        re_test.trans_test();
        shared_ptr<dependency_analyzer> tmp_da;
        re_test.analyze_txn_dependency(tmp_da);
        set<int> cycle_nodes;
        vector<int> sorted_nodes;
        tmp_da->check_txn_graph_cycle(cycle_nodes, sorted_nodes);
        tmp_da->print_dependency_graph();
        if (!cycle_nodes.empty()) { // need decycle
            for (auto txn_id : cycle_nodes) {
                auto new_stmt_queue = stmt_queue;
                auto new_usage_queue = usage_queue;
                int stmt_num = new_stmt_queue.size();

                // delete the txn whose id is txn_id
                for (int i = 0; i < stmt_num; i++) {
                    if (tid_queue[i] != txn_id)
                        continue;
                    // commit and abort stmt
                    if (usage_queue[i] == INIT_TYPE)
                        continue;
                    new_stmt_queue[i] = make_shared<txn_string_stmt>((prod *)0, SPACE_HOLDER_STMT);
                    new_usage_queue[i] = INIT_TYPE;
                    new_usage_queue[i].is_instrumented = false;
                }

                for (int tid = 0; tid < re_test.trans_num; tid++) {
                    re_test.trans_arr[tid].dut.reset();
                }

                delete_nodes.push_back(txn_id);
                cerr << "delete nodes: ";
                for (auto node:delete_nodes)
                    cerr << node << " ";
                cerr << endl;

                // after deleting the txn, try it again
                txn_decycle_test(d_info, new_stmt_queue, tid_queue, new_usage_queue, succeed_time, all_time, delete_nodes);
                delete_nodes.pop_back();
            }
        }
        else { // no cycle, perform txn sorting and check results
            vector<stmt_id> txn_stmt_path;
            for (auto txn_id:sorted_nodes) {
                auto txn_stmt_num = re_test.trans_arr[txn_id].stmt_num;
                for (int count = 0; count < txn_stmt_num; count++) {
                    auto s_id = stmt_id(txn_id, count);
                    auto stmt_idx = s_id.transfer_2_stmt_idx(tid_queue);
                    if (usage_queue[stmt_idx] == INIT_TYPE) // skip begin, commit, abort, SPACE_HOLDER_STMT
                        continue;
                    txn_stmt_path.push_back(s_id);
                }
            }

            re_test.normal_stmt_test(txn_stmt_path);
            if (re_test.check_normal_stmt_result(txn_stmt_path, false) == false) {
                string bug_str = "Find bugs in check_normal_stmt_result";
                cerr << RED << bug_str << RESET << endl;
                succeed_time++;
            }
            all_time++;
            cerr << "succeed_time: " << succeed_time << " all_time: " << all_time << endl;
        }
    } catch (exception &e) {
        string cur_err_info = e.what();
        cerr << "exception captured by test: " << cur_err_info << endl;
        all_time++;
        cerr << "succeed_time: " << succeed_time << " all_time: " << all_time << endl;
    }

    return;
}

void check_topo_sort(dbms_info& d_info,
                    vector<shared_ptr<prod>>& stmt_queue, 
                    vector<int>& tid_queue,
                    vector<stmt_usage>& usage_queue,
                    int& succeed_time,
                    int& all_time)
{
    fork_if_server_closed(d_info);

    transaction_test re_test(d_info);
    re_test.stmt_queue = stmt_queue;
    re_test.tid_queue = tid_queue;
    re_test.stmt_num = re_test.tid_queue.size();
    re_test.stmt_use = usage_queue;

    int max_tid = -1;
    for (auto tid:tid_queue) {
        if (tid > max_tid)
            max_tid = tid;
    }

    re_test.trans_num = max_tid + 1;
    delete[] re_test.trans_arr;
    re_test.trans_arr = new transaction[re_test.trans_num];

    cerr << "txn num: " << re_test.trans_num << endl
        << "tid_queue size: " << re_test.tid_queue.size() << endl
        << "stmt_queue size: " << re_test.stmt_queue.size() << endl;
    if (re_test.tid_queue.size() != re_test.stmt_queue.size()) {
        cerr << "tid queue size should equal to stmt queue size" << endl;
        return;
    }

    // init each transaction stmt
    for (int i = 0; i < re_test.stmt_num; i++) {
        auto tid = re_test.tid_queue[i];
        re_test.trans_arr[tid].stmts.push_back(re_test.stmt_queue[i]);
    }

    for (int tid = 0; tid < re_test.trans_num; tid++) {
        re_test.trans_arr[tid].dut = dut_setup(d_info);
        re_test.trans_arr[tid].stmt_num = re_test.trans_arr[tid].stmts.size();
        if (re_test.trans_arr[tid].stmts.empty()) {
            re_test.trans_arr[tid].status = TXN_ABORT;
            continue;
        }

        auto stmt_str = print_stmt_to_string(re_test.trans_arr[tid].stmts.back());
        if (stmt_str.find("COMMIT") != string::npos)
            re_test.trans_arr[tid].status = TXN_COMMIT;
        else
            re_test.trans_arr[tid].status = TXN_ABORT;
    }
    
    try {
        re_test.trans_test();
        shared_ptr<dependency_analyzer> tmp_da;
        re_test.analyze_txn_dependency(tmp_da);
        auto all_topo_sort = tmp_da->get_all_topo_sort_path();
	cerr << "topo sort size: " << all_topo_sort.size() << endl;
        for (auto& sort : all_topo_sort) {
            cerr << RED << "stmt path for normal test: " << RESET;
            print_stmt_path(sort, tmp_da->stmt_dependency_graph);

            re_test.normal_stmt_output.clear();
            re_test.normal_stmt_err_info.clear();
            re_test.normal_stmt_db_content.clear();
            re_test.normal_stmt_test(sort);
            if (re_test.check_normal_stmt_result(sort, false) == false) {
                succeed_time++;
            }
            all_time++;
            cerr << "succeed_time: " << succeed_time << " all_time: " << all_time << "/" << all_topo_sort.size() << endl;
        }
    } catch (exception &e) {
        string cur_err_info = e.what();
        cerr << "exception captured by test: " << cur_err_info << endl;
        all_time++;
        cerr << "succeed_time: " << succeed_time << " all_time: " << all_time << endl;
    }

    return;
}

void save_current_testcase(vector<shared_ptr<prod>>& stmt_queue,
                            vector<int>& tid_queue,
                            vector<stmt_usage>& usage_queue,
                            string stmt_file_name,
                            string tid_file_name,
                            string usage_file_name)
{
    // save stmt queue
    ofstream mimimized_stmt_output(stmt_file_name);
    for (int i = 0; i < stmt_queue.size(); i++) {
        mimimized_stmt_output << print_stmt_to_string(stmt_queue[i]) << endl;
        mimimized_stmt_output << endl;
    }
    mimimized_stmt_output.close();

    // save tid queue
    ofstream minimized_tid_output(tid_file_name);
    for (int i = 0; i < tid_queue.size(); i++) {
        minimized_tid_output << tid_queue[i] << endl;
    }
    minimized_tid_output.close();

    // save stmt usage queue
    ofstream minimized_usage_output(usage_file_name);
    for (int i = 0; i < usage_queue.size(); i++) {
        minimized_usage_output << usage_queue[i] << endl;
    }
    minimized_usage_output.close();
    
    return;
}

bool minimize_testcase(dbms_info& d_info,
                        vector<shared_ptr<prod>>& stmt_queue, 
                        vector<int>& tid_queue,
                        vector<stmt_usage> usage_queue)
{
    cerr << "Check reproduce..." << endl;
    string original_err;
    auto r_check = reproduce_routine(d_info, stmt_queue, tid_queue, usage_queue, original_err);
    if (!r_check) {
        cerr << "No" << endl;
        return false;
    }
    cerr << "Yes" << endl;
    
    int max_tid = -1;
    for (auto tid:tid_queue) {
        if (tid > max_tid)
            max_tid = tid;
    }
    int txn_num = max_tid + 1;
    
    auto final_stmt_queue = stmt_queue;
    vector<int> final_tid_queue = tid_queue;
    vector<stmt_usage> final_usage_queue = usage_queue;
    
    // txn level minimize
    for (int tid = 0; tid < txn_num; tid++) {
        cerr << "Try to delete txn " << tid << "..." << endl;

        auto tmp_stmt_queue = final_stmt_queue;
        vector<int> tmp_tid_queue = final_tid_queue;
        vector<stmt_usage> tmp_usage_queue = final_usage_queue;

        // delete current tid
        for (int i = 0; i < tmp_tid_queue.size(); i++) {
            if (tmp_tid_queue[i] != tid)
                continue;
            
            tmp_stmt_queue.erase(tmp_stmt_queue.begin() + i);
            tmp_tid_queue.erase(tmp_tid_queue.begin() + i);
            tmp_usage_queue.erase(tmp_usage_queue.begin() + i);
            i--;
        }

        // adjust tid queue
        for (int i = 0; i < tmp_tid_queue.size(); i++) {
            if (tmp_tid_queue[i] < tid)
                continue;
            
            tmp_tid_queue[i]--;
        }

        int try_time = 1;
        bool trigger_bug = false;
        while (try_time--) {
            trigger_bug = reproduce_routine(d_info, tmp_stmt_queue, tmp_tid_queue, tmp_usage_queue, original_err);
            if (trigger_bug == true)
                break;
        }
        if (trigger_bug == false)
            continue;
        // auto ret = reproduce_routine(d_info, tmp_stmt_queue, tmp_tid_queue, tmp_usage_queue);
        // if (ret == false)
        //     continue;

        // reduction succeed
        cerr << "Succeed to delete txn " << tid << "\n\n\n" << endl;
        
        int pause;
        cerr << "Enter an integer: 0 skip, other save" << endl;
        cin >> pause;
        if (pause == 0)
            continue;
	
	final_stmt_queue = tmp_stmt_queue;
        final_tid_queue = tmp_tid_queue;
        final_usage_queue = tmp_usage_queue;
        tid--;
        txn_num--;

	save_current_testcase(final_stmt_queue, final_tid_queue, final_usage_queue, 
                            "min_stmts.sql", "min_tid.txt", "min_usage.txt");
    }
    
    // stmt level minimize
    auto stmt_num = final_tid_queue.size();
    auto dut = dut_setup(d_info);
    for (int i = 0; i < stmt_num; i++) {
        cerr << "Try to delete stmt " << i << "..." << endl;

        auto tmp_stmt_queue = final_stmt_queue;
        vector<int> tmp_tid_queue = final_tid_queue;
        vector<stmt_usage> tmp_usage_queue = final_usage_queue;
	    auto tmp_stmt_num = stmt_num;

        // do not delete commit or abort
        auto tmp_stmt_str = print_stmt_to_string(tmp_stmt_queue[i]);
        if (tmp_stmt_str.find(dut->begin_stmt()) != string::npos)
            continue;
        if (tmp_stmt_str.find(dut->commit_stmt()) != string::npos)
            continue;
        if (tmp_stmt_str.find(dut->abort_stmt()) != string::npos)
            continue;

        // do not delete instrumented stmts
        if (tmp_usage_queue[i].is_instrumented == true)
            continue;
        
        auto original_i = i;

        // delete possible AFTER_WRITE_READ
        if (i + 1 <= tmp_usage_queue.size() && tmp_usage_queue[i + 1] == AFTER_WRITE_READ) {
            // deleting later stmt donot need to goback the "i"
            tmp_stmt_queue.erase(tmp_stmt_queue.begin() + i + 1);
            tmp_tid_queue.erase(tmp_tid_queue.begin() + i + 1);
            tmp_usage_queue.erase(tmp_usage_queue.begin() + i + 1);
            tmp_stmt_num--;
        }

        // delete the statement
        tmp_stmt_queue.erase(tmp_stmt_queue.begin() + i);
        tmp_tid_queue.erase(tmp_tid_queue.begin() + i);
        tmp_usage_queue.erase(tmp_usage_queue.begin() + i);
        tmp_stmt_num--;
        i--;

        // delete possible BEFORE_WRITE_READ and VERSION_SET_READ, note that i point the element before its original position
        while (i >= 0 && (tmp_usage_queue[i] == BEFORE_WRITE_READ ||
                            tmp_usage_queue[i] == VERSION_SET_READ)) {
            tmp_stmt_queue.erase(tmp_stmt_queue.begin() + i);
            tmp_tid_queue.erase(tmp_tid_queue.begin() + i);
            tmp_usage_queue.erase(tmp_usage_queue.begin() + i);
            tmp_stmt_num--;
            i--;
        }

        int try_time = 1;
        bool trigger_bug = false;
        while (try_time--) {
            trigger_bug = reproduce_routine(d_info, tmp_stmt_queue, tmp_tid_queue, tmp_usage_queue, original_err);
            if (trigger_bug == true)
                break;
        }
        if (trigger_bug == false) {
            i = original_i;
            continue;
        }
        
        // reduction succeed
        cerr << "Succeed to delete stmt " << "\n\n\n" << endl;
        
	    int pause;
        cerr << "Enter an integer: 0 skip, other save" << endl;
        cin >> pause;
        if (pause == 0) {
	        i = original_i;
	        continue;
	    }
	
	    final_stmt_queue = tmp_stmt_queue;
        final_tid_queue = tmp_tid_queue;
        final_usage_queue = tmp_usage_queue;
	    stmt_num = tmp_stmt_num;
        save_current_testcase(final_stmt_queue, final_tid_queue, final_usage_queue, 
                            "min_stmts.sql", "min_tid.txt", "min_usage.txt");
    }

    if (final_stmt_queue.size() == stmt_queue.size())
        return false;

    stmt_queue = final_stmt_queue;
    tid_queue = final_tid_queue;
    usage_queue = final_usage_queue;

    save_current_testcase(stmt_queue, tid_queue, usage_queue, 
                            "min_stmts.sql", "min_tid.txt", "min_usage.txt");

    return true;
}

int main(int argc, char *argv[])
{
    // analyze the options
    map<string,string> options;
    regex optregex("--\
(help|min|postgres|sqlite|monetdb|random-seed|\
postgres-db|postgres-port|\
tidb-db|tidb-port|\
mysql-db|mysql-port|\
mariadb-db|mariadb-port|\
oceanbase-db|oceanbase-port|\
monetdb-db|monetdb-port|\
cockroach-db|cockroach-port|\
output-or-affect-num|\
check-txn-cycle|\
txn-decycle|\
check-topo-sort|\
reproduce-sql|reproduce-tid|reproduce-usage)(?:=((?:.|\n)*))?");
  
    for(char **opt = argv + 1 ;opt < argv + argc; opt++) {
        smatch match;
        string s(*opt);
        if (regex_match(s, match, optregex)) {
            options[string(match[1])] = match[2];
        } else {
            cerr << "Cannot parse option: " << *opt << endl;
            options["help"] = "";
        }
    }

    if (options.count("help")) {
        cerr <<
            "    --postgres=connstr   postgres database to send queries to" << endl <<
            "    --postgres-db=connstr  Postgres database to send queries to, should used with --postgres-port" <<endl <<
            "    --postgres-port=int    Postgres server port number, , should used with --postgres-port" <<endl <<
            #ifdef HAVE_LIBSQLITE3
            "    --sqlite=URI         SQLite database to send queries to" << endl <<
            #endif
            #ifdef HAVE_MONETDB
            "    --monetdb-db=connstr  MonetDB database to send queries to" <<endl <<
            "    --monetdb-port=int    MonetDB server port number" <<endl <<
            #endif
            #ifdef HAVE_LIBMYSQLCLIENT
            #ifdef HAVE_TIDB
            "    --tidb-db=constr   tidb database name to send queries to (should used with" << endl << 
            "    --tidb-port=int    tidb server port number" << endl << 
            #endif
            #ifdef HAVE_MARIADB
            "    --mariadb-db=constr   mariadb database name to send queries to (should used with" << endl << 
            "    --mariadb-port=int    mariadb server port number" << endl <<
            #endif
            #ifdef HAVE_OCEANBASE
            "    --oceanbase-db=constr   oceanbase database name to send queries to (should used with" << endl << 
            "    --oceanbase-port=int    oceanbase server port number" << endl <<
            #endif
            #ifdef HAVE_MYSQL
            "    --mysql-db=constr  mysql database name to send queries to (should used with" << endl << 
            "    --mysql-port=int   mysql server port number" << endl << 
            #endif
            #endif
            "    --cockroach-db=constr  cockroach database name to send queries to (should used with" << endl << 
            "    --cockroach-port=int   cockroach server port number" << endl << 
            "    --output-or-affect-num=int     generating statement that output num rows or affect at least num rows (useless for crash test)" << endl <<
            "    --random-seed=filename         using file instead of random seed for test-case generation. It only can be used for one time generation" << endl <<
            "    --reproduce-sql=filename       sql file to reproduce the problem" << endl <<
            "    --reproduce-tid=filename       tid file to reproduce the problem" << endl <<
            "    --reproduce-usage=filename     stmt usage file to reproduce the problem" << endl <<
            "    --min                  minimize the reproduce test case (should be used with --reproduce-sql, --reproduce-tid, and --reproduce-usage)" << endl <<
            "    --check-txn-cycle      check whether the test case has transactional cycles (should be used with --reproduce-sql, --reproduce-tid, and --reproduce-usage)" << endl <<
            "    --txn-decycle          perform transactional decycling, and check whether still trigger the bug (should be used with --reproduce-sql, --reproduce-tid, and --reproduce-usage)" << endl <<
            "    --check-topo-sort      check whether all topological sorting results can trigger the bug (should be used with --reproduce-sql, --reproduce-tid, and --reproduce-usage)" << endl <<
            "    --help                 print available command line options and exit" << endl;
        return 0;
    } else if (options.count("version")) {
        return 0;
    }

    // set timeout action
    struct sigaction action;  
    memset(&action, 0, sizeof(action));  
    sigemptyset(&action.sa_mask);  
    action.sa_flags = 0;  
    action.sa_handler = user_signal;  
    if (sigaction(SIGUSR1, &action, NULL)) {
        cerr << "sigaction error" << endl;
        exit(1);
    }

    // set timeout action for fork
    struct sigaction sa;  
    memset(&sa, 0, sizeof(sa));  
    sigemptyset(&sa.sa_mask);  
    sa.sa_flags = SA_RESTART; 
    sa.sa_handler = kill_process_signal;  
    if (sigaction(SIGALRM, &sa, NULL)) {
        cerr << "sigaction error" << endl;
        exit(1);
    }

    dbms_info d_info(options);

    cerr << "-------------Test Info------------" << endl;
    cerr << "Test DBMS: " << d_info.dbms_name << endl;
    cerr << "Test database: " << d_info.test_db << endl;
    cerr << "Test port: " << d_info.test_port << endl;
    cerr << "Can trigger error in transaction: " << d_info.can_trigger_error_in_txn << endl;
    cerr << "Output or affect num: " << d_info.ouput_or_affect_num << endl;
    cerr << "----------------------------------" << endl;

    if (options.count("random-seed")) {
        auto random_file_name = options["random-seed"];
        file_random_machine::use_file(random_file_name);
        vector<string> output_rec;
        cerr << "generating database ... ";
        generate_database(d_info);
        cerr << "done" << endl;
        
        transaction_test tt(d_info);
        
        cerr << "generating transactions and testing ...";
        tt.crash_test();
        cerr << "done" << endl;
        
        save_backup_file("./", d_info);
        tt.save_test_case("./");
        return 0;
    }

    if (options.count("reproduce-sql")) {
        cerr << "enter reproduce mode" << endl;
        if (!options.count("reproduce-tid")) {
            cerr << "should also provide tid file" << endl;
            return 0;
        }

        // get stmt queue
        vector<shared_ptr<prod>> stmt_queue;
        ifstream stmt_file(options["reproduce-sql"]);
        stringstream buffer;
        buffer << stmt_file.rdbuf();
        stmt_file.close();
        
        string stmts(buffer.str());
        int old_off = 0;
        while (1) {
            int new_off = stmts.find(";\n\n", old_off);
            if (new_off == string::npos) 
                break;
            
            auto each_sql = stmts.substr(old_off, new_off - old_off); // not include ;\n\n
            old_off = new_off + string(";\n\n").size();

            stmt_queue.push_back(make_shared<txn_string_stmt>((prod *)0, each_sql));
        }
	    
        // get tid queue
        vector<int> tid_queue;
        ifstream tid_file(options["reproduce-tid"]);
        int tid;
        int max_tid = -1;
        while (tid_file >> tid) {
            tid_queue.push_back(tid);
            if (tid > max_tid)
                max_tid = tid;
        }
        tid_file.close();

        // get stmt use queue
        vector<stmt_usage> stmt_usage_queue;
        ifstream stmt_usage_file(options["reproduce-usage"]);
        int use;
        while (stmt_usage_file >> use) {
            switch (use) {
            case 0:
                stmt_usage_queue.push_back(stmt_usage(INIT_TYPE, false, "t_***"));
                break;
            case 1:
                stmt_usage_queue.push_back(stmt_usage(SELECT_READ, false, "t_***"));
                break;
            case 2:
                stmt_usage_queue.push_back(stmt_usage(UPDATE_WRITE, false, "t_***"));
                break;
            case 3:
                stmt_usage_queue.push_back(stmt_usage(INSERT_WRITE, false, "t_***"));
                break;
            case 4:
                stmt_usage_queue.push_back(stmt_usage(DELETE_WRITE, false, "t_***"));
                break;
            case 5:
                stmt_usage_queue.push_back(stmt_usage(BEFORE_WRITE_READ, true, "t_***"));
                break;
            case 6:
                stmt_usage_queue.push_back(stmt_usage(AFTER_WRITE_READ, true, "t_***"));
                break;
            case 7:
                stmt_usage_queue.push_back(stmt_usage(VERSION_SET_READ, true, "t_***"));
                break;
            default:
                cerr << "unknown stmt usage: " << use << endl;
                exit(-1);
                break;
            }
        }
        stmt_usage_file.close();

        if (options.count("min"))
            minimize_testcase(d_info, stmt_queue, tid_queue, stmt_usage_queue);
        else if (options.count("check-txn-cycle"))
            check_txn_cycle(d_info, stmt_queue, tid_queue, stmt_usage_queue);
        else if (options.count("txn-decycle")) {
            int succeed_time = 0;
            int all_time = 0;
            vector<int> delete_nodes;
            txn_decycle_test(d_info, stmt_queue, tid_queue, stmt_usage_queue, succeed_time, all_time, delete_nodes);
            cerr << "succeed time: " << succeed_time << endl;
            cerr << "all time: " << all_time << endl;
        }
        else if (options.count("check-topo-sort")) {
            int succeed_time = 0;
            int all_time = 0;
            check_topo_sort(d_info, stmt_queue, tid_queue, stmt_usage_queue, succeed_time, all_time);
            cerr << "succeed time: " << succeed_time << endl;
            cerr << "all time: " << all_time << endl;
        }
        else {
            string empty_str;
            reproduce_routine(d_info, stmt_queue, tid_queue, stmt_usage_queue, empty_str);
        }
            
        return 0;
    }
    
    while (1) {
        random_test(d_info);
    }

    return 0;
}
