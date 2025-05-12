#include "config.h"
#include <map>
#include <iostream>
#include "dbms_info.hh"
#include "general_process.hh"
#include "qcn_tester/qcn_tester.hh"
#include "qcn_tester/qcn_select_tester.hh"
#include "qcn_tester/qcn_update_tester.hh"
#include "qcn_tester/qcn_delete_tester.hh"
#include "qcn_tester/qcn_cte_tester.hh"
#include "qcn_tester/qcn_insert_select_tester.hh"

#include <thread>
#include <chrono>

#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

using namespace std;

#define DEFAULT_DB_TEST_TIME 50
// #define DB_RECORD_FILE "db_setup.sql"

unsigned long long test_start_timestamp_ms = 0;
unsigned long long dbms_execution_ms = 0;
int executed_test_num = 0;

void print_test_time_info() {
    auto now = get_cur_time_ms();
    double test_time_s = 1.0 * (now - test_start_timestamp_ms) / 1000;
    cout << "testing time: " << test_time_s << "s" << endl;
    double dbms_execution_s = 1.0 * dbms_execution_ms / 1000;
    cout << "DBMS execution time: " << dbms_execution_s << "s" << endl;
    double execution_percentage = 1.0 * dbms_execution_s / test_time_s;
    cout << "execution percentage: " <<  execution_percentage * 100 << "%" << endl;
    cout << "test throughput: " << 1.0 * executed_test_num / test_time_s << " tests/sec" << endl;
    return;
}

void print_output_to_file(multiset<vector<string>> &output, string filename)
{
    ofstream ofile(filename);
    for (auto& row : output) {
        for (auto& str : row) {
            ofile << str << " ";
        }
        ofile << "\n";
    }
    ofile.close();
}

void minimize_qcn_database(shared_ptr<qcn_tester> qcn, 
                            string input_db_record_file, 
                            string output_db_record_file,
                            multiset<vector<string>>* min_origin_output,
                            multiset<vector<string>>* min_qit_output)
{
    vector<string> stmt_queue;
    ifstream stmt_file(input_db_record_file);
    stringstream buffer;
    buffer << stmt_file.rdbuf();
    stmt_file.close();
    auto tmp_ignore_crash = qcn->ignore_crash;
    qcn->ignore_crash = true;

    string stmts(buffer.str());
    int old_off = 0;
    string seperate_label = ";\n";
    while (1) {
        auto new_off = stmts.find(seperate_label, old_off);
        if (new_off == string::npos)
            break;
        
        auto each_sql = stmts.substr(old_off, new_off - old_off); // not include the seperate_label
        old_off = new_off + seperate_label.size();

        stmt_queue.push_back(each_sql + ";");
    }

    for (int i = stmt_queue.size() - 1; i >= 0; i--) {
        // remove one stmt
        auto removed_stmt = stmt_queue[i];
        stmt_queue.erase(stmt_queue.begin() + i);
        cout << "-----------------" << endl;
        cout << "trying to remove stmt: " << removed_stmt << endl;
        
        shared_ptr<dut_base> dut;
        while (1) {
            try {
                dut = dut_setup(qcn->tested_dbms_info);
                dut->reset();
                break;
            } catch (exception& e) { // server might be crashed by other test process
                cerr << "error: " << e.what() << endl;
                chrono::minutes duration(1);
                this_thread::sleep_for(duration);
                continue;
            }
        }
        
        // feed the database with stmts
        int new_stmt_num = stmt_queue.size();
        bool trigger_error = false;
        for (int j = 0; j < new_stmt_num; j++) {
            try {
                dut->test(stmt_queue[j]);
            } catch (exception& e) {
                trigger_error = true;
                break;
            }
        }
        if (trigger_error) {
            // trigger error, add the stmt back
            stmt_queue.insert(stmt_queue.begin() + i, removed_stmt);
            continue;
        }

        while (1) {
            try {
                dut->backup();
                break;
            } catch (exception& e) { // server might be crashed by other test process
                cerr << "error: " << e.what() << endl;
                chrono::minutes duration(1);
                this_thread::sleep_for(duration);
                continue;
            }
        }

        // check whether the bug still exists
        bool no_trigger_bug = true;
        trigger_error = false;
        try {
            no_trigger_bug = qcn->qcn_test_without_initialization();
        } catch (exception& e) {
            trigger_error = true;
            continue;
        }

        if (trigger_error == false && no_trigger_bug == false) {
            cout << "successfully remove the stmt" << endl;
            if (min_origin_output)
                *min_origin_output = qcn->original_query_result;
            if (min_qit_output)
                *min_qit_output = qcn->qit_query_result;
            cout << "-----------------" << endl;
            continue;
        }
        
        // bug disapper, add the stmt back
        stmt_queue.insert(stmt_queue.begin() + i, removed_stmt);
    }

    // save the minimized database file
    ofstream new_stmt_file(output_db_record_file);
    for (int i = 0; i < stmt_queue.size(); i++) {
        new_stmt_file << stmt_queue[i] << endl;
    }
    new_stmt_file.close();

    qcn->ignore_crash = tmp_ignore_crash;

    return;
}

int cpu_affinity = -1;

int main(int argc, char *argv[]) {
    test_start_timestamp_ms = get_cur_time_ms();
    
    // pipefd for process communication
    int pipefd[2];
    if (pipe(pipefd) == -1) {
        perror("pipe");
        exit(EXIT_FAILURE);
    }

    // analyze the options
    map<string,string> options;
    regex optregex("--\
(help|db-test-num|seed|cpu-affinity|\
ignore-crash|\
sqlite|\
postgres-db|postgres-port|\
cockroach-db|cockroach-port|cockroach-host|\
yugabyte-db|yugabyte-port|yugabyte-host|\
clickhouse-db|clickhouse-port|\
tidb-db|tidb-port|\
mysql-db|mysql-port|\
oceanbase-db|oceanbase-port|oceanbase-host)(?:=((?:.|\n)*))?");
    
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
        "    --postgres-db=connstr  Postgres database to send queries to, should used with --postgres-port" <<endl <<
        "    --postgres-port=int    Postgres server port number, should used with --postgres-db" <<endl <<
#ifdef HAVE_LIBSQLITE3
        "    --sqlite=URI           SQLite database to send queries to" << endl <<
#endif
#ifdef HAVE_LIBMYSQLCLIENT
        "    --mysql-db=constr      MySQL database to send queries to, should be used with mysql-port" << endl << 
        "    --mysql-port=int       MySQL server port number, should be used with mysql-db" << endl << 
        "    --tidb-db=constr   tidb database name to send queries to (should used with" << endl << 
        "    --tidb-port=int    tidb server port number" << endl << 
        "    --oceanbase-db=constr      OceanBase database name to send queries to " << endl << 
        "    --oceanbase-port=int       OceanBase server port number" << endl << 
        "    --oceanbase-host=constr    OceanBase server host address" << endl <<
#endif
        "    --clickhouse-db=constr    ClickHouse tested database" << endl << 
        "    --clickhouse-port=int     ClickHouse server port number" << endl <<
        "    --cockroach-db=constr       CockroachDB tested database" << endl <<
        "    --cockroach-port=int        CockroachDB server port number" << endl <<
        "    --cockroach-host=constr     CockroachDB server host address" << endl <<
        "    --yugabyte-db=constr       YugaByte tested database" << endl <<
        "    --yugabyte-port=int        YugaByte server port number" << endl <<
        "    --yugabyte-host=constr     YugaByte server host address" << endl <<
        "    --db-test-num=int      number of qcn tests for each generated database" << endl <<
        "    --seed=int             seed RNG with specified int instead of PID" << endl <<
        "    --cpu-affinity=int     set cpu affinity of qcn and its child process to specific CPU" << endl << 
        "    --ignore-crash         ignore crash bug, the fuzzer will not stop when it finds crash issues" << endl << 
        "    --help                 print available command line options and exit" << endl;
        return 0;
    }
    
    dbms_info d_info(options);
    cerr << "-------------Test Info------------" << endl;
    cerr << "Test DBMS: " << d_info.dbms_name << endl;
    cerr << "Test database: " << d_info.test_db << endl;
    cerr << "Test port: " << d_info.test_port << endl;
    cerr << "Test host: " << d_info.host_addr << endl;
    cerr << "----------------------------------" << endl;

    int db_test_time = DEFAULT_DB_TEST_TIME;
    if (options.count("db-test-num") > 0)
        db_test_time = stoi(options["db-test-num"]);
    
    cpu_affinity = -1;
    if (options.count("cpu-affinity") > 0)
        cpu_affinity = stoi(options["cpu-affinity"]);
    
    if (cpu_affinity >= 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset); // clear the CPU set
        CPU_SET(cpu_affinity, &cpuset);

        if (sched_setaffinity(0, sizeof(cpuset), &cpuset) < 0) {
            cerr << "Failed to set CPU affinity" << endl;
            exit(EXIT_FAILURE);
        }
    }
    
    int round = 0;
    while (1) {
        cerr << "round " << round << " ... " << endl;
        round++;
        auto qcn_test_pid = fork();
        if (qcn_test_pid == 0) {
            close(pipefd[0]);  // Close unused read end

            cerr << "running on CPU core " << sched_getcpu() << " ... " << endl;

            // seed must set in the child process, otherwise the child always generate the same statement
            // smith::rng.seed(options.count("seed") ? stoi(options["seed"]) : time(NULL));
            random_device rd;
            auto rand_seed = rd();
            // rand_seed = 212331246;
            cerr << "random seed: " << rand_seed << " ... " << endl;
            smith::rng.seed(rand_seed);

            // don't need to restart server
            // fork_if_server_closed(d_info);
            while (true) {
                try {
                    generate_database(d_info);
                    break;
                } catch (exception &e) { // if fails, just try again
                    string err = e.what();
                    bool expected = (err.find("expected error") != string::npos) || (err.find("timeout") != string::npos);
                    if (!expected) {
                        cerr << "unexpected error generated from generate_database: " << err << endl;
                        // cerr << "cannot save backup as generating db is not finished"<< endl;
                        abort(); // stop if trigger unexpected error
                    }
                    rand_seed = rd();
                    cerr << "random seed: " << rand_seed << " ... " << endl;
                    smith::rng.seed(rand_seed);
                    continue;
                }
            }
            cerr << "have generated db ... " << endl;
            print_test_time_info();

            auto db_schema = get_schema(d_info);
            for (int i = 0; i < db_test_time; i++) {
                cerr << "[" << i << "]" << endl;
                shared_ptr<qcn_tester> qcn;
                int choices;
                if (db_schema->target_dbms == "clickhouse")
                    choices = 6; // clickhouse'update and delete sometimes undetermined
                else
                    choices = 12;

                auto choice = dx(choices);
                if (choice <= 3) {
                    cerr << "qcn_select_tester" << endl;
                    qcn = make_shared<qcn_select_tester>(d_info, db_schema);
                }
                else if (choice <= 6) {
                    cerr << "qcn_cte_tester" << endl;
                    qcn = make_shared<qcn_cte_tester>(d_info, db_schema);
                }
                else if (choice <= 9) {
                    cerr << "qcn_update_tester" << endl;
                    qcn = make_shared<qcn_update_tester>(d_info, db_schema);
                }
                else {
                // else if (choice <= 12) {
                    cerr << "qcn_delete_tester" << endl;
                    qcn = make_shared<qcn_delete_tester>(d_info, db_schema);
                }
                if (options.count("ignore-crash") > 0) {
                    qcn->ignore_crash = true;
                }

                cerr << "start test" << endl;
                if (qcn->qcn_test() == false) {
                    cerr << "LOGIC BUG!!! DBMS produces incorrect results" << endl;
                    save_backup_file(".", d_info);
                    qcn->save_testcase("origin");
                    cerr << "minimizing ..." << endl << endl;
                    qcn->minimize_testcase();
                    cerr << endl << endl << "done" << endl;

                    qcn->qcn_test_without_initialization(); // get latest results
                    qcn->save_testcase("minimized");

                    // cerr << "origin output:" << endl;
                    // qcn->print_stmt_output(qcn->original_query_result);
                    // cerr << "qit output:" << endl;
                    // qcn->print_stmt_output(qcn->qit_query_result);
                    // qcn->print_origin_qit_difference();

                    // reduce database
                    multiset<vector<string>> min_origin_result = qcn->original_query_result;
                    multiset<vector<string>> min_qit_result = qcn->qit_query_result;
                    minimize_qcn_database(qcn, DB_RECORD_FILE, 
                                            "minimized/" + string(DB_RECORD_FILE), 
                                            &min_origin_result,
                                            &min_qit_result);
                    print_output_to_file(min_origin_result, "minimized/origin.out");
                    print_output_to_file(min_qit_result, "minimized/qit.out");
                    exit(EXIT_FAILURE);
                }
                executed_test_num ++;
                print_test_time_info();
                // // used for checking statement results
                // cerr << "qit_query_result: " << endl;
                // for (auto& row : qcn->qit_query_result) {
                //     for (auto& item : row)
                //         cerr << item << " ";
                //     cerr << endl;
                // }
                // exit(EXIT_FAILURE);
            }
            ssize_t bytes_written = write(pipefd[1], &dbms_execution_ms, sizeof(dbms_execution_ms));
            if (bytes_written == -1) {
                perror("write");
                exit(EXIT_FAILURE);
            } else if (bytes_written != sizeof(dbms_execution_ms)) {
                std::cerr << "Not all bytes written. Expected: " << sizeof(dbms_execution_ms) << ", Written: " << bytes_written << std::endl;
                exit(EXIT_FAILURE);
            }
            close(pipefd[1]);
            exit(EXIT_SUCCESS);
        }
        // close(pipefd[1]);  // Close unused write end
        int status;
        auto res = waitpid(qcn_test_pid, &status, 0);
        if (res <= 0) {
            cerr << "main waitpid() fail: " <<  res << endl;
            abort();
        }
        if (WIFEXITED(status) && WEXITSTATUS(status) == EXIT_FAILURE) {
            cerr << "trigger a logic bug!! abort" << endl;
            abort();
        }
        // ignore memory issue currently
        if (WIFSIGNALED(status)) {
            cerr << "trigger a memory bug!! the server might be crashed" << endl;
            auto signal_num = WTERMSIG(status);
            cerr << "signal: " << signal_num << endl;
            cerr << "signal info: " << strsignal(signal_num) << endl;
            if (options.count("ignore-crash") > 0) {
                cerr << "option [ignore-crash] enabled, so sleep 1 min (wait for recovering the server) and skip this bug" << endl;
                chrono::minutes duration(1);
                this_thread::sleep_for(duration);
                cerr << "sleep over, restart testing" << endl;
            }
            else
                abort();
        }
        ssize_t bytes_read = read(pipefd[0], &dbms_execution_ms, sizeof(dbms_execution_ms));
        if (bytes_read == -1) {
            perror("read");
            exit(EXIT_FAILURE);
        } else if (bytes_read != sizeof(dbms_execution_ms)) {
            std::cerr << "Not all bytes read. Expected: " << sizeof(dbms_execution_ms) << ", Read: " << bytes_read << std::endl;
            exit(EXIT_FAILURE);
        }
        // close(pipefd[0]);
        executed_test_num = executed_test_num + db_test_time;
        print_test_time_info();
        cerr << "done" << endl;
    }
    
    return 0;
}
