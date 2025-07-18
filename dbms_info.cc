#include "dbms_info.hh"

dbms_info::dbms_info(map<string,string>& options)
{
    if (false) {}
    #ifdef HAVE_LIBSQLITE3
    else if (options.count("sqlite")) {
        dbms_name = "sqlite";
        test_port = 0; // no port
        test_db = options["sqlite"];
        can_trigger_error_in_txn = true;
    }
    #endif

    #ifdef HAVE_LIBMYSQLCLIENT
    else if (options.count("tidb-db") && options.count("tidb-port")) {
        dbms_name = "tidb";
        test_port = stoi(options["tidb-port"]);
        test_db = options["tidb-db"];
        can_trigger_error_in_txn = true;
    }
    else if (options.count("mysql-db") && options.count("mysql-port")) {
        dbms_name = "mysql";
        test_port = stoi(options["mysql-port"]);
        test_db = options["mysql-db"];
        can_trigger_error_in_txn = true;
    }
    else if (options.count("oceanbase-db") && options.count("oceanbase-port") && options.count("oceanbase-host")) {
        dbms_name = "oceanbase";
        test_port = stoi(options["oceanbase-port"]);
        test_db = options["oceanbase-db"];
        host_addr = options["oceanbase-host"];
        can_trigger_error_in_txn = true;
    }
    #endif

    else if (options.count("clickhouse-db") && options.count("clickhouse-port")) {
        dbms_name = "clickhouse";
        test_port = stoi(options["clickhouse-port"]);
        test_db = options["clickhouse-db"];
        can_trigger_error_in_txn = false;
    }
    else if (options.count("postgres-db") && options.count("postgres-port")) {
        dbms_name = "postgres";
        test_port = stoi(options["postgres-port"]);
        test_db = options["postgres-db"];
        inst_path = options.count("postgres-path") ? options["postgres-path"] : "/usr/local/pgsql";
        can_trigger_error_in_txn = false;
    }
    else if (options.count("yugabyte-db") &&
                    options.count("yugabyte-port") &&
                    options.count("yugabyte-host")) {
        dbms_name = "yugabyte";
        test_port = stoi(options["yugabyte-port"]);
        test_db = options["yugabyte-db"];
        host_addr = options["yugabyte-host"];
        can_trigger_error_in_txn = false;
    }
    else if (options.count("cockroach-db") &&
                    options.count("cockroach-port") &&
                    options.count("cockroach-host")) {
        dbms_name = "cockroach";
        test_port = stoi(options["cockroach-port"]);
        test_db = options["cockroach-db"];
        host_addr = options["cockroach-host"];
        can_trigger_error_in_txn = false;
    }
    else {
        cerr << "Sorry,  you should specify a dbms and its database, or your dbms is not supported, or you miss arguments" << endl;
        throw runtime_error("Does not define target dbms and db in dbms_info::dbms_info()");
    }

    if (options.count("output-or-affect-num"))
        ouput_or_affect_num = stoi(options["output-or-affect-num"]);
    else
        ouput_or_affect_num = 0;

    return;
}