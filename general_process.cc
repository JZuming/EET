#include "general_process.hh"

#define DB_RECORD_FILE "db_setup.sql"

extern int write_op_id;
extern unsigned long long dbms_execution_ms;

int make_dir_error_exit(string& folder)
{
    cerr << "try to mkdir " << folder << endl;
    int fail_time = 0;
    while (mkdir(folder.c_str(), 0700)) {
        cout << "fail to mkdir "<< folder << endl;
        if (folder.length() < 2)
            return 1;
        folder = folder.substr(0, folder.length() - 1) + "_tmp/";
        fail_time++;
        if (fail_time > 5)
            return 1;
    }
    cerr << "finally mkdir " << folder << endl;
    return 0;
}

shared_ptr<schema> get_schema(dbms_info& d_info)
{
    shared_ptr<schema> schema;
    static int try_time = 0;

    auto schema_start = get_cur_time_ms();
    try {
        if (false) {}
        #ifdef HAVE_SQLITE
        else if (d_info.dbms_name == "sqlite") 
            schema = make_shared<schema_sqlite>(d_info.test_db, true);
        #endif

        #ifdef HAVE_LIBMYSQLCLIENT
        #ifdef HAVE_MYSQL
        else if (d_info.dbms_name == "mysql") 
            schema = make_shared<schema_mysql>(d_info.test_db, d_info.test_port);
        #endif
        #ifdef HAVE_MARIADB
        else if (d_info.dbms_name == "mariadb") 
            schema = make_shared<schema_mariadb>(d_info.test_db, d_info.test_port);
        #endif
        #ifdef HAVE_OCEANBASE
        else if (d_info.dbms_name == "oceanbase") 
            schema = make_shared<schema_oceanbase>(d_info.test_db, d_info.test_port);
        #endif
        #ifdef HAVE_TIDB
        else if (d_info.dbms_name == "tidb") 
            schema = make_shared<schema_tidb>(d_info.test_db, d_info.test_port);
        #endif
        #endif

        #ifdef HAVE_MONETDB
        else if (d_info.dbms_name == "monetdb") 
            schema = make_shared<schema_monetdb>(d_info.test_db, d_info.test_port);
        #endif

        #ifdef HAVE_COCKROACH
        else if (d_info.dbms_name == "cockroach")
            schema = make_shared<schema_cockroachdb>(d_info.test_db, d_info.test_port);
        #endif
        
        else if (d_info.dbms_name == "postgres")
            schema = make_shared<schema_pqxx>(d_info.test_db, d_info.test_port, true);
        else if (d_info.dbms_name == "clickhouse")
            schema = make_shared<schema_clickhouse>(d_info.test_db, d_info.test_port);
        else if (d_info.dbms_name == "yugabyte")
            schema = make_shared<schema_yugabyte>(d_info.test_db, d_info.test_port, d_info.host_addr, true);
        else if (d_info.dbms_name == "cockroach")
            schema = make_shared<schema_cockroach>(d_info.test_db, d_info.test_port, d_info.host_addr, true);
        else {
            cerr << d_info.dbms_name << " is not supported yet in get_schema()" << endl;
            throw runtime_error("Unsupported DBMS in get_schema()");
        }
        dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - schema_start);

    } catch (exception &e) { // may occur occastional error
        dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - schema_start);
        
        string err = e.what();
        bool expected = (err.find("expected error") != string::npos) || (err.find("timeout") != string::npos);
        if (!expected) {
            cerr << "unexpected error in get_schema: " << err << endl;
            cerr << "cannot save test case in get_schema"<< endl;
            abort(); // stop if trigger unexpected error
        }
        if (try_time >= 8) {
            cerr << "Fail in get_schema() " << try_time << " times, return" << endl;
	        throw;
        }
        try_time++;
        schema = get_schema(d_info);
        try_time--;
        return schema;
    }
    return schema;
}

shared_ptr<dut_base> dut_setup(dbms_info& d_info)
{
    shared_ptr<dut_base> dut;
    if (false) {}
    #ifdef HAVE_SQLITE
    else if (d_info.dbms_name == "sqlite")
        dut = make_shared<dut_sqlite>(d_info.test_db);
    #endif

    #ifdef HAVE_LIBMYSQLCLIENT
    #ifdef HAVE_MYSQL
    else if (d_info.dbms_name == "mysql")
        dut = make_shared<dut_mysql>(d_info.test_db, d_info.test_port);
    #endif
    #ifdef HAVE_MARIADB
    else if (d_info.dbms_name == "mariadb")
        dut = make_shared<dut_mariadb>(d_info.test_db, d_info.test_port);
    #endif
    #ifdef HAVE_OCEANBASE
    else if (d_info.dbms_name == "oceanbase")
        dut = make_shared<dut_oceanbase>(d_info.test_db, d_info.test_port);
    #endif
    #ifdef HAVE_TIDB
    else if (d_info.dbms_name == "tidb")
        dut = make_shared<dut_tidb>(d_info.test_db, d_info.test_port);
    #endif
    #endif

    #ifdef HAVE_MONETDB
    else if (d_info.dbms_name == "monetdb")
        dut = make_shared<dut_monetdb>(d_info.test_db, d_info.test_port);
    #endif

    #ifdef HAVE_COCKROACH
    else if (d_info.dbms_name == "cockroach")
        dut = make_shared<dut_cockroachdb>(d_info.test_db, d_info.test_port);
    #endif
    else if (d_info.dbms_name == "clickhouse")
        dut = make_shared<dut_clickhouse>(d_info.test_db, d_info.test_port, DB_RECORD_FILE);
    else if (d_info.dbms_name == "postgres")
        dut = make_shared<dut_libpq>(d_info.test_db, d_info.test_port);
    else if (d_info.dbms_name == "yugabyte")
        dut = make_shared<dut_yugabyte>(d_info.test_db, d_info.test_port, d_info.host_addr);
    else if (d_info.dbms_name == "cockroach")
        dut = make_shared<dut_cockroach>(d_info.test_db, d_info.test_port, d_info.host_addr);
    else {
        cerr << d_info.dbms_name << " is not installed, or it is not supported yet in dut_setup()" << endl;
        throw runtime_error("Unsupported DBMS in dut_setup()");
    }

    return dut;
}

void save_query(string dir, string filename, string& query)
{
    ofstream s(dir + "/" + filename);
    s << query << ";" << endl;
    s.close();
}

int save_backup_file(string path, dbms_info& d_info)
{
    if (false) {}
    #ifdef HAVE_SQLITE
    else if (d_info.dbms_name == "sqlite")
        return dut_sqlite::save_backup_file(path, d_info.test_db);
    #endif

    #ifdef HAVE_MYSQL
    else if (d_info.dbms_name == "mysql")
        return dut_mysql::save_backup_file(d_info.test_db, path);
    #endif
    #ifdef HAVE_MARIADB
    else if (d_info.dbms_name == "mariadb")
        return dut_mariadb::save_backup_file(path);
    #endif
    #ifdef HAVE_OCEANBASE
    else if (d_info.dbms_name == "oceanbase")
        return dut_oceanbase::save_backup_file(path);
    #endif
    #ifdef HAVE_TIDB
    else if (d_info.dbms_name == "tidb") {
        return dut_tidb::save_backup_file(d_info.test_db, path);
    }
    #endif

    #ifdef HAVE_MONETDB
    else if (d_info.dbms_name == "monetdb")
        return dut_monetdb::save_backup_file(path);
    #endif

    #ifdef HAVE_COCKROACH
    else if (d_info.dbms_name == "cockroach")
        return dut_cockroachdb::save_backup_file(path);
    #endif

    else if (d_info.dbms_name == "postgres")
        return dut_libpq::save_backup_file(d_info.test_db, path);
    else if (d_info.dbms_name == "yugabyte")
        return dut_yugabyte::save_backup_file(d_info.test_db, path);
    else if (d_info.dbms_name == "cockroach")
        return dut_cockroach::save_backup_file(d_info.test_db, path);
    else if (d_info.dbms_name == "clickhouse") {
        string cmd = "cp " + string(DB_RECORD_FILE) + " " + path;
        return system(cmd.c_str());
    }
    else {
        cerr << d_info.dbms_name << " is not supported yet in save_backup_file()" << endl;
        throw runtime_error("Unsupported DBMS in save_backup_file()");
    }
}

pid_t fork_db_server(dbms_info& d_info)
{
    pid_t fork_pid = -1;
    if (false) {}
    #ifdef HAVE_SQLITE
    else if (d_info.dbms_name == "sqlite")
        fork_pid = 0;
    #endif
    
    #ifdef HAVE_LIBMYSQLCLIENT
    #ifdef HAVE_MYSQL
    else if (d_info.dbms_name == "mysql")
        fork_pid = dut_mysql::fork_db_server();
    #endif
    #ifdef HAVE_MARIADB
    else if (d_info.dbms_name == "mariadb")
        fork_pid = dut_mariadb::fork_db_server();
    #endif
    #ifdef HAVE_OCEANBASE
    else if (d_info.dbms_name == "oceanbase")
        fork_pid = dut_oceanbase::fork_db_server();
    #endif
    #ifdef HAVE_TIDB
    else if (d_info.dbms_name == "tidb")
        fork_pid = dut_tidb::fork_db_server();
    #endif
    #endif

    #ifdef HAVE_MONETDB
    else if (d_info.dbms_name == "monetdb")
        fork_pid = dut_monetdb::fork_db_server();
    #endif

    #ifdef HAVE_COCKROACH
    else if (d_info.dbms_name == "cockroach")
        fork_pid = dut_cockroachdb::fork_db_server();
    #endif

    else if (d_info.dbms_name == "postgres") {
        // Do nothing, because the server crash means there is a bug
        // fork_pid = dut_libpq::fork_db_server();
    }
    else if (d_info.dbms_name == "yugabyte") {
        // Do nothing, because the server crash means there is a bug
        // fork_pid = dut_libpq::fork_db_server();
    }
    else if (d_info.dbms_name == "cockroach") {
        // Do nothing, because the server crash means there is a bug
        // fork_pid = dut_libpq::fork_db_server();
    }
    else {
        cerr << d_info.dbms_name << " is not supported yet in fork_db_server()" << endl;
        throw runtime_error("Unsupported DBMS in fork_db_server()");
    }

    return fork_pid;
}

void user_signal(int signal)  
{  
    if(signal != SIGUSR1) {  
        printf("unexpect signal %d\n", signal);  
        exit(1);  
    }  
     
    cerr << "get SIGUSR1, stop the thread" << endl;
    pthread_exit(0);
}

void dut_reset(dbms_info& d_info)
{
    auto reset_start = get_cur_time_ms();
    try {
        auto dut = dut_setup(d_info);
        dut->reset();
        dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - reset_start);
    } catch (exception &e) {
        dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - reset_start);
        throw;
    }
}

void dut_backup(dbms_info& d_info)
{
    auto backup_start = get_cur_time_ms();
    try {
        auto dut = dut_setup(d_info);
        dut->backup();
        dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - backup_start);
    } catch (exception &e) {
        dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - backup_start);
        throw;
    }
}

void dut_reset_to_backup(dbms_info& d_info)
{
    auto dut = dut_setup(d_info);
    dut->reset_to_backup();
}

void dut_get_content(dbms_info& d_info, 
                    map<string, vector<vector<string>>>& content)
{
    vector<string> table_names;
    auto schema = get_schema(d_info);
    for (auto& table:schema->tables)
        table_names.push_back(table.ident());
    
    content.clear();
    auto dut = dut_setup(d_info);
    dut->get_content(table_names, content);
}

void interect_test(dbms_info& d_info, 
                    shared_ptr<prod> (* tmp_statement_factory)(scope *), 
                    bool need_affect,
                    string record_file)
{
    auto schema = get_schema(d_info);
    scope scope;
    schema->fill_scope(scope);
    
    shared_ptr<prod> gen = tmp_statement_factory(&scope);
    ostringstream s;
    gen->out(s);
    auto sql = s.str() + ";";

    static int try_time = 0;
    auto test_start = get_cur_time_ms();
    try {
        auto dut = dut_setup(d_info);    
        int affect_num = 0;
        dut->test(sql, NULL, &affect_num);
        
        if (need_affect && affect_num <= 0)
            throw runtime_error(string("expected error: affect result empty"));
        
        ofstream ofile(record_file, ios::app);
        ofile << sql << endl;
        ofile.close();
        dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - test_start);

    } catch(std::exception &e) { // ignore runtime error
        dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - test_start);
        
        string err = e.what();
        cerr << "err: " << e.what() << endl;
        bool expected = (err.find("expected error") != string::npos) || (err.find("timeout") != string::npos);
        if (!expected) {
            save_query(".", "unexpected.sql", sql);
            cerr << "unexpected error in interect_test: " << err << endl;
            cerr << "cannot save backup as generating db is not finished"<< endl;
            abort(); // stop if trigger unexpected error
        }
        if (try_time >= 8) {
            cerr << "Fail in interect_test() " << try_time << " times, return" << endl;
            cerr << "err: " << e.what() << endl;
            throw;
        }
        try_time++;
        interect_test(d_info, tmp_statement_factory, need_affect, record_file);
        try_time--;
    }
}

void normal_test(dbms_info& d_info, 
                    shared_ptr<schema>& schema, 
                    shared_ptr<prod> (* tmp_statement_factory)(scope *), 
                    bool need_affect,
                    string record_file)
{
    scope scope;
    schema->fill_scope(scope);

    shared_ptr<prod> gen = tmp_statement_factory(&scope);
    ostringstream s;
    gen->out(s);
    auto sql = s.str() + ";";

    static int try_time = 0;
    auto test_start = get_cur_time_ms();
    try {
        auto dut = dut_setup(d_info);
        int affect_num = 0;
        dut->test(sql, NULL, &affect_num);
        
        if (need_affect && affect_num <= 0)
            throw runtime_error(string("expected error: affect result empty"));
        
        ofstream ofile(record_file, ios::app);
        ofile << sql << endl;
        ofile.close();
        dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - test_start);

    } catch(std::exception &e) { // ignore runtime error
        dbms_execution_ms = dbms_execution_ms + (get_cur_time_ms() - test_start);

        string err = e.what();
        bool expected = (err.find("expected error") != string::npos) || (err.find("timeout") != string::npos);
        if (!expected) {
            save_query(".", "unexpected.sql", sql);
            cerr << "unexpected error in normal_test: " << err << endl;
            cerr << "cannot save backup as generating db is not finished"<< endl;
            abort(); // stop if trigger unexpected error
        }
        if (try_time >= 8) {
            cerr << "Fail in normal_test() " << try_time << " times, return" << endl;
            cerr << "SQL: " << sql << endl;
            cerr << "ERR: " << err << endl;
            throw;
        }
        try_time++;
        normal_test(d_info, schema, tmp_statement_factory, need_affect, record_file);
        try_time--;
    }
}

static size_t BKDRHash(const char *str, size_t hash)  
{
    while (size_t ch = (size_t)*str++)  {         
        hash = hash * 131 + ch;   // 也可以乘以31、131、1313、13131、131313..  
    }  
    return hash;  
}

static void hash_output_to_set(vector<vector<string>> &output, vector<size_t>& hash_set)
{
    size_t hash = 0;
    auto row_size = output.size();
    for (int i = 0; i < row_size; i++) {
        auto column_size = output[i].size();
        for (int j = 0; j < column_size; j++)
            hash = BKDRHash(output[i][j].c_str(), hash);
        hash_set.push_back(hash);
        hash = 0;
    }

    // sort the set, because some output order is random
    sort(hash_set.begin(), hash_set.end());
    return;
}

static void output_diff(string item_name, vector<vector<string>>& a_result, vector<vector<string>>& b_result)
{
    ofstream ofile("/tmp/comp_diff.txt", ios::app);
    ofile << "============================" << endl;
    ofile << "item name: " << item_name << endl;
    ofile << "A result: " << endl;
    for (auto& row_str : a_result) {
        for (auto& str : row_str)
            ofile << "    " << str;
    }
    ofile << endl;
    ofile << "B result: " << endl;
    for (auto& row_str : b_result) {
        for (auto& str : row_str)
            ofile << "    " << str;
    }
    ofile.close();
}

static bool is_number(const string &s) {
    if (s.empty() || s.length() <= 0) 
        return false;

    int point = 0;
    if (s.length() == 1 && (s[0] >'9' || s[0] < '0')) 
        return false;

    if(s.length() > 1) {
        if (s[0]!='.' && (s[0] >'9' || s[0] < '0')&&s[0]!='-' && s[0]!='+') 
            return false;
        
        if (s[0] == '.') 
            ++point;

        if ((s[0] == '+' || s[0] == '-') && (s[1] >'9' || s[1] < '0')) 
            return false;

        for (size_t i = 1; i < s.length(); ++i) {
            if (s[i]!='.' && (s[i] >'9' || s[i] < '0')) 
                return false;

            if (s[i] == '.') 
                ++point;
        }
    }

    if (point > 1) return false;
    
    return true;
}

static bool nomoalize_content(vector<vector<string>> &content)
{
    auto size = content.size();

    for (int i = 0; i < size; i++) {
        auto column_num = content[i].size();
        for (int j = 0; j < column_num; j++) {
            auto str = content[i][j];
            double value = 0;
            
            if (!is_number(str) || str.find(".") == string::npos)
                continue;

            // value is a float
            value = stod(str);
            value = round(value * 100) / 100; // keep 2 number after the point
            content[i][j] = to_string(value);
        }
    }
    return true;
}

bool compare_content(map<string, vector<vector<string>>>&a_content, 
                     map<string, vector<vector<string>>>&b_content)
{
    if (a_content.size() != b_content.size()) {
        cerr << "size not equal: " << a_content.size() << " " << b_content.size() << endl;
        return false;
    }
    
    for (auto iter = a_content.begin(); iter != a_content.begin(); iter++) {
        auto& table = iter->first;
        auto& con_table_content = iter->second;
        
        if (b_content.count(table) == 0) {
            cerr << "b_content does not have " << table << endl;
            return false;
        }

        auto& seq_table_content = b_content[table];

        nomoalize_content(con_table_content);
        nomoalize_content(seq_table_content);

        vector<size_t> con_table_set, seq_table_set;
        hash_output_to_set(con_table_content, con_table_set);
        hash_output_to_set(seq_table_content, seq_table_set);

        auto size = con_table_set.size();
        if (size != seq_table_set.size()) {
            cerr << "table " + table + " sizes are not equal" << endl;
            output_diff(table, con_table_content, seq_table_content);
            return false;
        }

        for (auto i = 0; i < size; i++) {
            if (con_table_set[i] != seq_table_set[i]) {
                cerr << "table " + table + " content are not equal" << endl;
                output_diff(table, con_table_content, seq_table_content);
                return false;
            }
        }
    }

    return true;
}

bool compare_output(vector<vector<vector<string>>>& a_output,
                    vector<vector<vector<string>>>& b_output)
{
    auto size = a_output.size();
    if (size != b_output.size()) {
        cerr << "stmt output sizes are not equel: "<< a_output.size() << " " << b_output.size() << endl;
        return false;
    }

    for (auto i = 0; i < size; i++) { // for each stmt
        auto& a_stmt_output = a_output[i];
        auto& b_stmt_output = b_output[i];
    
        nomoalize_content(a_stmt_output);
        nomoalize_content(b_stmt_output);
        
        vector<size_t> a_hash_set, b_hash_set;
        hash_output_to_set(a_stmt_output, a_hash_set);
        hash_output_to_set(b_stmt_output, b_hash_set);

        size_t stmt_output_size = a_hash_set.size();
        if (stmt_output_size != b_hash_set.size()) {
            cerr << "stmt[" << i << "] output sizes are not equel: " << a_hash_set.size() << " " << b_hash_set.size() << endl;
            output_diff("stmt["+ to_string(i) + "]", a_stmt_output, b_stmt_output);
            return false;
        }

        for (auto j = 0; j < stmt_output_size; j++) {
            if (a_hash_set[j] != b_hash_set[j]) {
                cerr << "stmt[" << i << "] output are not equel" << endl;
                output_diff("stmt["+ to_string(i) + "]", a_stmt_output, b_stmt_output);
                return false;
            }
        }
    }

    return true;
}

int generate_database(dbms_info& d_info)
{ 
    if (remove(DB_RECORD_FILE) != 0) {
        cerr << "generate_database: cannot remove file (" << DB_RECORD_FILE << ")" << endl;
    }
    
    dut_reset(d_info);

    auto ddl_stmt_num = d6() + 3; // at least 3 statements to create 3 tables
    for (auto i = 0; i < ddl_stmt_num; i++)
        interect_test(d_info, &ddl_statement_factory, false, DB_RECORD_FILE); // has disabled the not null, check and unique clause 
    
    auto basic_dml_stmt_num = 80 + d42(); // 80 - 122 inserted items
    auto schema = get_schema(d_info); // schema will not change in this stage
    for (auto i = 0; i < basic_dml_stmt_num; i++) 
        normal_test(d_info, schema, &basic_dml_statement_factory, true, DB_RECORD_FILE);

    dut_backup(d_info);

    return 0;
}

void gen_stmts_for_one_txn(shared_ptr<schema> &db_schema,
                        int trans_stmt_num,
                        vector<shared_ptr<prod>>& trans_rec,
                        dbms_info& d_info)
{
    auto can_error = d_info.can_trigger_error_in_txn;
    if (can_error == false || d_info.ouput_or_affect_num > 0)
        dut_reset_to_backup(d_info);
    
    vector<shared_ptr<prod>> all_tested_stmts; // if crash, report such statement
    scope scope;
    db_schema->fill_scope(scope);
    int stmt_num = 0;
    bool succeed = true;
    int fail_time = 0;
    int choice = -1;
    while (1) {
        if (succeed) 
            choice = d12();
        else { // if fail, do not change choice
            fail_time++;
            if (fail_time >= 8) {
                choice = d12();
                fail_time = 0;
            }
        }
        shared_ptr<prod> gen = txn_statement_factory(&scope, choice);
        succeed = false;

        ostringstream stmt_stream;
        gen->out(stmt_stream);
        auto stmt = stmt_stream.str() + ";";

        if (can_error == false || d_info.ouput_or_affect_num > 0) {
            try {
                auto dut = dut_setup(d_info);
                int affect_num = 0;
                vector<vector<string>> output;
                all_tested_stmts.push_back(gen);
                
                dut->test(stmt, &output, &affect_num);
                if (output.size() + affect_num < d_info.ouput_or_affect_num)
                    continue;
            } catch (exception &e) {
                string err = e.what();
                if (err.find("CONNECTION FAIL") != string::npos ||
                        err.find("BUG") != string::npos) {
                    
                    cerr << err << endl;
                    ofstream bug_file(NORMAL_BUG_FILE);
                    for (auto& stmt : all_tested_stmts) 
                        bug_file << print_stmt_to_string(stmt) << "\n" << endl;
                    bug_file.close();
                    throw;
                }
                // cerr << err << ", try again" << endl;
                // if (err.find("syntax") != string::npos && err.find("error") != string::npos) {
                //     cerr << RED << "The error statement: " << RESET << endl;
                //     cerr << stmt << endl;
                // }
                continue;
            }
        }
        trans_rec.push_back(gen);
        succeed = true;
        stmt_num++;
        if (stmt_num == trans_stmt_num)
            break;
    }
}

unsigned long long get_cur_time_ms() {
	struct timeval tv;
	struct timezone tz;

	gettimeofday(&tv, &tz);

	return (tv.tv_sec * 1000ULL) + tv.tv_usec / 1000;
}

pid_t server_process_id = 0xabcde;

void kill_server_process_with_SIGTERM()
{
    kill(server_process_id, SIGTERM);
    int ret;
    auto begin_time = get_cur_time_ms();
    while (1) {
        ret = kill(server_process_id, 0);
        if (ret != 0)
            break;
        
        auto now_time = get_cur_time_ms();
        if (now_time - begin_time > KILL_PROC_TIME_MS)
            break;
    }
}

// cannot be called by child process
bool try_to_kill_server()
{
    cerr << "try killing the server..." << endl;
    kill(server_process_id, SIGTERM);
    int ret;
    auto begin_time = get_cur_time_ms();
    bool flag = false;
    while (1) {
        ret = kill(server_process_id, 0);
        if (ret != 0) { // the process die
            flag = true;
            break;
        }

        int status;
        auto res = waitpid(server_process_id, &status, WNOHANG);
        if (res < 0) {
            if (errno == ECHILD) {
                cerr << "there is no child process to wait" << endl;
                flag = true;
                break;
            }
            else {
                cerr << "waitpid() fail: " <<  res << endl;
                throw runtime_error(string("waitpid() fail"));
            }
        }
        if (res == server_process_id) { // the dead process is collected
            cerr << "waitpid succeed for the server process !!!" << endl;
            flag = true;
            break;
        }

        auto now_time = get_cur_time_ms();
        if (now_time - begin_time > KILL_PROC_TIME_MS) {
            flag = false;
            break;
        }
    }
    return flag;
}

bool fork_if_server_closed(dbms_info& d_info)
{
    bool server_restart = false;
    auto time_begin = get_cur_time_ms();

    while (1) {
        try {
            auto dut = dut_setup(d_info);
            if (server_restart)
                sleep(3);
            break; // connect successfully, so break;
        
        } catch (exception &e) { // connect fail
            auto ret = kill(server_process_id, 0);
            if (ret != 0) { // server has die
                cerr << "testing server die, restart it" << endl;

                while (try_to_kill_server() == false) {} // just for safe
                server_process_id = fork_db_server(d_info);
                time_begin = get_cur_time_ms();
                server_restart = true;
                continue;
            }

            auto time_end = get_cur_time_ms();
            if (time_end - time_begin > WAIT_FOR_PROC_TIME_MS) {
                cerr << "testing server hang, kill it and restart" << endl;
                
                while (try_to_kill_server() == false) {}
                server_process_id = fork_db_server(d_info);
                time_begin = get_cur_time_ms();
                server_restart = true;
                continue;
            }
        }
    }

    return server_restart;
}

string print_stmt_to_string(shared_ptr<prod> stmt)
{
    ostringstream stmt_stream;
    stmt->out(stmt_stream);
    return stmt_stream.str() + ";";
}