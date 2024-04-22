#include "sqlite.hh"

static regex e_syntax("Parse error near[\\s\\S]*: syntax error[\\s\\S]*");
static regex e_user_abort("callback requested query abort");
static regex e_parstack_over("[\\s\\S]*parser stack overflow[\\s\\S]*");
static regex e_misuse_agg("[\\s\\S]*misuse of aggregate[\\s\\S]*");
static regex e_misuse_win("[\\s\\S]*misuse of window[\\s\\S]*");
static regex e_arg("[\\s\\S]*argument[\\s\\S]*must be a[\\s\\S]*");
static regex e_json_object("[\\s\\S]*json_object[\\s\\S]* labels must be TEXT[\\s\\S]*");
static regex e_json_blob("[\\s\\S]*JSON cannot hold BLOB values[\\s\\S]*");
static regex e_big_string("[\\s\\S]*string or blob too big[\\s\\S]*");
static regex e_many_refs("[\\s\\S]*too many references to[\\s\\S]*");
static regex e_constraint_fail("[\\s\\S]*constraint failed[\\s\\S]*");
static regex e_datatype_mismatch("[\\s\\S]*datatype mismatch[\\s\\S]*");
static regex e_join_tables("[\\s\\S]*at most[\\s\\S]*tables in a join[\\s\\S]*");
static regex e_store_columns("[\\s\\S]*cannot store[\\s\\S]*column[\\s\\S]*");
static regex e_integer_overflow("[\\s\\S]*integer overflow[\\s\\S]*");
static regex e_having_nonagg("[\\s\\S]*HAVING clause on a non-aggregate query[\\s\\S]*");
static regex e_complex_like("[\\s\\S]*LIKE or GLOB pattern too complex[\\s\\S]*");
static regex e_many_from("[\\s\\S]*too many FROM clause terms[\\s\\S]*");
static regex e_out_of_memory("Runtime error[\\s\\S]*: out of memory[\\s\\S]*"); // caused by too many reference
static regex e_no_table("Parse error[\\s\\S]*: no such table[\\s\\S]*"); // caused by too many reference
static regex e_on_reference("Parse error[\\s\\S]*: ON clause references tables to its right[\\s\\S]*");

#define QUERY_TIMEOUT_S 6
#define QUERY_TIMEOUT_US 0

extern "C" int my_sqlite3_busy_handler(void *, int)
{
    throw std::runtime_error("sqlite3 timeout");
}

extern "C" int callback(void *arg, int argc, char **argv, char **azColName)
{
    (void)arg;

    int i;
    for(i = 0; i < argc; i++){
        printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    return 0;
}

extern "C" int table_callback(void *arg, int argc, char **argv, char **azColName)
{
    (void) argc; (void) azColName;
    auto tables = (vector<table> *)arg;
    bool view = (string("view") == argv[0]);
    table tab(argv[2], "main", !view, !view);
    tables->push_back(tab);
    return 0;
}

extern "C" int index_callback(void *arg, int argc, char **argv, char **azColName)
{
    (void) argc; (void) azColName;
    auto indexes = (vector<string> *)arg;
    if (argv[0] != NULL) {
        indexes->push_back(argv[0]);
    }
    return 0;
}

extern "C" int column_callback(void *arg, int argc, char **argv, char **azColName)
{
    (void) argc; (void) azColName;
    table *tab = (table *)arg;
    column c(argv[1], sqltype::get(argv[2]));
    tab->columns().push_back(c);
    return 0;
}

sqlite_connection::sqlite_connection(std::string &conninfo)
{
    if(sqlite3_initialize()) {
        cerr << "fail to initialize sqlite lib" << endl;
        throw runtime_error("fail to initialize sqlite lib");
    }
    rc = sqlite3_open_v2(conninfo.c_str(), &db, SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, 0);
    if (rc) {
        cerr << sqlite3_errmsg(db) << endl;
        throw runtime_error(sqlite3_errmsg(db));
    }
    db_file = conninfo;
    // cerr << "SQLITE_VERSION: " << SQLITE_VERSION << endl;
    // cerr << pthread_self() << ": connect" << endl;
}

void sqlite_connection::q(const char *query)
{
    rc = sqlite3_exec(db, query, callback, 0, &zErrMsg);
    if( rc != SQLITE_OK ){
        auto e = std::runtime_error(zErrMsg);
        sqlite3_free(zErrMsg);
        throw e;
    }
}

sqlite_connection::~sqlite_connection()
{
    if (db)
        sqlite3_close(db);
    sqlite3_shutdown();
    // cerr << pthread_self() << ": disconnect" << endl;
}

schema_sqlite::schema_sqlite(string &conninfo, bool no_catalog)
  : sqlite_connection(conninfo)
{
    string query = "SELECT * FROM main.sqlite_master where type in ('table', 'view')";

    if (no_catalog)
        query+= " AND name NOT like 'sqlite_%%'";
  
    version = "SQLite " SQLITE_VERSION " " SQLITE_SOURCE_ID;

//   sqlite3_busy_handler(db, my_sqlite3_busy_handler, 0);
    // cerr << "Loading tables...";
    rc = sqlite3_exec(db, query.c_str(), table_callback, (void *)&tables, &zErrMsg);
    if (rc!=SQLITE_OK) {
        auto e = runtime_error("error in loading table: " + string(zErrMsg));
        sqlite3_free(zErrMsg);
        throw e;
    }

    if (!no_catalog) {
		// sqlite_master doesn't list itself, do it manually
		table tab("sqlite_master", "main", false, false);
		tables.push_back(tab);
    }
    // cerr << "done." << endl;

    // cerr << "Loading indexes...";
    string query_index = "SELECT name FROM sqlite_master WHERE type='index' ORDER BY 1;";
    rc = sqlite3_exec(db, query_index.c_str(), index_callback, (void *)&indexes, &zErrMsg);
    if (rc!=SQLITE_OK) {
        auto e = std::runtime_error("error in loading index: " + string(zErrMsg));
        sqlite3_free(zErrMsg);
        throw e;
    }
    // cerr << "done." << endl;

    // cerr << "Loading columns and constraints...";
    for (auto t = tables.begin(); t != tables.end(); ++t) {
        string q("pragma table_info(");
        q += t->name;
        q += ");";
        rc = sqlite3_exec(db, q.c_str(), column_callback, (void *)&*t, &zErrMsg);
        if (rc!=SQLITE_OK) {
            auto e = runtime_error("expected error in loading column: " + string(zErrMsg));
            sqlite3_free(zErrMsg);
            throw e;
        }
    }
    // cerr << "done." << endl;

    booltype = sqltype::get("BOOLEAN");
    inttype = sqltype::get("INTEGER");
    realtype = sqltype::get("REAL");
    texttype = sqltype::get("TEXT");
    datetype = sqltype::get("TEXT");

    // operation on different collation could be non determined
    // available_collation.push_back("BINARY");
    // available_collation.push_back("RTRIM");
    // available_collation.push_back("NOCASE");

    available_table_options.push_back("WITHOUT ROWID");
    available_table_options.push_back("STRICT");
    available_table_options.push_back("WITHOUT ROWID, STRICT");

    compound_operators.push_back("union");
    compound_operators.push_back("union all");
    compound_operators.push_back("intersect");
    compound_operators.push_back("except");
    // union cannot be used with collation "nocase", it will remove some rows (e.g. 'mI' and 'Mi')
    // union also cannot be used with collation "rtrim", it will remove some rows (e.g. '' and ' ')

    enable_partial_index = true;
    enable_analyze_stmt = true;

    supported_join_op.push_back("left outer");
    supported_join_op.push_back("right outer");
    supported_join_op.push_back("full outer");
    supported_join_op.push_back("inner");
    supported_join_op.push_back("cross");

    target_dbms = "sqlite";

    BINOP(||, texttype, texttype, texttype);
    BINOP(*, inttype, inttype, inttype);
    BINOP(*, realtype, realtype, realtype);
    BINOP(/, inttype, inttype, inttype);
    BINOP(/, realtype, realtype, realtype);
    BINOP(%, inttype, inttype, inttype);
    BINOP(+, inttype, inttype, inttype);
    BINOP(+, realtype, realtype, realtype);
    BINOP(-, inttype, inttype, inttype);
    BINOP(-, realtype, realtype, realtype);
    BINOP(>>, inttype, inttype, inttype);
    BINOP(<<, inttype, inttype, inttype);

    BINOP(&, inttype, inttype, inttype);
    BINOP(|, inttype, inttype, inttype);

    BINOP(<, inttype, inttype, booltype);
    BINOP(<, realtype, realtype, booltype);
    BINOP(<, texttype, texttype, booltype);

    BINOP(<=, inttype, inttype, booltype);
    BINOP(<=, realtype, realtype, booltype);
    BINOP(<=, texttype, texttype, booltype);

    BINOP(>, inttype, inttype, booltype);
    BINOP(>, realtype, realtype, booltype);
    BINOP(>, texttype, texttype, booltype);

    BINOP(>=, inttype, inttype, booltype);
    BINOP(>=, realtype, realtype, booltype);
    BINOP(>=, texttype, texttype, booltype);

    BINOP(=, inttype, inttype, booltype);
    BINOP(=, realtype, realtype, booltype);
    BINOP(=, texttype, texttype, booltype);

    BINOP(<>, inttype, inttype, booltype);
    BINOP(<>, realtype, realtype, booltype);
    BINOP(<>, texttype, texttype, booltype);

    FUNC1(abs, inttype, inttype);
    FUNC1(abs, realtype, realtype);

    // FUNC1(char, texttype, inttype); // may generate invalid string
    // FUNC2(char, texttype, inttype, inttype);
    // FUNC3(char, texttype, inttype, inttype, inttype);

    FUNC3(coalesce, texttype, texttype, texttype, texttype);
    FUNC3(coalesce, inttype, inttype, inttype, inttype);
    FUNC3(coalesce, realtype, realtype, realtype, realtype);

    FUNC2(glob, booltype, texttype, texttype);

    FUNC1(hex, texttype, texttype);

    FUNC2(ifnull, texttype, texttype, texttype);
    FUNC2(ifnull, inttype, inttype, inttype);
    FUNC2(ifnull, realtype, realtype, realtype);

    FUNC3(iif, texttype, booltype, texttype, texttype);
    FUNC3(iif, inttype, booltype, inttype, inttype);
    FUNC3(iif, realtype, booltype, realtype, realtype);

    FUNC2(instr, inttype, texttype, texttype);

    FUNC2(like, booltype, texttype, texttype);

    FUNC1(length, inttype, texttype);
    FUNC1(lower, texttype, texttype);
    FUNC1(ltrim, texttype, texttype);
    FUNC2(ltrim, texttype, texttype, texttype);

    FUNC2(max, inttype, inttype, inttype);
    FUNC2(max, realtype, realtype, realtype);
    FUNC2(min, inttype, inttype, inttype);
    FUNC2(min, realtype, realtype, realtype);

    FUNC2(nullif, inttype, inttype, inttype);
    FUNC2(nullif, realtype, realtype, realtype);
    FUNC2(nullif, texttype, texttype, texttype);

    FUNC1(quote, texttype, texttype);

    FUNC3(replace, texttype, texttype, texttype, texttype);

    FUNC1(round, inttype, realtype);
    FUNC2(round, inttype, realtype, inttype);

    FUNC1(rtrim, texttype, texttype);
    FUNC2(rtrim, texttype, texttype, texttype);

    FUNC1(sign, inttype, realtype);
    FUNC1(sign, inttype, inttype);

    // FUNC1(soundex, texttype, texttype); // only available if the SQLITE_SOUNDEX compile-time option is used

    FUNC1(sqlite_compileoption_get, texttype, inttype);
    FUNC1(sqlite_compileoption_used, inttype, texttype);
    FUNC(sqlite_source_id, texttype);
    FUNC(sqlite_version, texttype);

    FUNC2(substr, texttype, texttype, inttype);
    FUNC3(substr, texttype, texttype, inttype, inttype);

    FUNC1(trim, texttype, texttype);
    FUNC2(trim, texttype, texttype, texttype);

    FUNC1(typeof, texttype, inttype);
    FUNC1(typeof, texttype, realtype);
    FUNC1(typeof, texttype, texttype);
    FUNC1(typeof, texttype, booltype);

    FUNC1(unicode, inttype, texttype);
    FUNC1(upper, texttype, texttype);

    FUNC1(zeroblob, texttype, inttype);

    // available when -DSQLITE_ENABLE_MATH_FUNCTIONS compile-time option
    FUNC1(acos, realtype, inttype);
    FUNC1(acos, realtype, realtype);
    FUNC1(acos, realtype, texttype);

    FUNC1(acosh, realtype, inttype);
    FUNC1(acosh, realtype, realtype);
    FUNC1(acosh, realtype, texttype);

    FUNC1(asin, realtype, inttype);
    FUNC1(asin, realtype, realtype);
    FUNC1(asin, realtype, texttype);

    FUNC1(atan, realtype, inttype);
    FUNC1(atan, realtype, realtype);
    FUNC1(atan, realtype, texttype);

    FUNC2(atan2, realtype, inttype, inttype);
    FUNC2(atan2, realtype, realtype, realtype);
    FUNC2(atan2, realtype, texttype, texttype);

    FUNC1(atanh, realtype, inttype);
    FUNC1(atanh, realtype, realtype);
    FUNC1(atanh, realtype, texttype);

    FUNC1(ceil, inttype, inttype);
    FUNC1(ceil, inttype, realtype);
    FUNC1(ceil, inttype, texttype);

    FUNC1(cos, realtype, inttype);
    FUNC1(cos, realtype, realtype);
    FUNC1(cos, realtype, texttype);

    FUNC1(cosh, realtype, inttype);
    FUNC1(cosh, realtype, realtype);
    FUNC1(cosh, realtype, texttype);

    FUNC1(degrees, realtype, inttype);
    FUNC1(degrees, realtype, realtype);
    FUNC1(degrees, realtype, texttype);

    FUNC1(exp, realtype, inttype);
    FUNC1(exp, realtype, realtype);
    FUNC1(exp, realtype, texttype);

    FUNC1(floor, inttype, inttype);
    FUNC1(floor, inttype, realtype);
    FUNC1(floor, inttype, texttype);

    FUNC1(ln, realtype, inttype);
    FUNC1(ln, realtype, realtype);
    FUNC1(ln, realtype, texttype);

    FUNC1(log, realtype, inttype);
    FUNC1(log, realtype, realtype);
    FUNC1(log, realtype, texttype);

    FUNC2(log, realtype, inttype, inttype);
    FUNC2(log, realtype, realtype, realtype);
    FUNC2(log, realtype, texttype, texttype);

    FUNC1(log2, realtype, inttype);
    FUNC1(log2, realtype, realtype);
    FUNC1(log2, realtype, texttype);

    FUNC2(mod, realtype, inttype, inttype);
    FUNC2(mod, realtype, realtype, realtype);
    FUNC2(mod, realtype, texttype, texttype);

    FUNC(pi, realtype);

    FUNC2(pow, realtype, inttype, inttype);
    FUNC2(pow, realtype, realtype, realtype);
    FUNC2(pow, realtype, texttype, texttype);

    FUNC1(radians, realtype, inttype);
    FUNC1(radians, realtype, realtype);
    FUNC1(radians, realtype, texttype);

    FUNC1(sin, realtype, inttype);
    FUNC1(sin, realtype, realtype);
    FUNC1(sin, realtype, texttype);

    FUNC1(sinh, realtype, inttype);
    FUNC1(sinh, realtype, realtype);
    FUNC1(sinh, realtype, texttype);

    FUNC1(sqrt, realtype, inttype);
    FUNC1(sqrt, realtype, realtype);
    FUNC1(sqrt, realtype, texttype);

    FUNC1(tan, realtype, inttype);
    FUNC1(tan, realtype, realtype);
    FUNC1(tan, realtype, texttype);

    FUNC1(tanh, realtype, inttype);
    FUNC1(tanh, realtype, realtype);
    FUNC1(tanh, realtype, texttype);

    FUNC1(trunc, inttype, inttype);
    FUNC1(trunc, inttype, realtype);
    FUNC1(trunc, inttype, texttype);

    // json related function
    FUNC1(json_array, texttype, texttype);
    FUNC2(json_array, texttype, texttype, texttype);
    FUNC3(json_array, texttype, texttype, texttype, texttype);

    FUNC2(json_object, texttype, texttype, texttype);
    FUNC4(json_object, texttype, texttype, texttype, texttype, texttype);

    FUNC1(json_valid, inttype, texttype);

    FUNC1(json_quote, texttype, inttype);
    FUNC1(json_quote, texttype, realtype);
    FUNC1(json_quote, texttype, texttype);

    AGG1(avg, inttype, inttype);
    AGG1(avg, realtype, realtype);
    AGG(count, inttype);
    AGG1(count, inttype, realtype);
    AGG1(count, inttype, texttype);
    AGG1(count, inttype, inttype);

    // AGG1(group_concat, texttype, texttype);
    AGG1(max, realtype, realtype);
    AGG1(max, inttype, inttype);
    AGG1(max, texttype, texttype);
    AGG1(min, realtype, realtype);
    AGG1(min, inttype, inttype);
    AGG1(min, texttype, texttype);
    AGG1(sum, realtype, realtype);
    AGG1(sum, inttype, inttype);
    AGG1(total, realtype, inttype);
    AGG1(total, realtype, realtype);

    WIN(row_number, inttype);
    WIN(rank, inttype);
    WIN(dense_rank, inttype);
    WIN(percent_rank, realtype);
    WIN(cume_dist, realtype);
    WIN1(ntile, inttype, inttype);
    
    WIN1(lag, inttype, inttype);
    WIN1(lag, realtype, realtype);
    WIN1(lag, texttype, texttype);

    WIN2(lead, inttype, inttype, inttype);
    WIN2(lead, realtype, realtype, inttype);
    WIN2(lead, texttype, texttype, inttype);
    
    WIN1(first_value, inttype, inttype);
    WIN1(first_value, realtype, realtype);
    WIN1(first_value, texttype, texttype);
    WIN1(last_value, inttype, inttype);
    WIN1(last_value, realtype, realtype);
    WIN1(last_value, texttype, texttype);

    WIN2(nth_value, inttype, inttype, inttype);
    WIN2(nth_value, realtype, realtype, inttype);
    WIN2(nth_value, texttype, texttype, inttype);
    
    internaltype = sqltype::get("internal");
    arraytype = sqltype::get("ARRAY");

    true_literal = "true";
    false_literal = "false";
    null_literal = "null";

    generate_indexes();
}

void schema_sqlite::update_schema()
{
    // cerr << "Loading tables...";
    string query = "SELECT * FROM main.sqlite_master where type in ('table', 'view')";
    rc = sqlite3_exec(db, query.c_str(), table_callback, (void *)&tables, &zErrMsg);
    if (rc!=SQLITE_OK) {
        auto e = std::runtime_error(zErrMsg);
        sqlite3_free(zErrMsg);
        throw e;
    }
    // cerr << "done." << endl;

    // cerr << "Loading indexes...";
    string query_index = "SELECT name FROM sqlite_master WHERE type='index' ORDER BY 1;";
    rc = sqlite3_exec(db, query_index.c_str(), index_callback, (void *)&indexes, &zErrMsg);
    if (rc!=SQLITE_OK) {
        auto e = std::runtime_error(zErrMsg);
        sqlite3_free(zErrMsg);
        throw e;
    }
    // cerr << "done." << endl;

    // cerr << "Loading columns and constraints...";
    for (auto t = tables.begin(); t != tables.end(); ++t) {
        string q("pragma table_info(");
        q += t->name;
        q += ");";
        rc = sqlite3_exec(db, q.c_str(), column_callback, (void *)&*t, &zErrMsg);
        if (rc!=SQLITE_OK) {
            auto e = std::runtime_error(zErrMsg);
            sqlite3_free(zErrMsg);
            throw e;
        }
    }
    // cerr << "done." << endl;
    return;
}

dut_sqlite::dut_sqlite(std::string &conninfo)
  : sqlite_connection(conninfo)
{
    // q("PRAGMA main.auto_vacuum = 2");
    // if (register_signal == false) {
    //     struct sigaction sa;  
    //     memset(&sa, 0, sizeof(sa));  
    //     sigemptyset(&sa.sa_mask);  
    //     sa.sa_flags = SA_RESTART; 
    //     sa.sa_handler = kill_sqlite_process;  
    //     if (sigaction(SIGALRM, &sa, NULL)) {
    //         cerr << "sigaction error" << endl;
    //         abort();
    //     }
    // }
}

extern "C" int dut_callback(void *arg, int argc, char **argv, char **azColName)
{
    (void) arg; (void) argc; (void) argv; (void) azColName;
    return 0;
    // return SQLITE_ABORT;
}

static bool is_double(string myString, long double& result) {
    istringstream iss(myString);
    iss >> noskipws >> result; // noskipws considers leading whitespace invalid
    // Check the entire string was consumed and if either failbit or badbit is set
    return iss.eof() && !iss.fail(); 
}

static string process_number_string(string str)
{
    str.erase(0, str.find_first_not_of(" "));
    str.erase(str.find_last_not_of(" ") + 1);

    // process the string if the string is a number
    string final_str;
    long double result;
    if (is_double(str, result) == false) {
        final_str = str;
    }
    else {
        stringstream ss;
        ss << setiosflags(ios::fixed) << setprecision(0) << result;
        final_str = ss.str();
    }
    return final_str;
}

extern "C" int content_callback(void *data, int argc, char **argv, char **azColName){
    int i; (void) azColName;
    auto data_vec = (vector<vector<string>> *)data;
    if (data_vec == NULL)
        return 0;

    vector<string> row_output;
    for (i = 0; i < argc; i++) {
        if (argv[i] == NULL) {
            row_output.push_back("NULL");
            continue;
        }
        string str = argv[i];
        row_output.push_back(process_number_string(str));
    }
    data_vec->push_back(row_output);
    return 0;
}

void dut_sqlite::test(const string &stmt, 
    vector<vector<string>>* output, 
    int* affected_row_num,
    vector<string>* env_setting_stmts)
{
    ofstream testfile("sqlite3_test.sql");
    testfile << ".nullvalue NullValue" << endl; // setting null showing value
    testfile << ";" << endl;
    testfile << stmt << endl;
    testfile << ";" << endl;
    testfile.close();
    auto cmd = "timeout " + to_string(QUERY_TIMEOUT_S) + " sqlite3 " + db_file + " < sqlite3_test.sql > sqlite3_output 2> sqlite3_error";
    if (cpu_affinity >= 0){
        cmd = "taskset -c " + to_string(cpu_affinity) + " " + cmd;
    }
    auto ret = system(cmd.c_str());

    ifstream sql_error("sqlite3_error");
    stringstream err_buffer;
    err_buffer << sql_error.rdbuf();
    sql_error.close();
    string err(err_buffer.str());
    if (err.empty() == false) {
        if (regex_match(err, e_parstack_over)
            || regex_match(err, e_syntax)
            || regex_match(err, e_misuse_agg)
            || regex_match(err, e_misuse_win)
            || regex_match(err, e_arg)
            || regex_match(err, e_json_object)
            || regex_match(err, e_json_blob)
            || regex_match(err, e_big_string)
            || regex_match(err, e_many_refs)
            || regex_match(err, e_constraint_fail)
            || regex_match(err, e_datatype_mismatch)
            || regex_match(err, e_join_tables)
            || regex_match(err, e_store_columns)
            || regex_match(err, e_integer_overflow)
            || regex_match(err, e_having_nonagg)
            || regex_match(err, e_complex_like)
            || regex_match(err, e_many_from)
            || regex_match(err, e_out_of_memory)
            || regex_match(err, e_no_table)
            || regex_match(err, e_on_reference)
            )
            throw runtime_error("sqlite3 expected error [" + err + "]");
        
        throw runtime_error("sqlite3 fails [" + err + "]");
    }
    
    if (ret != 0) 
        throw runtime_error("sqlite3 expected error [timeout]");

    if (affected_row_num)
        *affected_row_num = 1; // cannot get affected_row_num, just set it to one
    
    if (output == NULL)
        return;

    output->clear();
    ifstream sql_output("sqlite3_output");
    stringstream output_buffer;
    output_buffer << sql_output.rdbuf();
    sql_output.close();
    string out(output_buffer.str());
    
    string item;
    vector<string> row;
    for (int i = 0; i < out.size(); i++) {
        if (out[i] == '\n') {
            if (item.empty())
                item = "NULL";
            row.push_back(process_number_string(item));
            output->push_back(row);
            row.clear();
            item.clear();
        }
        else if (out[i] == '|') {
            if (item.empty())
                item = "NULL";
            row.push_back(process_number_string(item));
            item.clear();
        }
        else {
            item += out[i];
        }
    }
    // handle the last line
    if (row.empty() == false) {
        if (item.empty())
            item = "NULL";
        row.push_back(process_number_string(item));
        output->push_back(row);
        row.clear();
        item.clear();
    }
}

void dut_sqlite::reset(void)
{
    if (db)
        sqlite3_close(db);
    remove(db_file.c_str());

    rc = sqlite3_open_v2(db_file.c_str(), &db, SQLITE_OPEN_READWRITE|SQLITE_OPEN_URI|SQLITE_OPEN_CREATE, 0);
    if (rc) {
        throw std::runtime_error(sqlite3_errmsg(db));
    }
}

void dut_sqlite::backup(void)
{
    auto bk_db = db_file;
    auto pos = bk_db.find(".db");
    if (pos != string::npos) {
        bk_db.erase(pos, 3);
    }
    bk_db += "_bk.db";
    remove(bk_db.c_str());

    sqlite3 *dst_db;
    auto dst_rc = sqlite3_open_v2(bk_db.c_str(), &dst_db, SQLITE_OPEN_READWRITE|SQLITE_OPEN_URI|SQLITE_OPEN_CREATE, 0);
    if (dst_rc) {
        throw std::runtime_error(sqlite3_errmsg(db));
    }

    auto bck = sqlite3_backup_init(dst_db, "main", db, "main");
    if (bck == nullptr) {
        throw std::runtime_error("sqlite3_backup_init fail");
    }

    auto err =sqlite3_backup_step(bck, -1);
    if (err != SQLITE_DONE) {
        sqlite3_backup_finish(bck);
        throw std::runtime_error("sqlite3_backup_step fail");
    }

    err = sqlite3_backup_finish(bck);

    sqlite3_close(dst_db);
    return;
}

// if bk_db is empty, it will reset to empty
void dut_sqlite::reset_to_backup(void)
{
    auto bk_db = db_file;
    auto pos = bk_db.find(".db");
    if (pos != string::npos) {
        bk_db.erase(pos, 3);
    }
    bk_db += "_bk.db";
    
    if (db)
        sqlite3_close(db);
    remove(db_file.c_str());

    sqlite3 *src_db;
    auto src_rc = sqlite3_open_v2(bk_db.c_str(), &src_db, SQLITE_OPEN_READWRITE|SQLITE_OPEN_URI|SQLITE_OPEN_CREATE, 0);
    if (src_rc) {
        throw std::runtime_error(sqlite3_errmsg(db));
    }

    rc = sqlite3_open_v2(db_file.c_str(), &db, SQLITE_OPEN_READWRITE|SQLITE_OPEN_URI|SQLITE_OPEN_CREATE, 0);
    if (rc) {
        throw std::runtime_error(sqlite3_errmsg(db));
    }

    auto bck = sqlite3_backup_init(db, "main", src_db, "main");
    if (bck == nullptr) {
        throw std::runtime_error("sqlite3_backup_init fail");
    }

    auto err =sqlite3_backup_step(bck, -1);
    if (err != SQLITE_DONE) {
        sqlite3_backup_finish(bck);
        throw std::runtime_error("sqlite3_backup_step fail");
    }

    err = sqlite3_backup_finish(bck);

    sqlite3_close(src_db);
    return;
}

int dut_sqlite::save_backup_file(string path, string db_name)
{
    auto bk_db = db_name;
    auto pos = bk_db.find(".db");
    if (pos != string::npos) {
        bk_db.erase(pos, 3);
    }
    bk_db += "_bk.db";

    string cp_cmd = "cp " + bk_db + " " + path;
    return system(cp_cmd.c_str());
}

void dut_sqlite::get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content)
{
    for (auto& table:tables_name) {
        vector<vector<string>> table_content;
        auto query = "SELECT * FROM " + table + " ORDER BY 1;";
        
        rc = sqlite3_exec(db, query.c_str(), content_callback, (void *)&table_content, &zErrMsg);
        if (rc != SQLITE_OK) {
            auto e = std::runtime_error(zErrMsg);
            sqlite3_free(zErrMsg);
            throw e;
        }

        content[table] = table_content;
    }
}

string dut_sqlite::commit_stmt() {
    return "COMMIT";
}

string dut_sqlite::abort_stmt() {
    return "ROLLBACK";
}

string dut_sqlite::begin_stmt() {
    return "BEGIN TRANSACTION";
}

string dut_sqlite::get_process_id() {
    cerr << "SQLite get_process_id is not implemented" << endl;
    abort();
    return "";
}