#include "tidb.hh"

static regex e_unknown_database("[\\s\\S]*Unknown database[\\s\\S]*");
static regex e_crash("[\\s\\S]*Lost connection[\\s\\S]*");
static regex e_dup_entry("Duplicate entry[\\s\\S]*for key[\\s\\S]*");
static regex e_large_results("Result of[\\s\\S]*was larger than max_allowed_packet[\\s\\S]*");
static regex e_col_ambiguous("Column[\\s\\S]*in field list is ambiguous");
static regex e_truncated("Truncated incorrect DOUBLE value:[\\s\\S]*");
static regex e_division_zero("Division by 0");
static regex e_unknown_col("Unknown column[\\s\\S]*"); // could be an unexpected error later
static regex e_incorrect_args("Incorrect arguments to[\\s\\S]*");
static regex e_out_of_range("[\\s\\S]*value is out of range[\\s\\S]*");
static regex e_win_context("You cannot use the window function[\\s\\S]*in this context[\\s\\S]*");
// same root cause of e_unknown_col, also could be an unexpected error later
static regex e_view_reference("[\\s\\S]*view lack rights to use them[\\s\\S]*");
static regex e_context_cancel("context canceled");
static regex e_string_convert("Cannot convert string[\\s\\S]*from binary to[\\s\\S]*");
static regex e_col_null("Column[\\s\\S]*cannot be null[\\s\\S]*");
static regex e_sridb_pk("Unsupported shard_row_id_bits for table with primary key as row id[\\s\\S]*");
static regex e_syntax("You have an error in your SQL syntax[\\s\\S]*");
static regex e_invalid_group("Invalid use of group function");
static regex e_invalid_group_2("In aggregated query without GROUP BY, expression[\\s\\S]*");
static regex e_oom("Out Of Memory Quota[\\s\\S]*");
static regex e_schema_changed("Information schema is changed during the execution of[\\s\\S]*");
static regex e_over_mem("[\\s\\S]*Your query has been cancelled due to exceeding the allowed memory limit for a single SQL query[\\s\\S]*");
static regex e_no_default("Field [\\s\\S]* doesn't have a default value");
static regex e_no_group_by("Expression [\\s\\S]* of SELECT list is not in GROUP BY clause and contains nonaggregated column[\\s\\S]*");
static regex e_timeout("Query execution was interrupted, maximum statement execution time exceeded");

// temporarily marked as expected for not reporting dupilcate bugs
static regex e_invalid_addr("[\\s\\S]*invalid memory address or nil pointer dereference[\\s\\S]*");
static regex e_idx_oor("[\\s\\S]*index out of range[\\s\\S]*");
static regex e_expr_pushdown("[\\s\\S]*expression[\\s\\S]*cannot be pushed down[\\s\\S]*");
static regex e_cannot_column("Can't find column[\\s\\S]*in schema Column[\\s\\S]*");
static regex e_makeslice("[\\s\\S]*makeslice: cap out of range[\\s\\S]*");
static regex e_undef_win("Window name [\\s\\S]* is not defined[\\s\\S]*");

tidb_connection::tidb_connection(string db, unsigned int port)
{
    test_db = db;
    test_port = port;
    
    if (!mysql_init(&mysql))
        throw std::runtime_error(string(mysql_error(&mysql)) + " in tidb_connection!");

    // password null: blank (empty) password field
    if (mysql_real_connect(&mysql, "127.0.0.1", "root", NULL, test_db.c_str(), test_port, NULL, 0)) 
        return; // success
    
    string err = mysql_error(&mysql);
    if (!regex_match(err, e_unknown_database))
        throw std::runtime_error("BUG!!!" + string(mysql_error(&mysql)) + " in tidb_connection!");

    // error caused by unknown database, so create one
    cerr << test_db + " does not exist, use default db" << endl;
    if (!mysql_real_connect(&mysql, "127.0.0.1", "root", NULL, NULL, port, NULL, 0))
        throw std::runtime_error(string(mysql_error(&mysql)) + " in tidb_connection!");
    
    cerr << "create database " + test_db << endl;
    string create_sql = "create database " + test_db + "; ";
    if (mysql_real_query(&mysql, create_sql.c_str(), create_sql.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + " in tidb_connection!");
    auto res = mysql_store_result(&mysql);
    mysql_free_result(res);

    cerr << "use database" + test_db << endl;
    string use_sql = "use " + test_db + "; ";
    if (mysql_real_query(&mysql, use_sql.c_str(), use_sql.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + " in tidb_connection!");
    res = mysql_store_result(&mysql);
    mysql_free_result(res);
}

tidb_connection::~tidb_connection()
{
    mysql_close(&mysql);
}

schema_tidb::schema_tidb(string db, unsigned int port)
  : tidb_connection(db, port)
{
    // cerr << "Loading tables...";
    string get_table_query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
        WHERE TABLE_SCHEMA='" + db + "' AND \
              TABLE_TYPE='BASE TABLE' ORDER BY 1;";
    
    if (mysql_real_query(&mysql, get_table_query.c_str(), get_table_query.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + " in schema_tidb (load table list)!");
    
    auto result = mysql_store_result(&mysql);
    while (auto row = mysql_fetch_row(result)) {
        table tab(row[0], "main", true, true);
        tables.push_back(tab);
    }
    mysql_free_result(result);
    // cerr << "done." << endl;

    // cerr << "Loading views...";
    string get_view_query = "select distinct table_name from information_schema.views \
        where table_schema='" + db + "' order by 1;";
    if (mysql_real_query(&mysql, get_view_query.c_str(), get_view_query.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + " in schema_tidb (load view list)!");
    
    result = mysql_store_result(&mysql);
    while (auto row = mysql_fetch_row(result)) {
        table tab(row[0], "main", false, false);
        tables.push_back(tab);
    }
    mysql_free_result(result);
    // cerr << "done." << endl;

    // cerr << "Loading indexes...";
    string get_index_query = "SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS \
                WHERE TABLE_SCHEMA='" + db + "' AND \
                    NON_UNIQUE=1 AND \
                    INDEX_NAME <> COLUMN_NAME AND \
                    INDEX_NAME <> 'PRIMARY' ORDER BY 1;";
    if (mysql_real_query(&mysql, get_index_query.c_str(), get_index_query.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + " in schema_tidb (load index list)!");

    result = mysql_store_result(&mysql);
    while (auto row = mysql_fetch_row(result)) {
        indexes.push_back(row[0]);
    }
    mysql_free_result(result);
    // cerr << "done." << endl;

    // cerr << "Loading columns and constraints...";
    for (auto& t : tables) {
        string get_column_query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS \
                WHERE TABLE_NAME='" + t.ident() + "' AND \
                    TABLE_SCHEMA='" + db + "'  ORDER BY ORDINAL_POSITION;";
        if (mysql_real_query(&mysql, get_column_query.c_str(), get_column_query.size()))
            throw std::runtime_error(string(mysql_error(&mysql)) + " in schema_tidb (load table " + t.ident() + ")!");
        result = mysql_store_result(&mysql);
        while (auto row = mysql_fetch_row(result)) {
            column c(row[0], sqltype::get(row[1]));
            t.columns().push_back(c);
        }
        mysql_free_result(result);
    }
    // cerr << "done." << endl;

    booltype = sqltype::get("tinyint");
    inttype = sqltype::get("integer");
    realtype = sqltype::get("double");
    texttype = sqltype::get("varchar(100)");
    datetype = sqltype::get("varchar(100)");

    available_table_options.push_back("SHARD_ROW_ID_BITS=0");
    available_table_options.push_back("SHARD_ROW_ID_BITS=6");
    available_table_options.push_back("PRE_SPLIT_REGIONS=0");
    available_table_options.push_back("PRE_SPLIT_REGIONS=4");
    available_table_options.push_back("AUTO_ID_CACHE=0");
    available_table_options.push_back("AUTO_ID_CACHE=200");

    compound_operators.push_back("union");
    compound_operators.push_back("union all");
    compound_operators.push_back("intersect");
    compound_operators.push_back("except");

    available_index_type.push_back("using BTREE");
    available_index_type.push_back("using HASH");
    available_index_type.push_back("using RTREE");

    available_index_keytype.push_back("UNIQUE");
    available_index_keytype.push_back("SPATIAL");
    available_index_keytype.push_back("FULLTEXT");

    enable_analyze_stmt = true;
    target_dbms = "tidb";

    supported_join_op.push_back("inner");
    supported_join_op.push_back("left");
    supported_join_op.push_back("right");
    supported_join_op.push_back("cross");

    BINOP(||, texttype, texttype, texttype);
    BINOP(*, inttype, inttype, inttype);
    // disable because they will produce large number, whose representation is not determined
    // BINOP(/, realtype, inttype, inttype);
    // BINOP(/, realtype, realtype, realtype);
    // BINOP(DIV, inttype, inttype, inttype);
    BINOP(%, inttype, inttype, inttype);

    BINOP(+, inttype, inttype, inttype);
    BINOP(-, inttype, inttype, inttype);

    // disable because they will produce large number, whose representation is not determined
    // BINOP(>>, inttype, inttype, inttype);
    // BINOP(<<, inttype, inttype, inttype);

    BINOP(&, inttype, inttype, inttype);
    BINOP(|, inttype, inttype, inttype);

    BINOP(<, inttype, inttype, booltype);
    BINOP(<, realtype, realtype, booltype);
    BINOP(<=, inttype, inttype, booltype);
    BINOP(<=, realtype, realtype, booltype);
    BINOP(>, inttype, inttype, booltype);
    BINOP(>, realtype, realtype, booltype);
    BINOP(>=, inttype, inttype, booltype);
    BINOP(>=, realtype, realtype, booltype);

    // BINOP(IS NOT, inttype, inttype, booltype);
    // BINOP(IS NOT, realtype, realtype, booltype);
    // BINOP(IS NOT, texttype, texttype, booltype);
    // BINOP(IS NOT, booltype, booltype, booltype);

    // BINOP(IS, inttype, inttype, booltype);
    // BINOP(IS, realtype, realtype, booltype);
    // BINOP(IS, texttype, texttype, booltype);
    // BINOP(IS, booltype, booltype, booltype);

    BINOP(=, inttype, inttype, booltype);
    BINOP(=, realtype, realtype, booltype);
    BINOP(<>, inttype, inttype, booltype);
    BINOP(<>, realtype, realtype, booltype);

    BINOP(and, booltype, booltype, booltype);
    BINOP(or, booltype, booltype, booltype);
    BINOP(xor, booltype, booltype, booltype);

    // tidb numeric
    FUNC(PI, realtype);

    FUNC1(abs, inttype, inttype);
    FUNC1(abs, realtype, realtype);
    FUNC1(hex, texttype, texttype);
    FUNC1(length, inttype, texttype);
    FUNC1(lower, texttype, texttype);
    FUNC1(ltrim, texttype, texttype);
    FUNC1(quote, texttype, texttype);
    FUNC1(round, inttype, realtype);
    FUNC1(rtrim, texttype, texttype);
    FUNC1(trim, texttype, texttype);
    FUNC1(upper, texttype, texttype);
    // add for tidb string
    FUNC1(ASCII, inttype, texttype);
    FUNC1(BIN, texttype, inttype);
    FUNC1(BIT_LENGTH, inttype, texttype);
    FUNC1(CHAR, texttype, inttype);
    FUNC1(CHAR_LENGTH, inttype, texttype);
    FUNC1(SPACE, texttype, inttype);
    FUNC1(REVERSE, texttype, texttype);
    FUNC1(ORD, inttype, texttype);
    FUNC1(OCT, texttype, inttype);
    FUNC1(UNHEX, texttype, texttype);
    // tidb numeric
    FUNC1(EXP, realtype, realtype);
    FUNC1(SQRT, realtype, realtype);
    FUNC1(LN, realtype, realtype);
    FUNC1(LOG, realtype, realtype);
    FUNC1(TAN, realtype, realtype);
    FUNC1(COT, realtype, realtype);
    FUNC1(SIN, realtype, realtype);
    FUNC1(COS, realtype, realtype);
    FUNC1(ATAN, realtype, realtype);
    FUNC1(ASIN, realtype, realtype);
    FUNC1(ACOS, realtype, realtype);
    FUNC1(RADIANS, realtype, realtype);
    FUNC1(DEGREES, realtype, realtype);
    FUNC1(CEILING, inttype, realtype);
    FUNC1(FLOOR, inttype, realtype);
    FUNC1(ROUND, inttype, realtype);
    FUNC1(SIGN, inttype, realtype);
    FUNC1(SIGN, inttype, inttype);
    FUNC1(CRC32, inttype, texttype);
    
    FUNC2(instr, inttype, texttype, texttype);
    FUNC2(round, realtype, realtype, inttype);
    FUNC2(substr, texttype, texttype, inttype);
    // tidb string
    FUNC2(INSTR, inttype, texttype, texttype);
    FUNC2(LEFT, texttype, texttype, inttype);
    FUNC2(RIGHT, texttype, texttype, inttype);
    FUNC2(REPEAT, texttype, texttype, inttype);
    FUNC2(STRCMP, inttype, texttype, texttype);
    // tidb numeric
    FUNC2(POW, realtype, realtype, realtype);
    FUNC2(LOG, realtype, realtype, realtype);
    FUNC2(MOD, inttype, inttype, inttype);
    FUNC2(ROUND, realtype, realtype, inttype);
    FUNC2(TRUNCATE, realtype, realtype, inttype);

    FUNC3(substr, texttype, texttype, inttype, inttype);
    FUNC3(replace, texttype, texttype, texttype, texttype);
    // add for tidb
    FUNC3(CONCAT, texttype, texttype, texttype, texttype);
    FUNC3(LPAD, texttype, texttype, inttype, texttype);
    FUNC3(RPAD, texttype, texttype, inttype, texttype);
    FUNC3(REPLACE, texttype, texttype, texttype, texttype);
    FUNC3(SUBSTRING, texttype, texttype, inttype, inttype);

    // add for tidb
    FUNC4(CONCAT_WS, texttype, texttype, texttype, texttype, texttype);
    FUNC4(ELT, texttype, inttype, texttype, texttype, texttype);
    // FUNC4(FIELD, inttype, texttype, texttype, texttype, texttype); // report a bug: https://bugs.mysql.com/bug.php?id=110502
    FUNC4(INSERT, texttype, texttype, inttype, inttype, texttype);

    FUNC5(EXPORT_SET, texttype, inttype, texttype, texttype, texttype, inttype);

    AGG1(avg, inttype, inttype);
    AGG1(avg, realtype, realtype);
    AGG(count, inttype);
    AGG1(count, inttype, realtype);
    AGG1(count, inttype, texttype);
    AGG1(count, inttype, inttype);

    AGG1(max, realtype, realtype);
    AGG1(max, inttype, inttype);
    AGG1(min, realtype, realtype);
    AGG1(min, inttype, inttype);
    AGG1(sum, realtype, realtype);
    AGG1(sum, inttype, inttype);

    // ranking window function
    WIN(CUME_DIST, realtype);
    WIN(DENSE_RANK, inttype);
    // WIN1(NTILE, inttype, inttype);
    WIN(RANK, inttype);
    WIN(ROW_NUMBER, inttype);
    WIN(PERCENT_RANK, realtype);

    // value window function
    WIN1(FIRST_VALUE, inttype, inttype);
    WIN1(FIRST_VALUE, realtype, realtype);
    WIN1(FIRST_VALUE, texttype, texttype);
    WIN1(LAST_VALUE, inttype, inttype);
    WIN1(LAST_VALUE, realtype, realtype);
    WIN1(LAST_VALUE, texttype, texttype);
    // WIN1(LAG, inttype, inttype);
    // WIN1(LAG, realtype, realtype);
    // WIN1(LAG, texttype, texttype);
    // WIN2(LEAD, inttype, inttype, inttype);
    // WIN2(LEAD, realtype, realtype, inttype);
    // WIN2(LEAD, texttype, texttype, inttype);

    internaltype = sqltype::get("internal");
    arraytype = sqltype::get("ARRAY");

    true_literal = "true";
    false_literal = "false";

    generate_indexes();
}

void schema_tidb::update_schema()
{
    // cerr << "Loading tables...";
    string get_table_query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
        WHERE TABLE_SCHEMA='" + test_db + "' AND \
              TABLE_TYPE='BASE TABLE' ORDER BY 1;";
    
    if (mysql_real_query(&mysql, get_table_query.c_str(), get_table_query.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + " in schema_tidb (load table list)!");
    
    auto result = mysql_store_result(&mysql);
    while (auto row = mysql_fetch_row(result)) {
        table tab(row[0], "main", true, true);
        tables.push_back(tab);
    }
    mysql_free_result(result);
    // cerr << "done." << endl;

    // cerr << "Loading views...";
    string get_view_query = "select distinct table_name from information_schema.views \
        where table_schema='" + test_db + "' order by 1;";
    if (mysql_real_query(&mysql, get_view_query.c_str(), get_view_query.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + " in schema_tidb (load view list)!");
    
    result = mysql_store_result(&mysql);
    while (auto row = mysql_fetch_row(result)) {
        table tab(row[0], "main", false, false);
        tables.push_back(tab);
    }
    mysql_free_result(result);
    // cerr << "done." << endl;

    // cerr << "Loading indexes...";
    string get_index_query = "SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS \
                WHERE TABLE_SCHEMA='" + test_db + "' AND \
                    NON_UNIQUE=1 AND \
                    INDEX_NAME <> COLUMN_NAME AND \
                    INDEX_NAME <> 'PRIMARY' ORDER BY 1;";
    if (mysql_real_query(&mysql, get_index_query.c_str(), get_index_query.size()))
        throw std::runtime_error(string(mysql_error(&mysql)) + " in schema_tidb (load index list)!");

    result = mysql_store_result(&mysql);
    while (auto row = mysql_fetch_row(result)) {
        indexes.push_back(row[0]);
    }
    mysql_free_result(result);
    // cerr << "done." << endl;

    // cerr << "Loading columns and constraints...";
    for (auto& t : tables) {
        string get_column_query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS \
                WHERE TABLE_NAME='" + t.ident() + "' AND \
                    TABLE_SCHEMA='" + test_db + "'  ORDER BY ORDINAL_POSITION;";
        if (mysql_real_query(&mysql, get_column_query.c_str(), get_column_query.size()))
            throw std::runtime_error(string(mysql_error(&mysql)) + " in schema_tidb (load table " + t.ident() + ")!");
        result = mysql_store_result(&mysql);
        while (auto row = mysql_fetch_row(result)) {
            column c(row[0], sqltype::get(row[1]));
            t.columns().push_back(c);
        }
        mysql_free_result(result);
    }
    // cerr << "done." << endl;
    return;
}

dut_tidb::dut_tidb(string db, unsigned int port)
  : tidb_connection(db, port)
{
    string stmt = "SET MAX_EXECUTION_TIME = 6000;"; // 6 seconds
    if (mysql_real_query(&mysql, stmt.c_str(), stmt.size())) {
        string err = mysql_error(&mysql);
        throw runtime_error(err + " in dut_tidb::dut_tidb"); 
    }
}

static bool is_double(string myString, long double& result) {
    istringstream iss(myString);
    iss >> noskipws >> result; // noskipws considers leading whitespace invalid
    // Check the entire string was consumed and if either failbit or badbit is set
    return iss.eof() && !iss.fail(); 
}

static vector<string> hash_a_row(vector<string>& row)
{
    string total = "";
    for (auto& str : row) {
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
        total = total + final_str + " ";
    }
    hash<string> hasher;
    auto hashed_str = to_string(hasher(total));
    vector<string> hased_row;
    hased_row.push_back(hashed_str);
    return hased_row;
}

void dut_tidb::test(const std::string &stmt, 
    vector<vector<string>>* output, 
    int* affected_row_num,
    vector<string>* env_setting_stmts)
{
    if (mysql_real_query(&mysql, stmt.c_str(), stmt.size())) {
        string err = mysql_error(&mysql);
        auto result = mysql_store_result(&mysql);
        mysql_free_result(result);
        if (err.find("Commands out of sync") != string::npos) {// occasionally happens, retry the statement again
            cerr << err << ", repeat the statement again" << endl;
            test(stmt, output, affected_row_num);
            return;
        }
        if (regex_match(err, e_crash)) {
            throw runtime_error("BUG!!! " + err + " in mysql::test"); 
        }
        string prefix = "tidb test expected error:";
        if (regex_match(err, e_dup_entry) 
            || regex_match(err, e_large_results) 
            || regex_match(err, e_timeout) 
            || regex_match(err, e_col_ambiguous)
            || regex_match(err, e_truncated) 
            || regex_match(err, e_division_zero)
            || regex_match(err, e_unknown_col) 
            || regex_match(err, e_incorrect_args)
            || regex_match(err, e_out_of_range) 
            || regex_match(err, e_win_context)
            || regex_match(err, e_view_reference) 
            || regex_match(err, e_context_cancel)
            || regex_match(err, e_string_convert)
            || regex_match(err, e_idx_oor)
            || regex_match(err, e_col_null)
            || regex_match(err, e_sridb_pk)
            || regex_match(err, e_syntax)
            || regex_match(err, e_expr_pushdown)
            || regex_match(err, e_invalid_group)
            || regex_match(err, e_invalid_group_2)
            || regex_match(err, e_oom)
            || regex_match(err, e_cannot_column)
            || regex_match(err, e_schema_changed)
            || regex_match(err, e_invalid_addr)
            || regex_match(err, e_makeslice)
            || regex_match(err, e_undef_win)
            || regex_match(err, e_over_mem)
            || regex_match(err, e_no_default)
            || regex_match(err, e_no_group_by)
           ) {
            throw runtime_error(prefix + err);
        }
        cerr << "the unexpected error [" << err << "]" << endl;
        throw runtime_error(err + " in dut_tidb::test"); 
    }

    if (affected_row_num)
        *affected_row_num = mysql_affected_rows(&mysql);

    auto result = mysql_store_result(&mysql);
    if (output && result) {
        auto row_num = mysql_num_rows(result);
        if (row_num == 0) {
            mysql_free_result(result);
            return;
        }

        auto column_num = mysql_num_fields(result);
        while (auto row = mysql_fetch_row(result)) {
            vector<string> row_output;
            for (int i = 0; i < column_num; i++) {
                string str;
                if (row[i] == NULL)
                    str = "NULL";
                else
                    str = row[i];
                row_output.push_back(str);
            }
            output->push_back(hash_a_row(row_output));
        }
    }
    mysql_free_result(result);
}

void dut_tidb::reset(void)
{
    string drop_sql = "drop database if exists " + test_db + "; ";
    if (mysql_real_query(&mysql, drop_sql.c_str(), drop_sql.size())) {
        string err = mysql_error(&mysql);
        throw std::runtime_error(err + " in mysql::reset");
    }
    auto res_sql = mysql_store_result(&mysql);
    mysql_free_result(res_sql);

    string create_sql = "create database " + test_db + "; ";
    if (mysql_real_query(&mysql, create_sql.c_str(), create_sql.size())) {
        string err = mysql_error(&mysql);
        throw std::runtime_error(err + " in mysql::reset");
    }
    res_sql = mysql_store_result(&mysql);
    mysql_free_result(res_sql);

    string use_sql = "use " + test_db + "; ";
    if (mysql_real_query(&mysql, use_sql.c_str(), use_sql.size())) {
        string err = mysql_error(&mysql);
        throw std::runtime_error(err + " in mysql::reset");
    }
    res_sql = mysql_store_result(&mysql);
    mysql_free_result(res_sql);
}

void dut_tidb::backup(void)
{
    auto backup_name = "/tmp/" + test_db + "_bk.sql";
    string mysql_dump = "mysqldump -h 127.0.0.1 -P " + to_string(test_port) + " -u root " + test_db + " > " + backup_name;
    int ret = system(mysql_dump.c_str());
    if (ret != 0) {
        cerr << "backup fail in dut_tidb::backup!!" << endl;
        throw std::runtime_error("backup fail in dut_tidb::backup"); 
    }
}

void dut_tidb::reset_to_backup(void)
{
    reset();
    string bk_file = "/tmp/" + test_db + "_bk.sql";
    if (access(bk_file.c_str(), F_OK ) == -1) 
        return;
    
    mysql_close(&mysql);
    
    string mysql_source = "mysql -h 127.0.0.1 -P " + to_string(test_port) + " -u root -D " + test_db + " < " + bk_file;
    if (system(mysql_source.c_str()) == -1) 
        throw std::runtime_error(string("system() error, return -1") + " in dut_tidb::reset_to_backup!");

    if (!mysql_real_connect(&mysql, "127.0.0.1", "root", NULL, test_db.c_str(), test_port, NULL, 0)) 
        throw std::runtime_error(string(mysql_error(&mysql)) + " in dut_tidb::reset_to_backup!");
}

int dut_tidb::save_backup_file(string testdb, string path)
{
    string bk_file = "/tmp/" + testdb + "_bk.sql";
    string cp_cmd = "cp " + bk_file + " " + path;
    return system(cp_cmd.c_str());
}

void dut_tidb::get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content)
{
    for (auto& table:tables_name) {
        vector<vector<string>> table_content;
        auto query = "SELECT * FROM " + table + " ORDER BY 1;";

        if (mysql_real_query(&mysql, query.c_str(), query.size())) {
            string err = mysql_error(&mysql);
            cerr << "Cannot get content of " + table + " in mysql::get_content" << endl;
            cerr << "Error: " + err + " in mysql::get_content" << endl;
            // throw std::runtime_error(err + " in mysql::get_content");
            continue;
        }

        auto result = mysql_store_result(&mysql);
        if (result) {
            auto column_num = mysql_num_fields(result);
            while (auto row = mysql_fetch_row(result)) {
                vector<string> row_output;
                for (int i = 0; i < column_num; i++) {
                    string str;
                    if (row[i] == NULL)
                        str = "NULL";
                    else
                        str = row[i];
                    row_output.push_back(str);
                }
                table_content.push_back(row_output);
            }
        }
        mysql_free_result(result);

        content[table] = table_content;
    }
}

string dut_tidb::commit_stmt() {
    return "COMMIT";
}

string dut_tidb::abort_stmt() {
    return "ROLLBACK";
}

string dut_tidb::begin_stmt() {
    return "BEGIN OPTIMISTIC";
}

pid_t dut_tidb::fork_db_server()
{
    // pid_t child = fork();
    // if (child < 0) {
    //     throw std::runtime_error(string("Fork db server fail") + " in dut_tidb::fork_db_server!");
    // }

    // if (child == 0) {
    //     char *server_argv[128];
    //     int i = 0;
    //     server_argv[i++] = (char *)"/root/.tiup/bin/tiup"; // path of tiup
    //     server_argv[i++] = (char *)"playground";
    //     server_argv[i++] = NULL;
    //     execv(server_argv[0], server_argv);
    //     cerr << "fork tidb server fail in dut_tidb::fork_db_server" << endl; 
    // }
    // sleep(30);
    // cout << "server pid: " << child << endl;
    // return child;
    return -1;
}
