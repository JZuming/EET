#include <stdexcept>
#include <cassert>
#include <cstring>
#include "oceanbase.hh"
#include <iostream>
#include <set>
#include <iomanip>

#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

using namespace std;

static regex e_unknown_database(".*Unknown database.*");
static regex e_db_dir_exists("[\\s\\S]*Schema directory[\\s\\S]*already exists. This must be resolved manually[\\s\\S]*");

static regex e_crash(".*Lost connection.*");
static regex e_dup_entry("Duplicate entry[\\s\\S]*for key[\\s\\S]*");
static regex e_large_results("Result of[\\s\\S]*was larger than max_allowed_packet[\\s\\S]*");
static regex e_timeout("Query execution was interrupted"); // timeout
static regex e_timeout2("Timeout, query has reached the maximum query timeout[\\s\\S]*"); // timeout

static regex e_col_ambiguous("Column [\\s\\S]* in [\\s\\S]* is ambiguous");
static regex e_truncated("Truncated incorrect DOUBLE value:[\\s\\S]*");
static regex e_division_zero("Division by 0");
static regex e_unknown_col("Unknown column[\\s\\S]*"); // could be an unexpected error later
static regex e_incorrect_args("Incorrect arguments to[\\s\\S]*");
static regex e_out_of_range("[\\s\\S]*value is out of range[\\s\\S]*");
static regex e_win_context("You cannot use the window function[\\s\\S]*in this context[\\s\\S]*");
static regex e_win_context2("Window Function not allowed here[\\s\\S]*");

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
static regex e_over_mem2("[\\s\\S]*sql resolver no memory[\\s\\S]*");
static regex e_over_mem3("[\\s\\S]*No memory or reach tenant memory limit[\\s\\S]*");

static regex e_no_default("Field [\\s\\S]* doesn't have a default value");
static regex e_no_group_by("Expression [\\s\\S]* of SELECT list is not in GROUP BY clause and contains nonaggregated column[\\s\\S]*");
static regex e_no_support_1("[\\s\\S]* not supported [\\s\\S]*");
static regex e_no_support_2("This version of MySQL doesn't yet support [\\s\\S]*");
static regex e_invalid_arguement("Invalid argument for [\\s\\S]*");
static regex e_incorrect_string("Incorrect string value: [\\s\\S]*");
static regex e_long_specified_key("Specified key was too long; max key length is [\\s\\S]* bytes");
static regex e_out_of_range_2("Out of range value for column [\\s\\S]*");
static regex e_table_not_exists("Table [\\s\\S]* doesn't exist");


extern "C"  {
#include <unistd.h>
}

#define debug_info (string(__func__) + "(" + string(__FILE__) + ":" + to_string(__LINE__) + ")")

static int remove_dir(string dir) {
    string command = "rm -r " + dir;
    return system(command.c_str());
}

static bool is_double(string myString, long double& result) {
    istringstream iss(myString);
    iss >> noskipws >> result; // noskipws considers leading whitespace invalid
    // Check the entire string was consumed and if either failbit or badbit is set
    return iss.eof() && !iss.fail(); 
}

static string process_an_item(string& item)
{
    string final_str;
    long double result;
    if (is_double(item, result) == false) {
        final_str = item;
    }
    else {
        if (result == 0) // result can be -0, represent it as 0
            final_str = "0";
        else {
            stringstream ss;
            int precision = 5;
            if (log10(result) > precision) // keep 5 valid number
                ss << setprecision(precision) << result;
            else // remove the number behind digit point
                ss << setiosflags(ios::fixed) << setprecision(0) << result;
            final_str = ss.str();
        }
    }
    return final_str;
}

ob_connection::ob_connection(string db, unsigned int port, string host)
{
    test_db = db;
    test_port = port;
    host_addr = host;
    
    if (!mysql_init(&conn))
        throw std::runtime_error(string(mysql_error(&conn)) + "\nLocation: " + debug_info);

    // password null: blank (empty) password field
    if (mysql_real_connect(&conn, host_addr.c_str(), "root", NULL, test_db.c_str(), test_port, NULL, 0)) 
        return; // success
    
    string err = mysql_error(&conn);
    if (!regex_match(err, e_unknown_database))
        throw std::runtime_error("BUG!!!" + string(mysql_error(&conn)) + "\nLocation: " + debug_info);

    // error caused by unknown database, so create one
    cerr << test_db + " does not exist, use default db" << endl;
    if (!mysql_real_connect(&conn, host_addr.c_str(), "root", NULL, NULL, port, NULL, 0))
        throw std::runtime_error(string(mysql_error(&conn)) + "\nLocation: " + debug_info);
    
    cerr << "create database " + test_db << endl;
    string create_sql = "create database " + test_db + "; ";
    if (mysql_real_query(&conn, create_sql.c_str(), create_sql.size())) {
        throw std::runtime_error(string(mysql_error(&conn)) + "\nLocation: " + debug_info);
        // auto create_db_err = string(mysql_error(&conn));
        // if (!regex_match(create_db_err, e_db_dir_exists))
        //     throw std::runtime_error(string(mysql_error(&conn)) + "\nLocation: " + debug_info);

        // cerr << "schema directory " << test_db << " already exists. Remove it" << endl;
        // string schema_dir = "/usr/local/mysql/data/" + test_db;
        // int status = remove_dir(schema_dir);
        // if (status != 0)
        //     throw runtime_error("error removing directory.");

        // cerr << "directory removed, create the database again" << endl;
        // if (mysql_real_query(&conn, create_sql.c_str(), create_sql.size())) 
        //     throw runtime_error(string(mysql_error(&conn)) + "\nLocation: " + debug_info);
    }
        
    auto res = mysql_store_result(&conn);
    mysql_free_result(res);

    cerr << "use database " + test_db << endl;
    string use_sql = "use " + test_db + "; ";
    if (mysql_real_query(&conn, use_sql.c_str(), use_sql.size()))
        throw std::runtime_error(string(mysql_error(&conn)) + "\nLocation: " + debug_info);
    res = mysql_store_result(&conn);
    mysql_free_result(res);
}

ob_connection::~ob_connection()
{
    mysql_close(&conn);
}

schema_ob::schema_ob(string db, unsigned int port, string host)
  : ob_connection(db, port, host)
{
    // Loading tables...;
    string get_table_query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
        WHERE TABLE_SCHEMA='" + db + "' AND \
              TABLE_TYPE='BASE TABLE' ORDER BY 1;";
    
    if (mysql_real_query(&conn, get_table_query.c_str(), get_table_query.size()))
        throw std::runtime_error(string(mysql_error(&conn)) + "\nLocation: " + debug_info);
    
    auto result = mysql_store_result(&conn);
    while (auto row = mysql_fetch_row(result)) {
        table tab(row[0], "main", true, true);
        tables.push_back(tab);
    }
    mysql_free_result(result);

    // Loading views...;
    string get_view_query = "select distinct table_name from information_schema.views \
        where table_schema='" + db + "' order by 1;";
    if (mysql_real_query(&conn, get_view_query.c_str(), get_view_query.size()))
        throw std::runtime_error(string(mysql_error(&conn)) + "\nLocation: " + debug_info);
    
    result = mysql_store_result(&conn);
    while (auto row = mysql_fetch_row(result)) {
        table tab(row[0], "main", false, false);
        tables.push_back(tab);
    }
    mysql_free_result(result);

    // Loading indexes...;
    string get_index_query = "SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS \
                WHERE TABLE_SCHEMA='" + db + "' AND \
                    NON_UNIQUE=1 AND \
                    INDEX_NAME <> COLUMN_NAME AND \
                    INDEX_NAME <> 'PRIMARY' ORDER BY 1;";
    if (mysql_real_query(&conn, get_index_query.c_str(), get_index_query.size()))
        throw std::runtime_error(string(mysql_error(&conn)) + "\nLocation: " + debug_info);

    result = mysql_store_result(&conn);
    while (auto row = mysql_fetch_row(result)) {
        indexes.push_back(row[0]);
    }
    mysql_free_result(result);

    // Loading columns and constraints...;
    for (auto& t : tables) {
        string get_column_query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS \
                WHERE TABLE_NAME='" + t.ident() + "' AND \
                    TABLE_SCHEMA='" + db + "'  ORDER BY ORDINAL_POSITION;";
        if (mysql_real_query(&conn, get_column_query.c_str(), get_column_query.size()))
            throw std::runtime_error(string(mysql_error(&conn)) + "\nLocation: " + debug_info + "\nTable: " + t.ident());
        result = mysql_store_result(&conn);
        while (auto row = mysql_fetch_row(result)) {
            column c(row[0], sqltype::get(row[1]));
            t.columns().push_back(c);
        }
        mysql_free_result(result);
    }

    target_dbms = "mysql";

    booltype = sqltype::get("tinyint");
    inttype = sqltype::get("int");
    realtype = sqltype::get("double");
    texttype = sqltype::get("varchar(200)");
    datetype = sqltype::get("DATETIME");
    
    compound_operators.push_back("union distinct");
    compound_operators.push_back("union all");

    supported_join_op.push_back("inner");
    supported_join_op.push_back("left outer");
    supported_join_op.push_back("right outer");
    supported_join_op.push_back("cross");

    // bitwise
    BINOP(&, inttype, inttype, inttype);
    BINOP(>>, inttype, inttype, inttype);
    BINOP(<<, inttype, inttype, inttype);
    BINOP(^, inttype, inttype, inttype);
    BINOP(|, inttype, inttype, inttype);

    // comparison
    BINOP(>, inttype, inttype, booltype);
    BINOP(>, texttype, texttype, booltype);
    BINOP(>, realtype, realtype, booltype);
    BINOP(<, inttype, inttype, booltype);
    BINOP(<, texttype, texttype, booltype);
    BINOP(<, realtype, realtype, booltype);
    BINOP(>=, inttype, inttype, booltype);
    BINOP(>=, texttype, texttype, booltype);
    BINOP(>=, realtype, realtype, booltype);
    BINOP(<=, inttype, inttype, booltype);
    BINOP(<=, texttype, texttype, booltype);
    BINOP(<=, realtype, realtype, booltype);
    BINOP(<>, inttype, inttype, booltype);
    BINOP(<>, texttype, texttype, booltype);
    BINOP(<>, realtype, realtype, booltype);
    BINOP(!=, inttype, inttype, booltype);
    BINOP(!=, texttype, texttype, booltype);
    BINOP(!=, realtype, realtype, booltype);
    BINOP(<=>, inttype, inttype, booltype);
    BINOP(<=>, texttype, texttype, booltype);
    BINOP(<=>, realtype, realtype, booltype);
    BINOP(=, realtype, realtype, booltype);

    // arithmetic
    BINOP(%, inttype, inttype, inttype);
    BINOP(%, realtype, realtype, realtype);
    BINOP(*, inttype, inttype, inttype);
    BINOP(*, realtype, realtype, realtype);
    BINOP(+, inttype, inttype, inttype);
    BINOP(+, realtype, realtype, realtype);
    BINOP(-, inttype, inttype, inttype);
    BINOP(-, realtype, realtype, realtype);
    BINOP(/, inttype, inttype, inttype);
    BINOP(/, realtype, realtype, realtype);
    BINOP(DIV, inttype, inttype, inttype);
    BINOP(DIV, realtype, realtype, realtype);

    // logic
    BINOP(&&, booltype, booltype, booltype);
    BINOP(||, booltype, booltype, booltype);
    BINOP(XOR, booltype, booltype, booltype);

    // comparison function
    FUNC3(GREATEST, inttype, inttype, inttype, inttype);
    FUNC3(GREATEST, realtype, realtype, realtype, realtype);
    FUNC3(LEAST, inttype, inttype, inttype, inttype);
    FUNC3(LEAST, realtype, realtype, realtype, realtype);
    FUNC3(INTERVAL, inttype, inttype, inttype, inttype);
    FUNC3(INTERVAL, inttype, realtype, realtype, realtype);

    // function
    FUNC1(ABS, inttype, inttype);
    FUNC1(ABS, realtype, realtype);
    FUNC1(ACOS, realtype, inttype);
    FUNC1(ACOS, realtype, realtype);
    FUNC1(ASIN, realtype, inttype);
    FUNC1(ASIN, realtype, realtype);
    FUNC1(ATAN, realtype, inttype);
    FUNC1(ATAN, realtype, realtype);
    FUNC2(ATAN, realtype, inttype, inttype);
    FUNC2(ATAN, realtype, realtype, realtype);
    FUNC2(ATAN2, realtype, inttype, inttype);
    FUNC2(ATAN2, realtype, realtype, realtype);
    FUNC1(CEILING, inttype, realtype);
    FUNC1(COS, realtype, inttype);
    FUNC1(COS, realtype, realtype);
    FUNC1(COT, realtype, inttype);
    FUNC1(COT, realtype, realtype);
    FUNC1(CRC32, realtype, texttype);
    FUNC1(DEGREES, realtype, inttype);
    FUNC1(DEGREES, realtype, realtype);
    FUNC1(EXP, realtype, inttype);
    FUNC1(EXP, realtype, realtype);
    FUNC1(FLOOR, inttype, realtype);
    FUNC1(LN, realtype, inttype);
    FUNC1(LN, realtype, realtype);
    FUNC1(LOG, realtype, inttype);
    FUNC1(LOG, realtype, realtype);
    FUNC2(LOG, realtype, inttype, inttype);
    FUNC2(LOG, realtype, realtype, realtype);
    FUNC1(LOG2, realtype, inttype);
    FUNC1(LOG2, realtype, realtype);
    FUNC1(LOG10, realtype, inttype);
    FUNC1(LOG10, realtype, realtype);
    FUNC(PI, realtype);
    FUNC2(POW, inttype, inttype, inttype);
    FUNC2(POW, realtype, realtype, realtype);
    FUNC1(RADIANS, realtype, inttype);
    FUNC1(RADIANS, realtype, realtype);
    FUNC1(ROUND, inttype, realtype);
    FUNC1(SIGN, inttype, inttype);
    FUNC1(SIGN, inttype, realtype);
    FUNC1(SIN, realtype, inttype);
    FUNC1(SIN, realtype, realtype);
    FUNC1(SQRT, realtype, inttype);
    FUNC1(SQRT, realtype, realtype);
    FUNC1(TAN, realtype, inttype);
    FUNC1(TAN, realtype, realtype);
    FUNC2(TRUNCATE, realtype, realtype, inttype);

    // datetime operation
    FUNC2(ADDDATE, datetype, datetype, inttype);
    FUNC2(DATEDIFF, inttype, datetype, datetype);
    // FUNC1(DATENAME, texttype, datetype); //FUNCTION testdb1.DATENAME does not exist
    FUNC1(DAYOFMONTH, inttype, datetype);
    FUNC1(DAYOFWEEK, inttype, datetype);
    FUNC1(DAYOFYEAR, inttype, datetype);
    FUNC1(HOUR, inttype, datetype);
    FUNC1(MINUTE, inttype, datetype);
    FUNC1(MONTH, inttype, datetype);
    FUNC1(MONTHNAME, texttype, datetype);
    FUNC1(QUARTER, inttype, datetype);
    FUNC1(SECOND, inttype, datetype);
    FUNC2(SUBDATE, datetype, datetype, inttype);
    FUNC1(TIME_TO_SEC, inttype, datetype);
    FUNC1(TO_DAYS, inttype, datetype);
    FUNC1(TO_SECONDS, inttype, datetype);
    FUNC1(UNIX_TIMESTAMP, inttype, datetype);
    FUNC1(WEEK, inttype, datetype);
    FUNC1(WEEKDAY, inttype, datetype);
    FUNC1(WEEKOFYEAR, inttype, datetype);
    FUNC1(YEAR, inttype, datetype);
    FUNC1(YEARWEEK, inttype, datetype);

    // string functions
    FUNC1(ASCII, inttype, texttype);
    FUNC1(BIN, texttype, inttype);
    FUNC1(BIT_LENGTH, inttype, texttype);
    FUNC1(CHAR_LENGTH, inttype, texttype);
    FUNC2(CONCAT, texttype, texttype, texttype);
    FUNC4(FIELD, inttype, texttype, texttype, texttype, texttype);
    FUNC2(LEFT, texttype, texttype, inttype);
    FUNC1(LENGTH, inttype, texttype);
    FUNC1(HEX, texttype, texttype);
    FUNC1(HEX, texttype, inttype);
    FUNC2(INSTR, inttype, texttype, texttype);
    FUNC2(LOCATE, inttype, texttype, texttype);
    FUNC1(LOWER, texttype, texttype);
    FUNC3(LPAD, texttype, texttype, inttype, texttype);
    FUNC1(LTRIM, texttype, texttype);
    FUNC4(MAKE_SET, texttype, inttype, texttype, texttype, texttype);
    FUNC1(OCT, texttype, inttype);
    FUNC1(ORD, inttype, texttype);
    FUNC1(QUOTE, texttype, texttype);
    FUNC2(REPEAT, texttype, texttype, inttype);
    FUNC3(REPLACE, texttype, texttype, texttype, texttype);
    FUNC1(REVERSE, texttype, texttype);
    FUNC2(RIGHT, texttype, texttype, inttype);
    FUNC3(RPAD, texttype, texttype, inttype, texttype);
    FUNC1(RTRIM, texttype, texttype);
    FUNC1(SOUNDEX, texttype, texttype);
    FUNC1(SPACE, texttype, inttype);
    FUNC3(SUBSTRING, texttype, texttype, inttype, inttype);
    FUNC1(TO_BASE64, texttype, texttype);
    FUNC1(TRIM, texttype, texttype);
    // FUNC1(UNHEX, texttype, texttype); it return a binary string, which is a different type from string. 
    // In case when, string and binary string will become binary string 
    FUNC1(UPPER, texttype, texttype);
    FUNC2(STRCMP, inttype, texttype, texttype);
    // FUNC1(CHAR, texttype, inttype);

    // bit function
    FUNC1(BIT_COUNT, inttype, inttype);

    // aggregate functions
    AGG1(AVG, realtype, inttype);
    AGG1(AVG, realtype, realtype);
    AGG1(BIT_AND, inttype, inttype);
    AGG1(BIT_OR, inttype, inttype);
    AGG1(BIT_XOR, inttype, inttype);
    AGG(COUNT, inttype);
    AGG1(COUNT, inttype, realtype);
    AGG1(COUNT, inttype, texttype);
    AGG1(COUNT, inttype, inttype);
    AGG1(MAX, realtype, realtype);
    AGG1(MAX, inttype, inttype);
    AGG1(MIN, realtype, realtype);
    AGG1(MIN, inttype, inttype);
    AGG1(STDDEV_POP, realtype, realtype);
    AGG1(STDDEV_POP, realtype, inttype);
    AGG1(STDDEV_SAMP, realtype, realtype);
    AGG1(STDDEV_SAMP, realtype, inttype);
    AGG1(SUM, realtype, realtype);
    AGG1(SUM, inttype, inttype);
    AGG1(VAR_POP, realtype, realtype);
    AGG1(VAR_POP, realtype, inttype);
    AGG1(VAR_SAMP, realtype, realtype);
    AGG1(VAR_SAMP, realtype, inttype);

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

void schema_ob::update_schema()
{
    // Loading tables...;
    string get_table_query = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
        WHERE TABLE_SCHEMA='" + test_db + "' AND \
              TABLE_TYPE='BASE TABLE' ORDER BY 1;";
    
    if (mysql_real_query(&conn, get_table_query.c_str(), get_table_query.size()))
        throw std::runtime_error(string(mysql_error(&conn)) + "\nLocation: " + debug_info);
    
    auto result = mysql_store_result(&conn);
    while (auto row = mysql_fetch_row(result)) {
        table tab(row[0], "main", true, true);
        tables.push_back(tab);
    }
    mysql_free_result(result);

    // Loading views...;
    string get_view_query = "select distinct table_name from information_schema.views \
        where table_schema='" + test_db + "' order by 1;";
    if (mysql_real_query(&conn, get_view_query.c_str(), get_view_query.size()))
        throw std::runtime_error(string(mysql_error(&conn)) + "\nLocation: " + debug_info);
    
    result = mysql_store_result(&conn);
    while (auto row = mysql_fetch_row(result)) {
        table tab(row[0], "main", false, false);
        tables.push_back(tab);
    }
    mysql_free_result(result);

    // Loading indexes...;
    string get_index_query = "SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS \
                WHERE TABLE_SCHEMA='" + test_db + "' AND \
                    NON_UNIQUE=1 AND \
                    INDEX_NAME <> COLUMN_NAME AND \
                    INDEX_NAME <> 'PRIMARY' ORDER BY 1;";
    if (mysql_real_query(&conn, get_index_query.c_str(), get_index_query.size()))
        throw std::runtime_error(string(mysql_error(&conn)) + "\nLocation: " + debug_info);

    result = mysql_store_result(&conn);
    while (auto row = mysql_fetch_row(result)) {
        indexes.push_back(row[0]);
    }
    mysql_free_result(result);

    // Loading columns and constraints...;
    for (auto& t : tables) {
        string get_column_query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS \
                WHERE TABLE_NAME='" + t.ident() + "' AND \
                    TABLE_SCHEMA='" + test_db + "'  ORDER BY ORDINAL_POSITION;";
        if (mysql_real_query(&conn, get_column_query.c_str(), get_column_query.size()))
            throw std::runtime_error(string(mysql_error(&conn)) + "\nLocation: " + debug_info + "\nTable: " + t.ident());
        result = mysql_store_result(&conn);
        while (auto row = mysql_fetch_row(result)) {
            column c(row[0], sqltype::get(row[1]));
            t.columns().push_back(c);
        }
        mysql_free_result(result);
    }
    return;
}

dut_ob::dut_ob(string db, unsigned int port, string host)
  : ob_connection(db, port, host)
{
}

static unsigned long long get_cur_time_ms(void) {
	struct timeval tv;
	struct timezone tz;

	gettimeofday(&tv, &tz);

	return (tv.tv_sec * 1000ULL) + tv.tv_usec / 1000;
}

void dut_ob::test(const string &stmt, 
    vector<vector<string>>* output, 
    int* affected_row_num,
    vector<string>* env_setting_stmts)
{
    if (env_setting_stmts != NULL) {
        for (auto& set_statement : *env_setting_stmts) {
            if (!mysql_real_query(&conn, set_statement.c_str(), stmt.size()))
                continue; // succeed
            
            string err = mysql_error(&conn);
            auto result = mysql_store_result(&conn);
            mysql_free_result(result);
            throw runtime_error("[OCEANBASE] setting error [" + err + "]");
        }
    }
    
    if (mysql_real_query(&conn, stmt.c_str(), stmt.size())) {
        string err = mysql_error(&conn);
        auto result = mysql_store_result(&conn);
        mysql_free_result(result);
        if (err.find("Commands out of sync") != string::npos) {// occasionally happens, retry the statement again
            cerr << err << " in test, repeat the statement again" << endl;
            test(stmt, output, affected_row_num);
            return;
        }
        if (regex_match(err, e_crash)) {
            throw std::runtime_error("BUG!!! " + err + " in dut_ob::test"); 
        }
        string prefix = "dut_ob::test expected error: ";
        if (regex_match(err, e_dup_entry) 
            || regex_match(err, e_large_results) 
            || regex_match(err, e_timeout) 
            || regex_match(err, e_timeout2) 
            || regex_match(err, e_col_ambiguous)
            || regex_match(err, e_truncated) 
            || regex_match(err, e_division_zero)
            || regex_match(err, e_unknown_col) 
            || regex_match(err, e_incorrect_args)
            || regex_match(err, e_out_of_range) 
            || regex_match(err, e_win_context)
            || regex_match(err, e_win_context2)
            || regex_match(err, e_view_reference) 
            || regex_match(err, e_context_cancel)
            || regex_match(err, e_string_convert)
            // || regex_match(err, e_idx_oor)
            || regex_match(err, e_col_null)
            || regex_match(err, e_sridb_pk)
            || regex_match(err, e_syntax)
            // || regex_match(err, e_expr_pushdown)
            || regex_match(err, e_invalid_group)
            || regex_match(err, e_invalid_group_2)
            || regex_match(err, e_oom)
            // || regex_match(err, e_cannot_column)
            || regex_match(err, e_schema_changed)
            // || regex_match(err, e_invalid_addr)
            // || regex_match(err, e_makeslice)
            // || regex_match(err, e_undef_win)
            || regex_match(err, e_over_mem)
            || regex_match(err, e_over_mem2)
            || regex_match(err, e_over_mem3)
            || regex_match(err, e_no_default)
            || regex_match(err, e_no_group_by)
            || regex_match(err, e_no_support_1)
            || regex_match(err, e_no_support_2)
            || regex_match(err, e_invalid_arguement)
            || regex_match(err, e_incorrect_string)
            || regex_match(err, e_long_specified_key)
            || regex_match(err, e_out_of_range_2)
            || regex_match(err, e_table_not_exists)
           ) {
            throw runtime_error(prefix + err);
        }

        throw std::runtime_error("[" + err + "] in dut_ob::test"); 
    }

    if (affected_row_num)
        *affected_row_num = mysql_affected_rows(&conn);

    auto result = mysql_store_result(&conn);
    if (mysql_errno(&conn) != 0) {
        string err = mysql_error(&conn);
        if (regex_match(err, e_out_of_range)
            || regex_match(err, e_string_convert)
            || regex_match(err, e_table_not_exists)
            ) {
            throw runtime_error("expected error: " + err);
        }

        throw std::runtime_error("[" + err + "]\nLocation: " + debug_info); 
    }

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
                row_output.push_back(process_an_item(str));
            }
            output->push_back(row_output);
        }
    }
    mysql_free_result(result);

    return;
}

void dut_ob::reset(void)
{
    string drop_sql = "drop database if exists " + test_db + "; ";
    if (mysql_real_query(&conn, drop_sql.c_str(), drop_sql.size())) {
        string err = mysql_error(&conn);
        throw std::runtime_error(err + "\nLocation: " + debug_info);
    }
    auto res_sql = mysql_store_result(&conn);
    mysql_free_result(res_sql);

    string create_sql = "create database " + test_db + "; ";
    if (mysql_real_query(&conn, create_sql.c_str(), create_sql.size())) {
        string err = mysql_error(&conn);
        throw std::runtime_error(err + "\nLocation: " + debug_info);
    }
    res_sql = mysql_store_result(&conn);
    mysql_free_result(res_sql);

    string use_sql = "use " + test_db + "; ";
    if (mysql_real_query(&conn, use_sql.c_str(), use_sql.size())) {
        string err = mysql_error(&conn);
        throw std::runtime_error(err + "\nLocation: " + debug_info);
    }
    res_sql = mysql_store_result(&conn);
    mysql_free_result(res_sql);
}

void dut_ob::backup(void)
{
    // auto backup_name = "/tmp/" + test_db + "_bk.sql";
    // string mysql_dump = "/usr/local/mysql/bin/mysqldump -h 127.0.0.1 -P " + to_string(test_port) + " -u root " + test_db + " > " + backup_name;
    // int ret = system(mysql_dump.c_str());
    // if (ret != 0) {
    //     cerr << "backup fail in dut_tidb::backup!!" << endl;
    //     throw std::runtime_error("backup fail in dut_tidb::backup"); 
    // }
}

void dut_ob::reset_to_backup(void)
{
    reset();
    auto stmt_queue = process_dbrecord_into_sqls(DB_RECORD_FILE);
    for (auto stmt : stmt_queue) {
        test(stmt);
    }
}

int dut_ob::save_backup_file(string testdb, string path)
{
    // string bk_file = "/tmp/" + testdb + "_bk.sql";
    // string cp_cmd = "cp " + bk_file + " " + path;
    // return system(cp_cmd.c_str());
    return 0;
}

void dut_ob::get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content)
{
    for (auto& table:tables_name) {
        vector<vector<string>> table_content;
        auto query = "SELECT * FROM " + table + " ORDER BY 1;";

        if (mysql_real_query(&conn, query.c_str(), query.size())) {
            string err = mysql_error(&conn);
            cerr << "Cannot get content of " + table + "\nLocation: " + debug_info << endl;
            cerr << "Error: " + err + "\nLocation: " + debug_info << endl;
            continue;
        }

        auto result = mysql_store_result(&conn);
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
                    row_output.push_back(process_an_item(str));
                }
                table_content.push_back(row_output);
            }
        }
        mysql_free_result(result);

        content[table] = table_content;
    }
}

