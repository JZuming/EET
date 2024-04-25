#include "clickhouse.hh"
#include <unistd.h>
#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

#define CLICKHOUSE_OUTPUT_FILE "clickhouse_output"
#define CLICKHOUSE_ERROR_FILE "clickhouse_error"
#define CLICKHOUSE_SQL_FILE "clickhouse_sql"

#define CLICKHOUSE_OUTPUT_SEPARATE '\t'
#define CLICKHOUSE_TIMEOUT_SECOND 6
#define CLICKHOUSE_GET_TABLES(db) \
    ("SELECT name FROM system.tables WHERE database = '" + db + "' and engine <> 'View';")
#define CLICKHOUSE_GET_VIEWS(db) \
    ("SELECT name FROM system.tables WHERE database = '" + db + "' and engine = 'View';")
#define CLICKHOUSE_GET_COLUMNS(table) \
    ("DESCRIBE " + table + ";")

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
        if (result == 0) // result can be -0, represent it as 0
            final_str = "0";
        else {
            stringstream ss;
            ss << setiosflags(ios::fixed) << setprecision(0) << result;
            final_str = ss.str();
        }
    }
    return final_str;
}

// true: execute query successfully
// false: trigger error, the error is store in err
// if db is NULL, don't specify database
bool execute_clickhouse_query(string* db, 
                    int port, string query, 
                    vector<vector<string>>* output, 
                    string& err,
                    vector<string>* env_setting_stmts)
{
    ofstream ofile(CLICKHOUSE_SQL_FILE);
    if (env_setting_stmts != NULL) {
        for (auto& setting_stmt : *env_setting_stmts)
            ofile << setting_stmt << endl;
    }
    ofile << query << endl;
    ofile.close();
    
    string cmd = "clickhouse client ";
    cmd = cmd + " --max_execution_time " + to_string(CLICKHOUSE_TIMEOUT_SECOND);
    cmd = cmd + " --port " + to_string(port);
    if (db != NULL)
        cmd = cmd + " --database " + *db;
    
    // // add it to prevent producing the duplicate bug as https://github.com/ClickHouse/ClickHouse/issues/50039 (fixed)
    // cmd = cmd + " --compile_expressions 0"; 

    // sometimes still undetermined even enable this options
    // cmd = cmd + " --mutations_sync 1"; // sychronize update/delete statement, otherwise undetermined results
    
    // cmd = cmd + " --join_use_nulls 1"; // without it, empty cell will be filled with default value
    cmd = cmd + " --queries-file " + string(CLICKHOUSE_SQL_FILE);
    cmd = cmd + " > " + string(CLICKHOUSE_OUTPUT_FILE);
    cmd = cmd + " 2> " + string(CLICKHOUSE_ERROR_FILE);

    auto ret = system(cmd.c_str());
    ifstream sql_error(CLICKHOUSE_ERROR_FILE);
    stringstream err_buffer;
    err_buffer << sql_error.rdbuf();
    sql_error.close();

    err = string(err_buffer.str());
    if (!err.empty()) {
        auto pos = err.find("(query:");
        if (pos != string::npos)
            err = err.substr(0, pos);
        return false;
    }
    
    if (output == NULL)
        return true;
    
    output->clear();

    ifstream sql_output(CLICKHOUSE_OUTPUT_FILE);
    stringstream output_buffer;
    output_buffer << sql_output.rdbuf();
    sql_output.close();
    string out(output_buffer.str());

    string item;
    vector<string> row;
    for (int i = 0; i < out.size(); i++) {
        if (out[i] == '\n') {
            row.push_back(process_number_string(item));
            output->push_back(row);
            row.clear();
            item.clear();
        }
        else if (out[i] == CLICKHOUSE_OUTPUT_SEPARATE) {
            row.push_back(process_number_string(item));
            item.clear();
        }
        else {
            item += out[i];
        }
    }
    // handle the last line
    if (row.empty() == false) {
        row.push_back(process_number_string(item));
        output->push_back(row);
        row.clear();
        item.clear();
    }

    return true;
}

clickhouse_connection::clickhouse_connection(string db, int port)
{
    test_db = db;
    test_port = port;
    
    // make sure clickhouse server is still alive and db is created 
    string error;
    string create_sql = "create database if not exists " + test_db + ";";
    auto succeed = execute_clickhouse_query(NULL, test_port, create_sql, NULL, error, NULL);
    if (!succeed) {
        throw runtime_error("CONNECTION ERROR [" + error + "]");
    }
}

schema_clickhouse::schema_clickhouse(string db, int port) : clickhouse_connection(db, port)
{
    vector<vector<string>> output;
    string error;
    // cerr << "Loading tables...";
    string query_tables = CLICKHOUSE_GET_TABLES(test_db);
    auto succeed = execute_clickhouse_query(&test_db, test_port, query_tables, &output, error, NULL);
    if (succeed == false)
        throw runtime_error("ERROR IN LOADING TABLES [" + error + "]");
    for (auto& row : output) {
        table tab(row[0], "main", true, true);
        tables.push_back(tab);
    }
    // cerr << "done." << endl;

    // cerr << "Loading views...";
    string query_views = CLICKHOUSE_GET_VIEWS(test_db);
    succeed = execute_clickhouse_query(&test_db, test_port, query_views, &output, error, NULL);
    if (succeed == false)
        throw runtime_error("ERROR IN LOADING VIEWS [" + error + "]");
    for (auto& row : output) {
        table tab(row[0], "main", false, false);
        tables.push_back(tab);
    }
    // cerr << "done." << endl;

    // do not need to get index

    // cerr << "Loading columns and constraints...";
    for (auto& t : tables) {
        string query_columns = CLICKHOUSE_GET_COLUMNS(t.ident());
        succeed = execute_clickhouse_query(&test_db, test_port, query_columns, &output, error, NULL);
        if (succeed == false)
            throw runtime_error("ERROR IN LOADING COLUMNS [" + error + "]");
        for (auto& row : output) {
            column c(row[0], sqltype::get(row[1]));
            t.columns().push_back(c);
        }
    }
    // cerr << "done." << endl;

    booltype = sqltype::get("Bool");
    inttype = sqltype::get("UInt32");
    realtype = sqltype::get("Float32");
    texttype = sqltype::get("String");
    datetype = sqltype::get("DateTime");

    compound_operators.push_back("union distinct");
    compound_operators.push_back("union all");
    compound_operators.push_back("intersect");
    compound_operators.push_back("except");

    // standard sql join
    supported_join_op.push_back("inner");
    supported_join_op.push_back("left outer");
    supported_join_op.push_back("right outer");
    supported_join_op.push_back("full outer");

    // cross join may produce inconsistent results: https://github.com/ClickHouse/ClickHouse/issues/50211
    // supported_join_op.push_back("cross");

    // additional join supported by ClickHouse
    // According to https://clickhouse.com/blog/clickhouse-fully-supports-joins,
    // SEMI JOIN, ANY JOIN and ASOF JOIN will cause non-determinism.
    // For example, A LEFT SEMI JOIN query returns column values for each row 
    // from the left table that has at least one join key match in the 
    // right table. Only the **first found** (undetermined) match is returned 
    // (the cartesian product is disabled).
    
    // supported_join_op.push_back("LEFT SEMI"); 
    // supported_join_op.push_back("RIGHT SEMI");
    // supported_join_op.push_back("LEFT ANY");
    // supported_join_op.push_back("RIGHT ANY");
    // supported_join_op.push_back("INNER ANY");
    // supported_join_op.push_back("ASOF");
    // supported_join_op.push_back("LEFT ASOF");

    // ANTI JOIN may produce garbage: https://github.com/ClickHouse/ClickHouse/issues/50198
    // supported_join_op.push_back("LEFT ANTI");
    // supported_join_op.push_back("RIGHT ANTI");

    // table engine
    supported_table_engine.push_back("MergeTree");
    supported_table_engine.push_back("ReplacingMergeTree");
    supported_table_engine.push_back("SummingMergeTree");
    supported_table_engine.push_back("AggregatingMergeTree");
    // supported_table_engine.push_back("CollapsingMergeTree"); // need parameters
    // supported_table_engine.push_back("VersionedCollapsingMergeTree"); // need parameters
    // supported_table_engine.push_back("GraphiteMergeTree"); // need parameters

    supported_table_engine.push_back("StripeLog"); //doesn't support mutations (e.g. UPDATE)
    supported_table_engine.push_back("Log"); //doesn't support mutations (e.g. UPDATE)
    supported_table_engine.push_back("TinyLog"); //doesn't support mutations (e.g. UPDATE)

    target_dbms = "clickhouse";
    
    // Arithmetic
    BINOP(+, inttype, inttype, inttype);
    BINOP(+, realtype, realtype, realtype);
    BINOP(-, inttype, inttype, inttype);
    BINOP(-, realtype, realtype, realtype);
    BINOP(*, inttype, inttype, inttype);
    BINOP(*, realtype, realtype, realtype);
    // do use mod or divide as they will produce non-determined results

    // Comparison
    BINOP(=, inttype, inttype, booltype);
    BINOP(=, realtype, realtype, booltype);
    BINOP(=, texttype, texttype, booltype);
    BINOP(<>, inttype, inttype, booltype);
    BINOP(<>, realtype, realtype, booltype);
    BINOP(<>, texttype, texttype, booltype);
    BINOP(<, inttype, inttype, booltype);
    BINOP(<, realtype, realtype, booltype);
    BINOP(<, texttype, texttype, booltype);
    BINOP(>, inttype, inttype, booltype);
    BINOP(>, realtype, realtype, booltype);
    BINOP(>, texttype, texttype, booltype);
    BINOP(<=, inttype, inttype, booltype);
    BINOP(<=, realtype, realtype, booltype);
    BINOP(<=, texttype, texttype, booltype);
    BINOP(>=, inttype, inttype, booltype);
    BINOP(>=, realtype, realtype, booltype);
    BINOP(>=, texttype, texttype, booltype);

    // BINOP(LIKE, texttype, texttype, booltype);
    // BINOP(NOT LIKE, texttype, texttype, booltype);
    // BINOP(ILIKE, texttype, texttype, booltype);

    // Logical
    BINOP(and, booltype, booltype, booltype);
    BINOP(or, booltype, booltype, booltype);

    // Concatenation
    BINOP(||, texttype, texttype, texttype);

    // Arithmetic Functions
    // FUNC2(intDiv, inttype, inttype, inttype);
    // FUNC2(intDiv, inttype, realtype, realtype);
    FUNC2(intDivOrZero, inttype, inttype, inttype); // UInt
    FUNC2(intDivOrZero, inttype, realtype, realtype);
    FUNC2(moduloOrZero, inttype, inttype, inttype);
    FUNC2(moduloOrZero, realtype, realtype, realtype);
    // FUNC2(positiveModulo, inttype, inttype, inttype);
    // FUNC2(positiveModulo, realtype, realtype, realtype);
    FUNC1(abs, inttype, inttype);
    FUNC1(abs, realtype, realtype);
    FUNC2(gcd, inttype, inttype, inttype);
    FUNC2(lcm, inttype, inttype, inttype);
    FUNC2(max2, realtype, inttype, inttype);
    FUNC2(max2, realtype, realtype, realtype);
    FUNC2(min2, realtype, inttype, inttype);
    FUNC2(min2, realtype, realtype, realtype);

    // Bit Functions
    FUNC2(bitAnd, inttype, inttype, inttype);
    FUNC2(bitOr, inttype, inttype, inttype);
    FUNC2(bitXor, inttype, inttype, inttype);
    FUNC1(bitNot, inttype, inttype);

    // Enable these four it again after the fix for https://github.com/ClickHouse/ClickHouse/issues/50236
    // the issues have been fixed now
    FUNC2(bitShiftLeft, inttype, inttype, inttype);
    FUNC2(bitShiftLeft, texttype, texttype, inttype);
    FUNC2(bitShiftRight, inttype, inttype, inttype);
    FUNC2(bitShiftRight, texttype, texttype, inttype);

    FUNC2(bitRotateLeft, inttype, inttype, inttype);
    FUNC2(bitRotateRight, inttype, inttype, inttype);

    FUNC3(bitSlice, texttype, texttype, inttype, inttype);
    FUNC2(bitTest, booltype, inttype, inttype);
    FUNC4(bitTestAll, booltype, inttype, inttype, inttype, inttype);
    FUNC4(bitTestAny, booltype, inttype, inttype, inttype, inttype);
    FUNC1(bitCount, inttype, inttype);
    FUNC2(bitHammingDistance, inttype, inttype, inttype);

    // Conditional Functions
    FUNC3(if, inttype, booltype, inttype, inttype);
    FUNC3(if, texttype, booltype, texttype, texttype);
    FUNC3(if, realtype, booltype, realtype, realtype);
    FUNC3(if, booltype, booltype, booltype, booltype);
    FUNC5(multiIf, inttype, booltype, inttype, booltype, inttype, inttype);
    FUNC5(multiIf, texttype, booltype, texttype, booltype, texttype, texttype);
    FUNC5(multiIf, realtype, booltype, realtype, booltype, realtype, realtype);
    FUNC5(multiIf, booltype, booltype, booltype, booltype, booltype, booltype);

    // Encoding Functions
    FUNC5(char, texttype, inttype, inttype, inttype, inttype, inttype);
    FUNC5(char, texttype, realtype, realtype, realtype, realtype, realtype);
    FUNC1(hex, texttype, inttype);
    FUNC1(hex, texttype, realtype);
    FUNC1(hex, texttype, texttype);
    FUNC1(unhex, texttype, texttype);
    FUNC1(bin, texttype, inttype);
    FUNC1(bin, texttype, realtype);
    FUNC1(bin, texttype, texttype);
    FUNC1(unbin, texttype, texttype);
    FUNC1(bitmaskToList, texttype, inttype);
    
    // Hash function causes a false positive: https://github.com/ClickHouse/ClickHouse/issues/50320
    // // Hash Functions
    // FUNC3(halfMD5, inttype, inttype, realtype, texttype);
    // FUNC1(MD4, texttype, texttype);
    // FUNC1(MD5, texttype, texttype);
    // FUNC3(sipHash64, inttype, inttype, realtype, texttype);
    // FUNC3(sipHash128, texttype, inttype, realtype, texttype);
    // FUNC3(sipHash128Reference, texttype, inttype, realtype, texttype);
    // FUNC3(cityHash64, inttype, inttype, realtype, texttype);
    // FUNC1(intHash32, inttype, inttype);
    // FUNC1(SHA1, texttype, texttype);
    // FUNC1(SHA224, texttype, texttype);
    // FUNC1(SHA256, texttype, texttype);
    // FUNC1(SHA512, texttype, texttype);
    // // FUNC1(BLAKE3, texttype, texttype);
    // FUNC3(farmHash64, inttype, inttype, realtype, texttype);
    // FUNC1(javaHash, inttype, inttype);
    // FUNC1(javaHash, inttype, texttype);
    // FUNC1(javaHashUTF16LE, inttype, texttype);
    // FUNC1(hiveHash, inttype, texttype);
    // // clickhouse has a lot hash function, TODO: add all of them

    // Logical Functions
    FUNC2(xor, booltype, booltype, booltype);

    // Mathematical Functions
    FUNC(e, realtype);
    FUNC(pi, realtype);
    FUNC1(exp, realtype, realtype);
    FUNC1(exp, realtype, inttype);
    FUNC1(log, realtype, realtype);
    FUNC1(log, realtype, inttype);
    FUNC1(exp2, realtype, realtype);
    FUNC1(exp2, realtype, inttype);

    // produce undetermined results, see https://github.com/ClickHouse/ClickHouse/issues/50048
    // FUNC1(intExp2, inttype, realtype);
    // FUNC1(intExp2, inttype, inttype);

    FUNC1(log2, realtype, realtype);
    FUNC1(log2, realtype, inttype);
    FUNC1(exp10, realtype, realtype);
    FUNC1(exp10, realtype, inttype);

    // produce undetermined results, see https://github.com/ClickHouse/ClickHouse/issues/50048
    // FUNC1(intExp10, inttype, realtype);
    // FUNC1(intExp10, inttype, inttype);

    FUNC1(log10, realtype, realtype);
    FUNC1(log10, realtype, inttype);
    FUNC1(sqrt, realtype, realtype);
    FUNC1(sqrt, realtype, inttype);
    FUNC1(cbrt, realtype, realtype);
    FUNC1(cbrt, realtype, inttype);
    FUNC1(erf, realtype, inttype);
    FUNC1(erf, realtype, realtype);
    FUNC1(erfc, realtype, inttype);
    FUNC1(erfc, realtype, realtype);
    FUNC1(lgamma, realtype, inttype);
    FUNC1(lgamma, realtype, realtype);
    FUNC1(tgamma, realtype, inttype);
    FUNC1(tgamma, realtype, realtype);
    FUNC1(sin, realtype, inttype);
    FUNC1(sin, realtype, realtype);
    FUNC1(cos, realtype, inttype);
    FUNC1(cos, realtype, realtype);
    FUNC1(tan, realtype, inttype);
    FUNC1(tan, realtype, realtype);
    FUNC1(asin, realtype, inttype);
    FUNC1(asin, realtype, realtype);
    FUNC1(acos, realtype, inttype);
    FUNC1(acos, realtype, realtype);
    FUNC1(atan, realtype, inttype);
    FUNC1(atan, realtype, realtype);
    FUNC2(pow, realtype, realtype, realtype);
    FUNC1(cosh, realtype, realtype);
    FUNC1(acosh, realtype, realtype);
    FUNC1(sinh, realtype, realtype);
    FUNC1(asinh, realtype, realtype);
    FUNC1(atanh, realtype, realtype);
    FUNC2(atan2, realtype, realtype, realtype);
    FUNC2(hypot, realtype, realtype, realtype);
    FUNC1(log1p, realtype, realtype);
    FUNC1(sign, inttype, inttype);
    FUNC1(sign, inttype, realtype);
    FUNC1(degrees, realtype, realtype);
    FUNC1(radians, realtype, realtype);
    FUNC1(factorial, inttype, inttype);
    FUNC4(width_bucket, inttype, realtype, realtype, realtype, inttype);
    FUNC4(width_bucket, inttype, inttype, inttype, inttype, inttype);

    // other functions
    FUNC(hostName, texttype);
    FUNC(FQDN, texttype);
    FUNC1(basename, texttype, texttype);
    FUNC1(visibleWidth, inttype, inttype);
    FUNC1(visibleWidth, inttype, realtype);
    FUNC1(visibleWidth, inttype, texttype);
    FUNC1(visibleWidth, inttype, booltype);
    FUNC1(toTypeName, texttype, inttype);
    FUNC1(toTypeName, texttype, realtype);
    FUNC1(toTypeName, texttype, texttype);
    FUNC1(toTypeName, texttype, booltype);
    FUNC1(materialize, inttype, inttype);
    FUNC1(materialize, realtype, realtype);
    FUNC1(materialize, texttype, texttype);
    FUNC1(materialize, booltype, booltype);
    FUNC1(ignore, inttype, inttype);
    FUNC1(ignore, inttype, realtype);
    FUNC1(ignore, inttype, texttype);
    FUNC1(ignore, inttype, booltype);
    FUNC(currentDatabase, texttype);
    FUNC(currentUser, texttype);
    FUNC1(isConstant, inttype, inttype); // acutal Uint8
    FUNC1(isConstant, inttype, realtype);
    FUNC1(isConstant, inttype, texttype);
    FUNC1(isConstant, inttype, booltype);
    FUNC1(isFinite, inttype, realtype);
    FUNC1(isInfinite, inttype, realtype);
    FUNC2(ifNotFinite, realtype, realtype, realtype);
    FUNC1(isNaN, inttype, realtype);
    FUNC4(bar, texttype, inttype, inttype, inttype, inttype);
    FUNC1(formatReadableDecimalSize, texttype, inttype);
    FUNC1(formatReadableSize, texttype, inttype);
    FUNC1(formatReadableQuantity, texttype, inttype);
    FUNC1(formatReadableTimeDelta, texttype, inttype);
    FUNC2(least, inttype, inttype, inttype);
    FUNC2(least, realtype, realtype, realtype);
    FUNC2(least, texttype, texttype, texttype);
    FUNC2(greatest, inttype, inttype, inttype);
    FUNC2(greatest, realtype, realtype, realtype);
    FUNC2(greatest, texttype, texttype, texttype);
    FUNC(version, texttype);
    FUNC1(toColumnTypeName, texttype, inttype);
    FUNC1(toColumnTypeName, texttype, realtype);
    FUNC1(toColumnTypeName, texttype, texttype);
    // toColumnTypeName cannot be used because the results change if using [case when ... else null end]
    // dumpColumnStructure cannot be used because the results change if using [case when ... else null end]
    // defaultValueOfArgumentType cannot be used because the results change if using [case when ... else null end]
    FUNC1(identity, inttype, inttype);
    FUNC1(identity, realtype, realtype);
    FUNC1(identity, texttype, texttype);
    FUNC1(identity, booltype, booltype);
    FUNC1(countDigits, inttype, inttype);
    FUNC1(errorCodeToName, texttype, inttype);
    FUNC(getOSKernelVersion, texttype);

    // DateTime related function
    FUNC(timeZone, texttype);
    FUNC1(timeZoneOf, texttype, datetype);
    FUNC1(timeZoneOffset, inttype, datetype);
    FUNC1(toYear, inttype, datetype);
    FUNC1(toQuarter, inttype, datetype);
    FUNC1(toMonth, inttype, datetype);
    FUNC1(toDayOfYear, inttype, datetype);
    FUNC1(toDayOfMonth, inttype, datetype);
    FUNC1(toDayOfWeek, inttype, datetype);
    FUNC1(toHour, inttype, datetype);
    FUNC1(toMinute, inttype, datetype);
    FUNC1(toSecond, inttype, datetype);
    FUNC1(toUnixTimestamp, inttype, datetype);
    FUNC1(toStartOfDay, datetype, datetype);
    FUNC1(toStartOfHour, datetype, datetype);
    FUNC1(toStartOfMinute, datetype, datetype);
    FUNC1(toStartOfFiveMinutes, datetype, datetype);
    FUNC1(toStartOfTenMinutes, datetype, datetype);
    FUNC1(toStartOfFifteenMinutes, datetype, datetype);
    FUNC1(toTime, datetype, datetype);
    FUNC1(toRelativeYearNum, inttype, datetype);
    FUNC1(toRelativeQuarterNum, inttype, datetype);
    FUNC1(toRelativeMonthNum, inttype, datetype);
    FUNC1(toRelativeWeekNum, inttype, datetype);
    FUNC1(toRelativeDayNum, inttype, datetype);
    FUNC1(toRelativeHourNum, inttype, datetype);
    FUNC1(toRelativeMinuteNum, inttype, datetype);
    FUNC1(toRelativeSecondNum, inttype, datetype);
    FUNC1(toISOYear, inttype, datetype);
    FUNC1(toISOWeek, inttype, datetype);
    FUNC1(toWeek, inttype, datetype);
    FUNC1(toYearWeek, inttype, datetype);
    FUNC1(toYYYYMM, inttype, datetype);
    FUNC1(toYYYYMMDD, inttype, datetype);
    FUNC1(toYYYYMMDDhhmmss, inttype, datetype);
    FUNC2(addYears, datetype, datetype, inttype);
    FUNC2(addMonths, datetype, datetype, inttype);
    FUNC2(addWeeks, datetype, datetype, inttype);
    FUNC2(addDays, datetype, datetype, inttype);
    FUNC2(addHours, datetype, datetype, inttype);
    FUNC2(addMinutes, datetype, datetype, inttype);
    FUNC2(addSeconds, datetype, datetype, inttype);
    FUNC2(addQuarters, datetype, datetype, inttype);
    FUNC2(subtractYears, datetype, datetype, inttype);
    FUNC2(subtractMonths, datetype, datetype, inttype);
    FUNC2(subtractWeeks, datetype, datetype, inttype);
    FUNC2(subtractDays, datetype, datetype, inttype);
    FUNC2(subtractHours, datetype, datetype, inttype);
    FUNC2(subtractMinutes, datetype, datetype, inttype);
    FUNC2(subtractSeconds, datetype, datetype, inttype);
    FUNC2(subtractQuarters, datetype, datetype, inttype);
    FUNC1(monthName, texttype, datetype);
    FUNC1(fromUnixTimestamp, datetype, inttype);

    // aggregate function
    AGG(count, inttype);
    AGG1(count, inttype, inttype);
    AGG1(count, inttype, texttype);
    AGG1(count, inttype, realtype);
    AGG1(min, inttype, inttype);
    AGG1(min, texttype, texttype);
    AGG1(min, realtype, realtype);
    AGG1(max, inttype, inttype);
    AGG1(max, texttype, texttype);
    AGG1(max, realtype, realtype);
    AGG1(sum, inttype, inttype);
    AGG1(sum, realtype, realtype);
    AGG1(avg, realtype, inttype);
    AGG1(avg, realtype, realtype);
    AGG1(varPop, realtype, inttype);
    AGG1(varPop, realtype, realtype);
    AGG1(stddevPop, realtype, inttype);
    AGG1(stddevPop, realtype, realtype);
    AGG1(varSamp, realtype, inttype);
    AGG1(varSamp, realtype, realtype);
    AGG1(stddevSamp, realtype, inttype);
    AGG1(stddevSamp, realtype, realtype);
    AGG2(covarPop, realtype, inttype, inttype);
    AGG2(covarPop, realtype, realtype, realtype);
    AGG2(covarSamp, realtype, inttype, inttype);
    AGG2(covarSamp, realtype, realtype, realtype);
    AGG2(corr, realtype, inttype, inttype);
    AGG2(corr, realtype, realtype, realtype);
    AGG1(sumWithOverflow, inttype, inttype);
    AGG1(sumWithOverflow, realtype, realtype);

    WIN(count, inttype);
    WIN(rank, inttype);
    WIN(dense_rank, inttype);
    WIN(row_number, inttype);

    internaltype = sqltype::get("internal");
    arraytype = sqltype::get("ARRAY");

    true_literal = "true";
    false_literal = "false";

    generate_indexes();
}

void dut_clickhouse::test(const string &stmt, 
                    vector<vector<string>>* output, 
                    int* affected_row_num,
                    vector<string>* env_setting_stmts)
{
    string error;
    auto succeed = execute_clickhouse_query(&test_db, test_port, stmt, output, error, env_setting_stmts);
    if (!succeed) {
        if (error.find("(BAD_ARGUMENTS)") != string::npos // not follow the argument scope
            || error.find("(ILLEGAL_DIVISION)") != string::npos // divided by zero
            || error.find("(NO_COMMON_TYPE)") != string::npos // here is no supertype for types UInt64, Int64
            || error.find("(NOT_IMPLEMENTED)") != string::npos 
            || error.find("(TIMEOUT_EXCEEDED)") != string::npos
            || error.find("(ILLEGAL_TYPE_OF_ARGUMENT)") != string::npos
            || error.find("(TYPE_MISMATCH)") != string::npos
            // || error.find("(SYNTAX_ERROR)") != string::npos
            || error.find("(ZERO_ARRAY_OR_TUPLE_INDEX)") != string::npos
            || error.find("(ILLEGAL_COLUMN)") != string::npos
            || error.find("(CANNOT_CONVERT_TYPE)") != string::npos
            || error.find("(UNFINISHED)") != string::npos
            || error.find("(ARGUMENT_OUT_OF_BOUND)") != string::npos
            || error.find("(ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER)") != string::npos
            || error.find("(MEMORY_LIMIT_EXCEEDED)") != string::npos // just happens occasionaly, not a bug
            || error.find("Max query size exceeded") != string::npos
            || error.find("(BAD_TYPE_OF_FIELD)") != string::npos
            || error.find("(TOO_MANY_QUERY_PLAN_OPTIMIZATIONS)") != string::npos // query is too complex
            || error.find("(DECIMAL_OVERFLOW)") != string::npos
            || error.find("(CANNOT_ALLOCATE_MEMORY)") != string::npos // Amount of memory requested to allocate is more than allowed
            ) {
            throw runtime_error("clickhouse test expected error [" + error + "]");
        }
        throw runtime_error("clickhouse test fails [" + error + "]");
    }

    if (affected_row_num)
        *affected_row_num = 1;
    
    return;
}

void dut_clickhouse::reset()
{
    string error;
    string drop_sql = "drop database if exists " + test_db + "; ";
    auto succeed = execute_clickhouse_query(NULL, test_port, drop_sql, NULL, error, NULL);
    if (!succeed) {
        throw runtime_error("CLICKHOUSE DROP fail in reset [" + error + "]");
    }

    string create_sql = "create database if not exists " + test_db + ";";
    succeed = execute_clickhouse_query(NULL, test_port, create_sql, NULL, error, NULL);
    if (!succeed) {
        throw runtime_error("CLICKHOUSE CREATE fail in reset [" + error + "]");
    }

    return;
}

void dut_clickhouse::backup()
{
    return; // do nothing because it get backup_file from the caller
}

void dut_clickhouse::reset_to_backup()
{
    reset();
    if (access(test_backup_file.c_str(), F_OK ) == -1) 
        throw runtime_error("CLICKHOUSE access fail in reset_to_backup");
    
    string cmd = "clickhouse client ";
    cmd = cmd + " --port " + to_string(test_port);
    cmd = cmd + " --database " + test_db;
    cmd = cmd + " --queries-file " + test_backup_file;
    cmd = cmd + " > " + string(CLICKHOUSE_OUTPUT_FILE);
    cmd = cmd + " 2> " + string(CLICKHOUSE_ERROR_FILE);

    auto ret = system(cmd.c_str());
    ifstream sql_error(CLICKHOUSE_ERROR_FILE);
    stringstream err_buffer;
    err_buffer << sql_error.rdbuf();
    sql_error.close();

    auto error = string(err_buffer.str());
    if (!error.empty()) 
        throw runtime_error("CLICKHOUSE execute queries-file fail in reset_to_backup + [" + error + "]");
    
    return;
}

void dut_clickhouse::get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content)
{
    for (auto& table : tables_name) {
        vector<vector<string>> table_content;
        string error;
        auto query = "SELECT * FROM " + table + " ORDER BY 1;";
        auto succeed = execute_clickhouse_query(&test_db, test_port, query, &table_content, error, NULL);
        if (!succeed) {
            throw runtime_error("CLICKHOUSE cannot get content + [" + error + "]");
        }
        content[table] = table_content;
    }

    return;
}

dut_clickhouse::dut_clickhouse(string db, int port, string backup_file)
    : clickhouse_connection(db, port)
{
    test_backup_file = backup_file;
}
