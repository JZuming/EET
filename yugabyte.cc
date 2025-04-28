#include "yugabyte.hh"
#include "config.h"
#include <iostream>
#include <iomanip>
#include <cmath>

#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

using namespace std;

#define YUGABYTE_TIMEOUT_SECOND 6
// #define PSQL_PATH "/app/yugabyte-db/bin/psql"
// #define YUGABYTE_HOST "127.0.1.1"
#define YUGABYTE_ROLE "yugabyte"

#define debug_info (string(__func__) + "(" + string(__FILE__) + ":" + to_string(__LINE__) + ")")

static bool has_types = false;
static vector<yugabyte_type *> static_type_vec;

static bool has_operators = false;
static vector<op> static_op_vec;

static bool has_routines = false;
static vector<routine> static_routine_vec;

static bool has_routine_para = false;
static map<string, vector<yugabyte_type *>> static_routine_para_map;

static bool has_aggregates = false;
static vector<routine> static_aggregate_vec;

static bool has_aggregate_para = false;
static map<string, vector<yugabyte_type *>> static_aggregate_para_map;

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
        if (str == "-Infinity")
            str  = "Infinity";
        final_str = str;
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

bool yugabyte_type::consistent(sqltype *rvalue)
{
    yugabyte_type *t = dynamic_cast<yugabyte_type*>(rvalue);
    if (!t) {
        cerr << "unknown type: " << rvalue->name  << endl;
        return false;
    }

    switch(typtype_) {
        case 'b': /* base type */
        case 'c': /* composite type */
        case 'd': /* domain */
        case 'r': /* range */
        case 'e': /* enum */
        case 'm': /* multirange */
            return this == t;
        case 'p':
            if (name == "anyarray") {
                return t->typelem_ != InvalidOid;
            } else if (name == "anynonarray") {
                return t->typelem_ == InvalidOid;
            } else if (name == "anyelement") {
                return t->typelem_ == InvalidOid;
            } else if(name == "anyenum") {
                return t->typtype_ == 'e';
            } else if (name == "anyrange") {
                return t->typtype_ == 'r';
            } else if (name == "record") {
                return t->typtype_ == 'c';
            } else if (name == "cstring") {
                return this == t;
            } else if (name == "any") {
                return true;
            } else if (name == "void") {
                return this == t;
            } else {
                return false;
            }
        default:
            cerr << "error type: " << name << " " << oid_ << " " << typdelim_ << " "
                << typrelid_ << " " << typelem_ << " " << typarray_ << " " << typtype_ << endl;
            cerr << "t type: " << t->name << " " << t->oid_ << " " << t->typdelim_ << " "
                << t->typrelid_ << " " << t->typelem_ << " " << t->typarray_ << " " << t->typtype_ << endl;
            throw std::logic_error("unknown typtype");
    }
}

static PGresult* pqexec_handle_error(PGconn *conn, string& query)
{
    auto res = PQexec(conn, query.c_str());
    auto status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
        string err = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(err + " in " + debug_info);
    }
    return res;
}

// true: the proc is suitable
static bool is_suitable_proc(string proc_name)
{
    if (proc_name.find("pg_") != string::npos)
        return false;
    
    if (proc_name == "clock_timestamp"
        || proc_name == "inet_client_port"
        || proc_name == "now"
        || proc_name.find("random") != string::npos 
        || proc_name == "statement_timestamp"
        || proc_name == "timeofday"
        || (proc_name.find("has_") != string::npos && proc_name.find("_privilege") != string::npos)
        // || proc_name == "has_table_privilege"
        // || proc_name == "has_function_privilege"
        // || proc_name == "has_tablespace_privilege"
        // || proc_name == "has_foreign_data_wrapper_privilege"
        // || proc_name == "has_database_privilege"
        // || proc_name == "has_server_privilege"
        // || proc_name == "has_schema_privilege"
        // || proc_name == "current_setting"
        || proc_name == "set_config"
        || proc_name.find("current") != string::npos 
        || proc_name == "row_security_active"
        || proc_name == "string_agg" // may generate random-ordered string
        || proc_name == "regr_slope" // may give undetermine result when the slope close to infinite or 0
        ) {
        return false;
    }

    return true;
}

bool schema_yugabyte::is_consistent_with_basic_type(sqltype *rvalue)
{
    if (booltype->consistent(rvalue) ||
        inttype->consistent(rvalue) ||
        realtype->consistent(rvalue) ||
        texttype->consistent(rvalue) ||
        datetype->consistent(rvalue))
        return true;
    
    return false;
}

schema_yugabyte::schema_yugabyte(string db, unsigned int port, string host, bool no_catalog)
    : yugabyte_connection(db, port, host)
{
    string version_sql = "select version();";
    auto res = pqexec_handle_error(conn, version_sql);
    version = PQgetvalue(res, 0, 0);
    PQclear(res);

    string version_num_sql = "SHOW server_version_num;";
    res = pqexec_handle_error(conn, version_num_sql);
    version_num = atoi(PQgetvalue(res, 0, 0));
    PQclear(res);

    // address the schema change in postgresql 11 that replaced proisagg and proiswindow with prokind
    string procedure_is_aggregate = version_num < 110000 ? "proisagg" : "prokind = 'a'";
    string procedure_is_window = version_num < 110000 ? "proiswindow" : "prokind = 'w'";

    // cerr << "Loading types...";
    if (has_types == false) {
        string load_type_sql = "select typname, oid, typdelim, typrelid, typelem, typarray, typtype "
            "from pg_type ;";
        res = pqexec_handle_error(conn, load_type_sql);
        auto row_num = PQntuples(res);
        for (int i = 0; i < row_num; i++) {
            string name(PQgetvalue(res, i, 0));
            OID oid = atol(PQgetvalue(res, i, 1));
            string typdelim(PQgetvalue(res, i, 2));
            OID typrelid = atol(PQgetvalue(res, i, 3));
            OID typelem = atol(PQgetvalue(res, i, 4));
            OID typarray = atol(PQgetvalue(res, i, 5));
            string typtype(PQgetvalue(res, i, 6));

            auto t = new yugabyte_type(name, oid, typdelim[0], typrelid, typelem, typarray, typtype[0]);
            static_type_vec.push_back(t);
        }
        PQclear(res);
        has_types = true;
    }
    for (auto t : static_type_vec) {
        oid2type[t->oid_] = t;
        name2type[t->name] = t;
        types.push_back(t);
    }

    if (name2type.count("bool") > 0 &&  // no boolean type in pg_type
            name2type.count("int4") > 0 && // no integer type in pg_type
            name2type.count("numeric") > 0 &&
            name2type.count("text") > 0 &&
            name2type.count("timestamp") > 0) {
        
        booltype = name2type["bool"];
        inttype = name2type["int4"];
        realtype = name2type["numeric"];
        texttype = name2type["text"];
        datetype = name2type["timestamp"];
    }
    else {
        cerr << "at least one of booltype, inttype, realtype, texttype is not exist in" << debug_info << endl;
        throw runtime_error("at least one of booltype, inttype, realtype, texttype is not exist in" + debug_info);
    }

    internaltype = name2type["internal"];
    arraytype = name2type["anyarray"];
    true_literal = "true";
    false_literal = "false";
    null_literal = "null";

    compound_operators.push_back("union");
    compound_operators.push_back("union all");
    compound_operators.push_back("intersect");
    compound_operators.push_back("intersect all");
    compound_operators.push_back("except");
    compound_operators.push_back("except all");

    supported_join_op.push_back("cross");
    supported_join_op.push_back("inner");
    supported_join_op.push_back("left outer");
    supported_join_op.push_back("right outer");
    supported_join_op.push_back("full outer");
    
    // Setting Configuration
    supported_setting["yb_enable_optimizer_statistics"] = vector<string>({"on", "off"});
    supported_setting["yb_enable_base_scans_cost_model"] = vector<string>({"on", "off"});

    // supported_setting["enable_async_append"] = vector<string>({"on", "off"});
    // supported_setting["enable_bitmapscan"] = vector<string>({"on", "off"});
    // supported_setting["enable_gathermerge"] = vector<string>({"on", "off"});
    // supported_setting["enable_hashagg"] = vector<string>({"on", "off"});
    // supported_setting["enable_hashjoin"] = vector<string>({"on", "off"});
    // supported_setting["enable_incremental_sort"] = vector<string>({"on", "off"});
    // supported_setting["enable_indexscan"] = vector<string>({"on", "off"});
    // supported_setting["enable_indexonlyscan"] = vector<string>({"on", "off"});
    // supported_setting["enable_material"] = vector<string>({"on", "off"});
    // supported_setting["enable_memoize"] = vector<string>({"on", "off"});
    // supported_setting["enable_mergejoin"] = vector<string>({"on", "off"});
    // supported_setting["enable_nestloop"] = vector<string>({"on", "off"});
    // supported_setting["enable_parallel_append"] = vector<string>({"on", "off"});
    // supported_setting["enable_parallel_hash"] = vector<string>({"on", "off"});
    // supported_setting["enable_partition_pruning"] = vector<string>({"on", "off"});
    // supported_setting["enable_partitionwise_join"] = vector<string>({"on", "off"});
    // supported_setting["enable_partitionwise_aggregate"] = vector<string>({"on", "off"});
    // supported_setting["enable_seqscan"] = vector<string>({"on", "off"});
    // supported_setting["enable_sort"] = vector<string>({"on", "off"});
    // supported_setting["enable_tidscan"] = vector<string>({"on", "off"});

    // // Genetic Query Optimizer
    // supported_setting["geqo"] = vector<string>({"on", "off"});

    // // Other Planner Options
    // supported_setting["constraint_exclusion"] = vector<string>({"on", "off", "partition"});
    // supported_setting["jit"] = vector<string>({"on", "off"});
    // supported_setting["plan_cache_mode"] = vector<string>({"auto", "force_custom_plan", "force_generic_plan"});

    target_dbms = "yugabyte";

    // cerr << "Loading tables...";
    string load_table_sql = "select table_name, \
                                 table_schema, \
                                 is_insertable_into, \
                                 table_type \
                             from information_schema.tables;";
    res = pqexec_handle_error(conn, load_table_sql);
    auto row_num = PQntuples(res);
    for (int i = 0; i < row_num; i++) {
        string table_name(PQgetvalue(res, i, 0));
        string schema(PQgetvalue(res, i, 1));
        string insertable(PQgetvalue(res, i, 2));
        string table_type(PQgetvalue(res, i, 3));

        if (no_catalog && ((schema == "pg_catalog") || (schema == "information_schema")))
            continue;
        
        tables.push_back(table(table_name, schema,
                ((insertable == "YES") ? true : false),
                ((table_type == "BASE TABLE") ? true : false)));
    }
    PQclear(res);    

    // cerr << "Loading columns and constraints...";
    for (auto t = tables.begin(); t != tables.end(); ++t) {
        string q("select attname, atttypid \
                  from pg_attribute join pg_class c on( c.oid = attrelid ) \
                    join pg_namespace n on n.oid = relnamespace \
                  where not attisdropped \
                    and attname not in \
                    ('xmin', 'xmax', 'ctid', 'cmin', 'cmax', 'tableoid', 'oid') ");
        q += " and relname = '" + t->name + "'";
        q += " and nspname = '" + t->schema + "';";

        res = pqexec_handle_error(conn, q);
        row_num = PQntuples(res);
        for (int i = 0; i < row_num; i++) {
            string column_name(PQgetvalue(res, i, 0));

            // ybctid is a default column but it cannot be inserted, so exclude this column
            if (column_name == "ybctid")
                continue;

            auto column_type = oid2type[atol(PQgetvalue(res, i, 1))];
            column c(column_name, column_type);            
            t->columns().push_back(c);
        }
        PQclear(res);

        q = "select conname from pg_class t "
                "join pg_constraint c on (t.oid = c.conrelid) "
                "where contype in ('f', 'u', 'p') ";
        q = q + " and relnamespace = (select oid from pg_namespace where nspname = '" + t->schema + "')";
        q = q + " and relname = '" + t->name + "';";

        res = pqexec_handle_error(conn, q);
        row_num = PQntuples(res);
        for (int i = 0; i < row_num; i++) {
            t->constraints.push_back(PQgetvalue(res, i, 0));
        }
        PQclear(res);
    }

    // cerr << "Loading operators...";
    if (has_operators == false) {
        string load_operators_sql = "select oprname, oprleft,"
                                    "oprright, oprresult "
                                "from pg_catalog.pg_operator "
                                "where 0 not in (oprresult, oprright, oprleft) ;";
        res = pqexec_handle_error(conn, load_operators_sql);
        row_num = PQntuples(res);
        for (int i = 0; i < row_num; i++) {
            string op_name(PQgetvalue(res, i, 0));
            auto op_left_type = oid2type[atol(PQgetvalue(res, i, 1))];
            auto op_right_type = oid2type[atol(PQgetvalue(res, i, 2))];
            auto op_result_type = oid2type[atol(PQgetvalue(res, i, 3))];

            // only consider basic type
            if (!is_consistent_with_basic_type(op_left_type) ||
                !is_consistent_with_basic_type(op_right_type) ||
                !is_consistent_with_basic_type(op_result_type))
                continue;

            op o(op_name, op_left_type, op_right_type, op_result_type);

            if (op_name == "&<|") {
                continue;
            }
            static_op_vec.push_back(o);
        }
        PQclear(res);
        has_operators = true;
    }
    for (auto& o:static_op_vec) {
        register_operator(o);
    }
    
    // cerr << "Loading routines...";
    if (has_routines == false) {
        string load_routines_sql = 
            "select (select nspname from pg_namespace where oid = pronamespace), oid, prorettype, proname "
            "from pg_proc "
            "where prorettype::regtype::text not in ('event_trigger', 'trigger', 'opaque', 'internal') "
                "and not (proretset or " + procedure_is_aggregate + " or " + procedure_is_window + ") ;";
        
        res = pqexec_handle_error(conn, load_routines_sql);
        row_num = PQntuples(res);
        for (int i = 0; i < row_num; i++) {
            string r_name(PQgetvalue(res, i, 0));
            string oid_str(PQgetvalue(res, i, 1));
            auto prorettype = oid2type[atol(PQgetvalue(res, i, 2))];
            string proname(PQgetvalue(res, i, 3));

            // only consider basic type
            if (!is_consistent_with_basic_type(prorettype))
                continue;

            if (!is_suitable_proc(proname))
                continue;
            
            routine proc(r_name, oid_str, prorettype, proname);
            static_routine_vec.push_back(proc);
        }
        PQclear(res);
        has_routines = true;
    }

    // cerr << "Loading routine parameters...";
    if (has_routine_para == false) {
        for (int i = 0; i < static_routine_vec.size(); i++) {
            auto& proc = static_routine_vec[i];
        // for (auto &proc : static_routine_vec) {
            string q("select unnest(proargtypes) from pg_proc ");
            q = q + " where oid = " + proc.specific_name + ";";

            res = pqexec_handle_error(conn, q);
            row_num = PQntuples(res);

            bool has_not_basic_type = false;
            vector <yugabyte_type *> para_vec;
            for (int i = 0; i < row_num; i++) {
                auto t = oid2type[atol(PQgetvalue(res, i, 0))];
                assert(t);
                if (!is_consistent_with_basic_type(t)) {
                    has_not_basic_type = true;
                    break;
                }
                para_vec.push_back(t);
            }
            if (has_not_basic_type) {
                static_routine_vec.erase(static_routine_vec.begin() + i);
                i--;
                continue;
            }
            static_routine_para_map[proc.specific_name] = para_vec;
            PQclear(res);
        }
        has_routine_para = true;
    }

    for (auto& proc:static_routine_vec) {
        register_routine(proc);
    }

    for (auto &proc : routines) {
        auto& para_vec = static_routine_para_map[proc.specific_name];
        for (auto t:para_vec) {
            proc.argtypes.push_back(t);
        }
    }

    // cerr << "Loading aggregates...";
    if (has_aggregates == false) {
        string load_aggregates_sql = 
            "select (select nspname from pg_namespace where oid = pronamespace), oid, prorettype, proname "
            "from pg_proc "
                "where prorettype::regtype::text not in ('event_trigger', 'trigger', 'opaque', 'internal') "
                "and proname not in ('pg_event_trigger_table_rewrite_reason') "
                "and proname not in ('percentile_cont', 'dense_rank', 'cume_dist', "
                "'rank', 'test_rank', 'percent_rank', 'percentile_disc', 'mode', 'test_percentile_disc') "
                "and proname !~ '^ri_fkey_' "
                "and not (proretset or " + procedure_is_window + ") "
                "and " + procedure_is_aggregate + ";";
        res = pqexec_handle_error(conn, load_aggregates_sql);
        row_num = PQntuples(res);
        for (int i = 0; i < row_num; i++) {
            string nspname(PQgetvalue(res, i, 0));
            string oid_str(PQgetvalue(res, i, 1));
            auto prorettype = oid2type[atol(PQgetvalue(res, i, 2))];
            string proname(PQgetvalue(res, i, 3));

            // only consider basic type
            if (!is_consistent_with_basic_type(prorettype))
                continue;

            if (!is_suitable_proc(proname))
                continue;

            routine proc(nspname, oid_str, prorettype, proname);
            static_aggregate_vec.push_back(proc);
        }
        PQclear(res);
        has_aggregates = true;
    }
    // for (auto& proc:static_aggregate_vec) {
    //     register_aggregate(proc);
    // }

    // cerr << "Loading aggregate parameters...";
    if (has_aggregate_para == false) {
        for (int i = 0; i < static_aggregate_vec.size(); i++) {
            auto& proc = static_aggregate_vec[i];
        // for (auto &proc : aggregates) {
            string q("select unnest(proargtypes) "
                "from pg_proc ");
            q = q + " where oid = " + proc.specific_name + ";";
            res = pqexec_handle_error(conn, q);
            row_num = PQntuples(res);
            
            bool has_not_basic_type = false;
            vector<yugabyte_type *> para_vec;
            for (int i = 0; i < row_num; i++) {
                auto t = oid2type[atol(PQgetvalue(res, i, 0))];
                assert(t);
                if (!is_consistent_with_basic_type(t)) {
                    has_not_basic_type = true;
                    break;
                }
                para_vec.push_back(t);
            }
            if (has_not_basic_type) {
                static_aggregate_vec.erase(static_aggregate_vec.begin() + i);
                i--;
                continue;
            }
            static_aggregate_para_map[proc.specific_name] = para_vec;
            PQclear(res);
        }
        has_aggregate_para = true;
    }

    for (auto& proc:static_aggregate_vec) {
        register_aggregate(proc);
    }

    for (auto &proc : aggregates) {
        auto& para_vec = static_aggregate_para_map[proc.specific_name];
        for (auto t:para_vec) {
            proc.argtypes.push_back(t);
        }
    }

    generate_indexes();
}

schema_yugabyte::~schema_yugabyte()
{
    // auto types_num = types.size();
    // for (int i = 0; i < types_num; i++) {
    //     pg_type* ptype = dynamic_cast<pg_type*>(types[i]);
    //     if (!ptype) // not a pg_type
    //         continue;
    //     types.erase(types.begin() + i);
    //     delete ptype;
    //     i--;
    //     types_num--;
    // }
}

yugabyte_connection::yugabyte_connection(string db, unsigned int port, string host)
{    
    test_db = db;
    test_port = port;
    host_addr = host;

    conn = PQsetdbLogin(host_addr.c_str(), to_string(port).c_str(), NULL, NULL, db.c_str(), YUGABYTE_ROLE, NULL);
    if (PQstatus(conn) == CONNECTION_OK)
        return; // succeed
    
    string err = PQerrorMessage(conn);
    if (err.find("does not exist") == string::npos) {
        cerr << "[CONNECTION FAIL]  " << err << " in " << debug_info << endl;
        throw runtime_error("[CONNECTION FAIL] " + err + " in " + debug_info);
    }
    
    cerr << "try to create database testdb" << endl;
    conn = PQsetdbLogin(host_addr.c_str(), to_string(test_port).c_str(), NULL, NULL, "yugabyte", YUGABYTE_ROLE, NULL);
    if (PQstatus(conn) != CONNECTION_OK) {
        string err = PQerrorMessage(conn);
        cerr << err << " in " << debug_info << endl;
        throw runtime_error(err + " in " + debug_info);
    }

    string create_sql = "create database " + test_db + "; ";
    auto res = PQexec(conn, create_sql.c_str());
    auto status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
        string err = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(err + " in " + debug_info);
    }
    PQclear(res);

    PQfinish(conn);
    conn = PQsetdbLogin(host_addr.c_str(), to_string(test_port).c_str(), NULL, NULL, test_db.c_str(), YUGABYTE_ROLE, NULL);
    if (PQstatus(conn) != CONNECTION_OK) {
        string err = PQerrorMessage(conn);
        cerr << err << " in " << debug_info << endl;
        throw runtime_error(err + " in " + debug_info);
    }
    cerr << "create successfully" << endl;
    return;
}

yugabyte_connection::~yugabyte_connection()
{
    PQfinish(conn);
}

dut_yugabyte::dut_yugabyte(string db, unsigned int port, string host)
    : yugabyte_connection(db, port, host)
{
    // string set_timeout_cmd = "SET statement_timeout = '" + to_string(YUGABYTE_TIMEOUT_SECOND) + "s';";
    // test(set_timeout_cmd, NULL, NULL);
}

static bool is_expected_error(string error)
{
    if (error.find("violates not-null constraint") != string::npos
        || error.find("duplicate key value violates unique constraint") != string::npos 
        || error.find("encoding conversion from UTF8 to ASCII not supported") != string::npos 
        || error.find("cannot take logarithm of zero") != string::npos 
        || error.find("invalid regular expression: parentheses") != string::npos
        || error.find("invalid normalization form") != string::npos
        || error.find("precision must be between") != string::npos
        || error.find("invalid regular expression: brackets") != string::npos
        || error.find("invalid large-object descriptor") != string::npos
        || error.find("not recognized for type timestamp without time zone") != string::npos
        || error.find("invalid regular expression") != string::npos
        || error.find("input is out of range") != string::npos
        || error.find("field value out of range") != string::npos
        || error.find("date out of range") != string::npos
        || error.find("timestamp out of range") != string::npos
        || error.find("out of range for type") != string::npos
        || error.find("division by zero") != string::npos
        || error.find("window functions are not allowed in WHERE") != string::npos
        || error.find("aggregate functions are not allowed in") != string::npos
        || error.find("a negative number raised to a non-integer power yields a complex result") != string::npos
        || error.find("invalid value for parameter") != string::npos
        || error.find("null character not permitted") != string::npos
        || error.find("invalid escape string") != string::npos
        || error.find("subquery uses ungrouped column") != string::npos
        || error.find("negative substring length not allowed") != string::npos
        || error.find("aggregate function calls cannot be nested") != string::npos
        || error.find("cannot take logarithm of a negative number") != string::npos
        || error.find("zero raised to a negative power is undefined") != string::npos
        || error.find("character number must be positive") != string::npos
        || error.find("requested character not valid for encoding") != string::npos
        || error.find("encoding conversion from") != string::npos
        || error.find("integer out of range") != string::npos
        || error.find("unsupported XML feature") != string::npos
        || error.find("and decimal point together") != string::npos
        || error.find("field position must not be zero") != string::npos
        || error.find("lower bound cannot equal upper bound") != string::npos
        || error.find("cannot take square root of a negative number") != string::npos
        || error.find("count must be greater than zero") != string::npos
        || error.find("is not a valid encoding code") != string::npos
        || error.find("multiple decimal points") != string::npos
        || error.find("is not a number") != string::npos
        || error.find("invalid preceding or following size in window function") != string::npos
        || error.find("FULL JOIN is only supported with merge-joinable or hash-joinable join conditions") != string::npos
        || error.find("invalid name syntax") != string::npos
        || error.find("requested length too large") != string::npos
        || error.find("canceling statement due to statement timeout") != string::npos
        || error.find("cannot use \"") != string::npos
        || error.find("lower and upper bounds must be finite") != string::npos
        || error.find("value out of range") != string::npos
        || error.find("LIKE pattern must not end with escape character") != string::npos
        || error.find("value overflows numeric format") != string::npos
        || error.find("index row size") != string::npos
        || error.find("stack depth limit exceeded") != string::npos
        || error.find("requested character too large for encoding") != string::npos
        || error.find("invalid Unicode escape") != string::npos
        || error.find("does not support") != string::npos
        // || error.find("operator does not exist") != string::npos
        || error.find("value is too big in tsquery") != string::npos
        || (error.find("index row requires") != string::npos && error.find("bytes, maximum size") != string::npos)
        || error.find("invalid memory alloc request size") != string::npos
        || error.find(" must be ahead of ") != string::npos
        || error.find("unterminated format() type specifier") != string::npos
        || error.find("operand, lower bound, and upper bound cannot be NaN") != string::npos
        || error.find("Unicode normalization can only be performed if server encoding is UTF8") != string::npos
        || error.find("Cannot enlarge string buffer containing") != string::npos
        || error.find("nvalid input syntax for type numeric: ") != string::npos
        || error.find("numeric field overflow") != string::npos
        || error.find("could not create unique index") != string::npos
        || error.find("Unicode categorization can only be performed if server encoding is UTF8") != string::npos
        || error.find("invalid type name") != string::npos
        )
        return true;

    return false;
}

void dut_yugabyte::test(const string &stmt, 
                    vector<vector<string>>* output, 
                    int* affected_row_num,
                    vector<string>* env_setting_stmts)
{
    if (env_setting_stmts != NULL) {
        for (auto& set_statement : *env_setting_stmts) {
            auto res = PQexec(conn, set_statement.c_str());
            cerr << "setting: " << set_statement << endl;
            auto status = PQresultStatus(res);
            if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
                string err = PQerrorMessage(conn);
                PQclear(res);
                // clear the current result
                while (res != NULL) {
                    res = PQgetResult(conn);
                    PQclear(res);
                }
                throw runtime_error("[YUGABYTE] setting error [" + err + "]");
            }
        }
    }
    
    auto res = PQexec(conn, stmt.c_str());
    auto status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
        string err = PQerrorMessage(conn);
        PQclear(res);
        
        // clear the current result
        while (res != NULL) {
            res = PQgetResult(conn);
            PQclear(res);
        }

        if (is_expected_error(err))
            throw runtime_error("[YUGABYTE] expected error [" + err + "]");
        else
            throw runtime_error("[YUGABYTE] execution error [" + err + "]");
    }

    if (affected_row_num) {
        auto char_num = PQcmdTuples(res);
        if (char_num != NULL) 
            *affected_row_num = atoi(char_num);
        else
            *affected_row_num = 0;
    }

    if (output) {
        auto field_num = PQnfields(res);
        auto row_num = PQntuples(res);
        for (int i = 0; i < row_num; i++) {
            vector<string> row;
            for (int j = 0; j < field_num; j++) {
                auto tmp = PQgetvalue(res, i, j);
                string str;
                if (tmp == NULL)
                    str = "NULL";
                else {
                    auto res_unit = process_number_string(tmp);
                    str = res_unit;
                }
                row.push_back(str);
            }
            output->push_back(row);
        }
    }
    PQclear(res);

    return;    
}

void dut_yugabyte::reset(void)
{
    if (conn) 
        PQfinish(conn);
    conn = PQsetdbLogin(host_addr.c_str(), to_string(test_port).c_str(), NULL, NULL, "yugabyte", YUGABYTE_ROLE, NULL);
    if (PQstatus(conn) != CONNECTION_OK) {
        string err = PQerrorMessage(conn);
        cerr << err << " in " << debug_info << endl;
        throw runtime_error(err + " in " + debug_info);
    }

    string drop_sql = "drop database if exists " + test_db + " with (force);";
    auto res = PQexec(conn, drop_sql.c_str());
    auto status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
        string err = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(err + " in " + debug_info);
    }
    PQclear(res);

    string create_sql = "create database " + test_db + "; ";
    res = PQexec(conn, create_sql.c_str());
    status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK){
        string err = PQerrorMessage(conn);
        PQclear(res);
        throw runtime_error(err + " in " + debug_info);
    }
    PQclear(res);

    PQfinish(conn);
    conn = PQsetdbLogin(host_addr.c_str(), to_string(test_port).c_str(), NULL, NULL, test_db.c_str(), YUGABYTE_ROLE, NULL);
    if (PQstatus(conn) != CONNECTION_OK) {
        string err = PQerrorMessage(conn);
        cerr << err << " in " << debug_info << endl;
        throw runtime_error(err + " in " + debug_info);
    }
}

void dut_yugabyte::backup(void)
{
    // do nothing as we can use DB_RECORD_FILE
    
    // string pgsql_dump = "/usr/local/pgsql/bin/pg_dump -p " + 
    //                     to_string(test_port) + " " + test_db + " > " + YUGABYTE_BK_FILE(test_db);
    // int ret = system(pgsql_dump.c_str());
    // if (ret != 0) {
    //     std::cerr << "backup fail \nLocation: " + debug_info << endl;
    //     throw std::runtime_error("backup fail \nLocation: " + debug_info); 
    // }
}

void dut_yugabyte::reset_to_backup(void)
{
    reset();
    auto stmt_queue = process_dbrecord_into_sqls(DB_RECORD_FILE);
    for (auto stmt : stmt_queue) {
        test(stmt);
    }
}

int dut_yugabyte::save_backup_file(string db_name, string path)
{
    // string cp_cmd = "cp " + YUGABYTE_BK_FILE(db_name) + " " + path;
    // return system(cp_cmd.c_str());
    return 0;
}

void dut_yugabyte::get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content)
{
    for (auto& table:tables_name) {
        vector<vector<string>> table_content;
        auto query = "SELECT * FROM " + table + " ORDER BY 1;";

        auto res = PQexec(conn, query.c_str());
        auto status = PQresultStatus(res);
        if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
            string err = PQerrorMessage(conn);
            PQclear(res);
            cerr << "Cannot get content of " + table + "\nLocation: " + debug_info << endl;
            cerr << "Error: " + err + "\nLocation: " + debug_info << endl;
            continue;
        }

        auto field_num = PQnfields(res);
        auto row_num = PQntuples(res);
        for (int i = 0; i < row_num; i++) {
            vector<string> row_output;
            for (int j = 0; j < field_num; j++) {
                auto tmp = PQgetvalue(res, i, j);
                string str;
                if (tmp == NULL)
                    str = "NULL";
                else {
                    auto res_unit = process_number_string(tmp);
                    str = res_unit;
                }
                row_output.push_back(str);
            }
            table_content.push_back(row_output);
        }
        PQclear(res);
        content[table] = table_content;
    }
    return;
}
