#include "postgres.hh"
#include "config.h"
#include <iostream>
#include <fstream>
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

#define POSTGRES_TIMEOUT_SECOND 6

#define POSTGRES_BK_FILE(DB_NAME) ("/tmp/pg_" + DB_NAME + "_bk.sql")

static regex e_timeout("ERROR:  canceling statement due to statement timeout(\n|.)*");
static regex e_syntax("ERROR:  syntax error at or near(\n|.)*");

#define debug_info (string(__func__) + "(" + string(__FILE__) + ":" + to_string(__LINE__) + ")")

static bool has_types = false;
static vector<pg_type *> static_type_vec;

static bool has_operators = false;
static vector<op> static_op_vec;

static bool has_routines = false;
static vector<routine> static_routine_vec;

static bool has_routine_para = false;
static map<string, vector<pg_type *>> static_routine_para_map;

static bool has_aggregates = false;
static vector<routine> static_aggregate_vec;

static bool has_aggregate_para = false;
static map<string, vector<pg_type *>> static_aggregate_para_map;

static vector<string> pgerrmsg;

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

bool pg_type::consistent(sqltype *rvalue)
{
    pg_type *t = dynamic_cast<pg_type*>(rvalue);
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

bool schema_pqxx::is_consistent_with_basic_type(sqltype *rvalue)
{
    if (booltype->consistent(rvalue) ||
        inttype->consistent(rvalue) ||
        realtype->consistent(rvalue) ||
        texttype->consistent(rvalue) ||
        datetype->consistent(rvalue))
        return true;

    return false;
}

schema_pqxx::schema_pqxx(string db, unsigned int port, string path, bool no_catalog)
    : pgsql_connection(db, port, path)
{
    ifstream pgerr("pgsqlerr.txt");
    if (pgerr.is_open()) {
        std::string line;
        while (pgerr >> line)
            pgerrmsg.push_back(line);
    } else {
        std::cerr << "Unable to open file for reading." << std::endl;
    }

    // c.set_variable("application_name", "'" PACKAGE "::schema'");

    // pqxx::work w(c);
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

            auto t = new pg_type(name, oid, typdelim[0], typrelid, typelem, typarray, typtype[0]);
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

    // Planner Method Configuration
    supported_setting["enable_async_append"] = vector<string>({"on", "off"});
    supported_setting["enable_bitmapscan"] = vector<string>({"on", "off"});
    supported_setting["enable_gathermerge"] = vector<string>({"on", "off"});
    supported_setting["enable_hashagg"] = vector<string>({"on", "off"});
    supported_setting["enable_hashjoin"] = vector<string>({"on", "off"});
    supported_setting["enable_incremental_sort"] = vector<string>({"on", "off"});
    supported_setting["enable_indexscan"] = vector<string>({"on", "off"});
    supported_setting["enable_indexonlyscan"] = vector<string>({"on", "off"});
    supported_setting["enable_material"] = vector<string>({"on", "off"});
    supported_setting["enable_memoize"] = vector<string>({"on", "off"});
    supported_setting["enable_mergejoin"] = vector<string>({"on", "off"});
    supported_setting["enable_nestloop"] = vector<string>({"on", "off"});
    supported_setting["enable_parallel_append"] = vector<string>({"on", "off"});
    supported_setting["enable_parallel_hash"] = vector<string>({"on", "off"});
    supported_setting["enable_partition_pruning"] = vector<string>({"on", "off"});
    supported_setting["enable_partitionwise_join"] = vector<string>({"on", "off"});
    supported_setting["enable_partitionwise_aggregate"] = vector<string>({"on", "off"});
    supported_setting["enable_seqscan"] = vector<string>({"on", "off"});
    supported_setting["enable_sort"] = vector<string>({"on", "off"});
    supported_setting["enable_tidscan"] = vector<string>({"on", "off"});

    // Planner Cost Constants
    // supported_setting["seq_page_cost"] = vector<string>({"0", "1", "2", "4", "8", "16", "32", "64", "128", "256", "512", "1024", "2048", "4096", "8192", "2147483647"});
    // supported_setting["random_page_cost "] = vector<string>({"0", "1", "2", "4", "8", "16", "32", "64", "128", "256", "512", "1024", "2048", "4096", "8192", "2147483647"});
    // supported_setting["cpu_tuple_cost"] = vector<string>({"0", "0.001", "0.01", "0.1", "1", "10", "100", "1000", "10000", "100000", "1000000"});
    // supported_setting["cpu_index_tuple_cost"] = vector<string>({"0", "0.0005", "0.005", "0.05", "0.5", "5", "50", "500", "5000", "50000", "500000"});
    // supported_setting["cpu_operator_cost"] = vector<string>({"0", "0.00025", "0.0025", "0.025", "0.25", "2.5", "25", "250", "2500", "25000", "250000"});
    // supported_setting["parallel_setup_cost"] = vector<string>({"0", "1", "2", "4", "8", "16", "32", "64", "128", "256", "512", "1024", "2048", "4096", "8192", "2147483647"});
    // supported_setting["parallel_tuple_cost"] = vector<string>({"0", "0.01", "0.1", "1", "10", "100", "1000", "10000", "100000", "1000000", "10000000"});
    // supported_setting["min_parallel_table_scan_size"] = vector<string>({"0", "1", "10", "100", "1000", "10000", "100000", "1000000", "10000000", "715827882"});
    // supported_setting["min_parallel_index_scan_size"] = vector<string>({"0", "1", "10", "100", "1000", "10000", "100000", "1000000", "10000000", "715827882"});
    // supported_setting["effective_cache_size"] = vector<string>({"1", "10", "100", "1000", "10000", "100000", "1000000", "10000000", "100000000", "1000000000", "2147483647"});
    // supported_setting["jit_above_cost"] = vector<string>({"-1", "0", "1", "10", "100", "1000", "10000", "100000", "1000000", "10000000", "100000000", "1000000000"});
    // supported_setting["jit_inline_above_cost"] = vector<string>({"-1", "0", "1", "10", "100", "1000", "10000", "100000", "1000000", "10000000", "100000000", "1000000000"});
    // supported_setting["jit_optimize_above_cost"] = vector<string>({"-1", "0", "1", "10", "100", "1000", "10000", "100000", "1000000", "10000000", "100000000", "1000000000"});

    // Genetic Query Optimizer
    supported_setting["geqo"] = vector<string>({"on", "off"});
    // supported_setting["geqo_threshold"] = vector<string>({"2", "4", "8", "16", "32", "64", "128", "256", "512", "1024", "2048", "4096", "8192", "2147483647"});
    // supported_setting["geqo_effort"] = vector<string>({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"});
    // supported_setting["geqo_pool_size"] = vector<string>({"0", "1", "2", "4", "8", "16", "32", "64", "128", "256", "512", "1024", "2048", "4096", "8192", "2147483647"});
    // supported_setting["geqo_generations"] = vector<string>({"0", "1", "2", "4", "8", "16", "32", "64", "128"});
    // supported_setting["geqo_selection_bias"] = vector<string>({"1.50", "1.60", "1.70", "1.80", "1.90", "2.00"});
    // supported_setting["geqo_seed"] = vector<string>({"0.0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9", "1.0"});

    // Other Planner Options
    // supported_setting["default_statistics_target"] = vector<string>({"1", "2", "4", "8", "16", "32", "64", "128", "256", "512", "1024", "2048", "4096", "8192", "10000"});
    supported_setting["constraint_exclusion"] = vector<string>({"on", "off", "partition"});
    // supported_setting["cursor_tuple_fraction"] = vector<string>({"0.0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9", "1.0"});
    // supported_setting["from_collapse_limit"] = vector<string>({"1", "2", "4", "8", "16", "32", "64", "128", "256", "512", "1024", "2048", "4096", "8192", "2147483647"});
    supported_setting["jit"] = vector<string>({"on", "off"});
    // supported_setting["join_collapse_limit"] = vector<string>({"1", "2", "4", "8", "16", "32", "64", "128", "256", "512", "1024", "2048", "4096", "8192", "2147483647"});
    supported_setting["plan_cache_mode"] = vector<string>({"auto", "force_custom_plan", "force_generic_plan"});
    // supported_setting["recursive_worktable_factor"] = vector<string>({"0.001", "0.01", "0.1", "1", "10", "100", "1000", "10000", "100000", "1000000"});

    target_dbms = "postgres";

    // cerr << "Loading tables...";
    string load_table_sql = "select table_name, "
                                "table_schema, "
                                "is_insertable_into, "
                                "table_type "
                            "from information_schema.tables;";
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
        string q("select attname, atttypid "
                "from pg_attribute join pg_class c on( c.oid = attrelid ) "
                    "join pg_namespace n on n.oid = relnamespace "
                "where not attisdropped "
                    "and attname not in "
                    "('xmin', 'xmax', 'ctid', 'cmin', 'cmax', 'tableoid', 'oid') ");
        q += " and relname = '" + t->name + "'";
        q += " and nspname = '" + t->schema + "';";

        res = pqexec_handle_error(conn, q);
        row_num = PQntuples(res);
        for (int i = 0; i < row_num; i++) {
            string column_name(PQgetvalue(res, i, 0));
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
    // for (auto& proc:static_routine_vec) {
    //     register_routine(proc);
    // }

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
            vector <pg_type *> para_vec;
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
            vector<pg_type *> para_vec;
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

schema_pqxx::~schema_pqxx()
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

extern "C" {
    void dut_libpq_notice_rx(void *arg, const PGresult *res);
}

void dut_libpq_notice_rx(void *arg, const PGresult *res)
{
    (void) arg;
    (void) res;
}

pgsql_connection::pgsql_connection(string db, unsigned int port) {
    test_db = db;
    test_port = port;
}

pgsql_connection::pgsql_connection(string db, unsigned int port, string path) : pgsql_connection(db, port)
{
    test_db = db;
    test_port = port;
    inst_path = path;

    conn = PQsetdbLogin("localhost", to_string(port).c_str(), NULL, NULL, db.c_str(), NULL, NULL);
    if (PQstatus(conn) == CONNECTION_OK)
        return; // succeed

    string err = PQerrorMessage(conn);
    if (err.find("does not exist") == string::npos) {
        cerr << "[CONNECTION FAIL]  " << err << " in " << debug_info << endl;
        throw runtime_error("[CONNECTION FAIL] " + err + " in " + debug_info);
    }

    cerr << "try to create database testdb" << endl;
    conn = PQsetdbLogin("localhost", to_string(test_port).c_str(), NULL, NULL, "postgres", NULL, NULL);
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
    conn = PQsetdbLogin("localhost", to_string(test_port).c_str(), NULL, NULL, test_db.c_str(), NULL, NULL);
    if (PQstatus(conn) != CONNECTION_OK) {
        string err = PQerrorMessage(conn);
        cerr << err << " in " << debug_info << endl;
        throw runtime_error(err + " in " + debug_info);
    }
    cerr << "create successfully" << endl;
    return;
}

pgsql_connection::~pgsql_connection()
{
    PQfinish(conn);
}

dut_libpq::dut_libpq(string db, unsigned int port, string path)
    : pgsql_connection(db, port, path)
{
    string set_timeout_cmd = "SET statement_timeout = '" + to_string(POSTGRES_TIMEOUT_SECOND) + "s';";
    test(set_timeout_cmd, NULL, NULL);
}

static bool is_expected_error(string error)
{
    for (const auto& err : pgerrmsg)
        if (error.find(err))
            return true;

    return false;
}

void dut_libpq::test(const string &stmt,
                    vector<vector<string>>* output,
                    int* affected_row_num,
                    vector<string>* env_setting_stmts)
{
    if (env_setting_stmts != NULL) {
        for (auto& set_statement : *env_setting_stmts) {
            auto res = PQexec(conn, set_statement.c_str());
            // cerr << "setting: " << set_statement << endl;
            auto status = PQresultStatus(res);
            if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
                string err = PQerrorMessage(conn);
                PQclear(res);
                // clear the current result
                while (res != NULL) {
                    res = PQgetResult(conn);
                    PQclear(res);
                }
                throw runtime_error("[POSTGRES] setting error [" + err + "]");
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
            throw runtime_error("[POSTGRES] expected error [" + err + "]");
        else
            throw runtime_error("[POSTGRES] execution error [" + err + "]");
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

void dut_libpq::reset(void)
{
    if (conn)
        PQfinish(conn);
    conn = PQsetdbLogin("localhost", to_string(test_port).c_str(), NULL, NULL, "postgres", NULL, NULL);
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
    conn = PQsetdbLogin("localhost", to_string(test_port).c_str(), NULL, NULL, test_db.c_str(), NULL, NULL);
    if (PQstatus(conn) != CONNECTION_OK) {
        string err = PQerrorMessage(conn);
        cerr << err << " in " << debug_info << endl;
        throw runtime_error(err + " in " + debug_info);
    }
}

void dut_libpq::backup(void)
{
     string pgsql_dump = inst_path + "/bin/pg_dump -p " +
                        to_string(test_port) + " " + test_db + " > " + POSTGRES_BK_FILE(test_db);

    int ret = system(pgsql_dump.c_str());
    if (ret != 0) {
        std::cerr << "backup fail \nLocation: " + debug_info << endl;
        throw std::runtime_error("backup fail \nLocation: " + debug_info);
    }
}

void dut_libpq::reset_to_backup(void)
{
    reset();
    string bk_file = POSTGRES_BK_FILE(test_db);
    if (access(bk_file.c_str(), F_OK ) == -1)
        return;

    PQfinish(conn);

    string pgsql_source = inst_path + "/bin/psql -p "
                        + to_string(test_port) + " " + test_db + " < "
                        + POSTGRES_BK_FILE(test_db) + " 1> /dev/null";
    if (system(pgsql_source.c_str()) == -1)
        throw std::runtime_error(string("system() error, return -1") + "\nLocation: " + debug_info);

    conn = PQsetdbLogin("localhost", to_string(test_port).c_str(), NULL, NULL, test_db.c_str(), NULL, NULL);
    if (PQstatus(conn) != CONNECTION_OK) {
        string err = PQerrorMessage(conn);
        throw runtime_error("[CONNECTION FAIL] " + err + " in " + debug_info);
    }
}

int dut_libpq::save_backup_file(string db_name, string path)
{
    string cp_cmd = "cp " + POSTGRES_BK_FILE(db_name) + " " + path;
    return system(cp_cmd.c_str());
}

void dut_libpq::get_content(vector<string>& tables_name, map<string, vector<vector<string>>>& content)
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
