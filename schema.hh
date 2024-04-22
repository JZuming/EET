/// @file
/// @brief Base class providing schema information to grammar

#ifndef SCHEMA_HH
#define SCHEMA_HH

#include <string>
#include <iostream>
#include <pqxx/pqxx>
#include <numeric>
#include <memory>

#include "relmodel.hh"
#include "random.hh"

#define BINOP(n, a, b, r) do {\
    op o(#n, a, b, r); \
    register_operator(o); \
} while(0)

#define FUNC(n, r) do {							\
    routine proc("", "", r, #n);				\
    register_routine(proc);						\
} while(0)

#define FUNC1(n, r, a) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    register_routine(proc);						\
} while(0)

#define FUNC2(n, r, a, b) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    proc.argtypes.push_back(b);				\
    register_routine(proc);						\
} while(0)

#define FUNC3(n, r, a, b, c) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    proc.argtypes.push_back(b);				\
    proc.argtypes.push_back(c);				\
    register_routine(proc);						\
} while(0)

#define FUNC4(n, r, a, b, c, d) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    proc.argtypes.push_back(b);				\
    proc.argtypes.push_back(c);				\
    proc.argtypes.push_back(d);				\
    register_routine(proc);						\
} while(0)

#define FUNC5(n, r, a, b, c, d, e) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    proc.argtypes.push_back(b);				\
    proc.argtypes.push_back(c);				\
    proc.argtypes.push_back(d);				\
    proc.argtypes.push_back(e);				\
    register_routine(proc);						\
} while(0)

#define AGG1(n, r, a) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    register_aggregate(proc);						\
} while(0)

#define AGG2(n, r, a, b) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    proc.argtypes.push_back(b);				\
    register_aggregate(proc);						\
} while(0)

#define AGG3(n, r, a, b, c) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    proc.argtypes.push_back(b);				\
    proc.argtypes.push_back(c);				\
    register_aggregate(proc);						\
} while(0)

#define AGG(n, r) do {						\
    routine proc("", "", r, #n);				\
    register_aggregate(proc);						\
} while(0)

#define WIN(n, r) do {						\
    routine proc("", "", r, #n);				\
    register_windows(proc);						\
} while(0)

#define WIN1(n, r, a) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    register_windows(proc);						\
} while(0)

#define WIN2(n, r, a, b) do {						\
    routine proc("", "", r, #n);				\
    proc.argtypes.push_back(a);				\
    proc.argtypes.push_back(b);				\
    register_windows(proc);						\
} while(0)

struct schema {
    sqltype *booltype = NULL;
    sqltype *inttype = NULL;
    sqltype *realtype = NULL;
    sqltype *texttype = NULL;
    sqltype *internaltype = NULL;
    sqltype *arraytype = NULL;
    sqltype *datetype = NULL;

    std::vector<sqltype *> types;
  
    std::vector<table> tables;
    std::vector<string> indexes;
    std::vector<op> operators;
    std::vector<routine> routines;
    std::vector<routine> aggregates;
    std::vector<routine> windows;

    typedef std::tuple<sqltype *,sqltype *,sqltype *> typekey;
    std::multimap<typekey, op> index;
    typedef std::multimap<typekey, op>::iterator op_iterator;

    std::multimap<sqltype*, routine*> routines_returning_type;
    std::multimap<sqltype*, routine*> aggregates_returning_type;
    std::multimap<sqltype*, routine*> windows_returning_type;
    std::multimap<sqltype*, routine*> parameterless_routines_returning_type;
    std::multimap<sqltype*, table*> tables_with_columns_of_type;
    std::multimap<sqltype*, op*> operators_returning_type;
    std::multimap<sqltype*, sqltype*> concrete_type;
    std::vector<table*> base_tables;

    string version;
    int version_num; // comparable version number

    const char *true_literal = "true";
    const char *false_literal = "false";
    const char *null_literal = "null";

    vector<string> available_collation;
    bool enable_partial_index = false; // can or cannot use where in indexes
    vector<string> available_table_options;
    bool enable_analyze_stmt = false; //  can or cannot use analyze statement
    vector<string> compound_operators;
    vector<string> available_index_type;
    vector<string> available_index_keytype;
    static string target_dbms;
    vector<string> supported_join_op;
    vector<string> supported_table_engine;
    map<string, vector<string>> supported_setting;
  
    virtual std::string quote_name(const std::string &id) = 0;
  
    void summary() {
        std::cout << "Found " << tables.size() <<
            " user table(s) in information schema." << std::endl;
    }

    void fill_scope(struct scope &s) {
        for (auto &t : tables)
            s.tables.push_back(&t);
        for (auto i : indexes)
            s.indexes.push_back(i);
        s.schema = this;
    }

    virtual void register_operator(op& o) {
        operators.push_back(o);
        typekey t(o.left, o.right, o.result);
        index.insert(std::pair<typekey, op>(t, o));
    }

    virtual void register_routine(routine& r) {
        routines.push_back(r);
    }

    virtual void register_aggregate(routine& r) {
        aggregates.push_back(r);
    }

    virtual void register_windows(routine& r) {
        windows.push_back(r);
    }

    virtual op_iterator find_operator(sqltype *left, sqltype *right, sqltype *res) {
        typekey t(left, right, res);
        auto cons = index.equal_range(t);
        if (cons.first == cons.second)
            return index.end();
        else
            return random_pick<>(cons.first, cons.second);
    }

    schema() { }
    // virtual void update_schema() = 0; // only update dynamic information, e.g. table, columns, index
    void generate_indexes();
};

#endif

