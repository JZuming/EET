#include <typeinfo>
#include "config.h"
#include "schema.hh"
#include "relmodel.hh"
#include <pqxx/pqxx>
#include <set>
#include "gitrev.h"

using namespace std;
using namespace pqxx;

string schema::target_dbms = "";

void schema::generate_indexes() {
    // enable aggregate function
    for(auto &r: aggregates) {
        assert(r.restype);
        aggregates_returning_type.insert(pair<sqltype*, routine*>(r.restype, &r));
    }

    // enable routine function
    for(auto &r: routines) {
        assert(r.restype);
        routines_returning_type.insert(pair<sqltype*, routine*>(r.restype, &r));
        if(!r.argtypes.size())
            parameterless_routines_returning_type.insert(pair<sqltype*, routine*>(r.restype, &r));
    }

    // enable "atomic_subselect" use specific tables
    for (auto &t: tables) {
        set<sqltype *> type_set_in_table;
        for (auto &c: t.columns()) { // filter repeated column types
            assert(c.type);
            type_set_in_table.insert(c.type);
        }

        for (auto uniq_type : type_set_in_table) {
            tables_with_columns_of_type.insert(pair<sqltype*, table*>(uniq_type, &t));
        }
    }

    // enable operator
    for (auto &o: operators) {
        operators_returning_type.insert(pair<sqltype*, op*>(o.result, &o));
    }

    // enable window function
    for(auto &r: windows) {
        assert(r.restype);
        windows_returning_type.insert(pair<sqltype*, routine*>(r.restype, &r));
    }

    for (auto &t: tables) {
        if (t.is_base_table)
            base_tables.push_back(&t);
    }

    assert(booltype);
    assert(inttype);
    assert(texttype);
    assert(realtype);
    assert(datetype);
    assert(internaltype);
    assert(arraytype);
}
