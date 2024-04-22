#ifndef DATA_AWARE_MUTATOR_HH
#define DATA_AWARE_MUTATOR_HH

#include "schema.hh"
#include "prod.hh"
#include "expr.hh"
#include "grammar.hh"
#include <set>

using namespace std;

struct data_aware_mutator {
    void mutate(shared_ptr<schema>& db_schema, 
                        map<string, vector<vector<string>>>& db_content,
                        vector<shared_ptr<prod>>& stmt_queue);
    void inclusive_mutate(shared_ptr<schema>& db_schema, 
                            map<string, vector<vector<string>>>& db_content,
                            shared_ptr<prod>& stmt);
    void exclusive_mutate(shared_ptr<schema>& db_schema, 
                            map<string, vector<vector<string>>>& db_content,
                            shared_ptr<prod>& stmt);

    bool get_predicate_scope(shared_ptr<prod>& stmt, 
                            shared_ptr<bool_expr>* &ret_predicate,
                            vector<named_relation *>* &ret_refscope);
    
    bool pick_pkey(shared_ptr<prod>& stmt, 
                                    map<string, vector<vector<string>>>& db_content, 
                                    vector<named_relation *>& ref_scope,
                                    string& ret_pkey_value,
                                    named_relation* &ret_chosen_table);

    map<shared_ptr<prod>, set<string>> specified_rows;
};

#endif