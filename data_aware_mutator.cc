#include "data_aware_mutator.hh"

void data_aware_mutator::mutate(shared_ptr<schema>& db_schema, 
                        map<string, vector<vector<string>>>& db_content,
                        vector<shared_ptr<prod>>& stmt_queue) 
{
    for (auto& stmt : stmt_queue) {
        inclusive_mutate(db_schema, db_content, stmt);
        exclusive_mutate(db_schema, db_content, stmt);
    }
}

void data_aware_mutator::inclusive_mutate(shared_ptr<schema>& db_schema, 
                            map<string, vector<vector<string>>>& db_content,
                            shared_ptr<prod>& stmt)
{
    shared_ptr<bool_expr>* predicate;
    vector<named_relation *>* ref_scope;
    auto has_predicate = get_predicate_scope(stmt, predicate, ref_scope);
    if (has_predicate == false)
        return;
    
    string pkey_value;
    named_relation* chosen_table;
    if (pick_pkey(stmt, db_content, *ref_scope, pkey_value, chosen_table) == false)
        return;

    auto  pkey_value_expr = make_shared<const_expr>(stmt.get(), pkey_value);

    op *equal_op = NULL;
    for (auto& op : db_schema->operators) {
        if (op.name == "=") {
            equal_op = &op;
            break;
        }
    }
    if (equal_op == NULL) 
        throw runtime_error("intrument insert statement: cannot find = operator");
    
    auto columns = chosen_table->columns();
    auto pkey_column = make_shared<column_reference>(stmt.get(), columns[PKEY_INDEX].type, columns[PKEY_INDEX].name, chosen_table->name);
    auto new_constraints = make_shared<comparison_op>(stmt.get(), equal_op, pkey_column, pkey_value_expr);
    *predicate = make_shared<bool_term>(stmt.get(), true, *predicate, new_constraints); // add a or operation after the original predicate
}

void data_aware_mutator::exclusive_mutate(shared_ptr<schema>& db_schema, 
                            map<string, vector<vector<string>>>& db_content,
                            shared_ptr<prod>& stmt)
{
    shared_ptr<bool_expr>* predicate;
    vector<named_relation *>* ref_scope;
    auto has_predicate = get_predicate_scope(stmt, predicate, ref_scope);
    if (has_predicate == false)
        return;
    
    op *not_equal_op = NULL;
    for (auto& op : db_schema->operators) {
        if (op.name == "<>") {
            not_equal_op = &op;
            break;
        }
    }
    if (not_equal_op == NULL) 
        throw runtime_error("intrument insert statement: cannot find <> operator");
    
    auto exclusive_num = d6();
    for (int i = 0; i < exclusive_num; i++) {
        string pkey_value;
        named_relation* chosen_table;
        if (pick_pkey(stmt, db_content, *ref_scope, pkey_value, chosen_table) == false)
            return;

        auto  pkey_value_expr = make_shared<const_expr>(stmt.get(), pkey_value);
        auto columns = chosen_table->columns();
        auto pkey_column = make_shared<column_reference>(stmt.get(), columns[PKEY_INDEX].type, columns[PKEY_INDEX].name, chosen_table->name);
        auto new_constraints = make_shared<comparison_op>(stmt.get(), not_equal_op, pkey_column, pkey_value_expr);
        *predicate = make_shared<bool_term>(stmt.get(), false, *predicate, new_constraints); // add an and operation after the original predicate
    }
}

// true: has predicate, store the predicate in ret_predicate, and store the reference scope in ret_refscope
// false: no predicate
bool data_aware_mutator::get_predicate_scope(shared_ptr<prod>& stmt, 
                            shared_ptr<bool_expr>* &ret_predicate,
                            vector<named_relation *>* &ret_refscope)
{
    if (auto update_statement = dynamic_pointer_cast<update_stmt>(stmt)) {
        ret_predicate = &update_statement->search;
        ret_refscope = &update_statement->scope->refs;
        return true;
    }
    else if (auto delete_statement = dynamic_pointer_cast<delete_stmt>(stmt)) {
        ret_predicate = &delete_statement->search;
        ret_refscope = &delete_statement->scope->refs;
        return true;
    }
    else if (auto select_statement = dynamic_pointer_cast<query_spec>(stmt)) {
        ret_predicate = &select_statement->search;
        ret_refscope = &select_statement->scope->refs;
        return true;
    }
    else if (auto cte_statement = dynamic_pointer_cast<common_table_expression>(stmt)) {
        ret_predicate = &cte_statement->query->search;
        ret_refscope = &cte_statement->query->scope->refs;
        return true;
    }
    else
        return false;
}

bool data_aware_mutator::pick_pkey(shared_ptr<prod>& stmt, 
                                    map<string, vector<vector<string>>>& db_content, 
                                    vector<named_relation *>& ref_scope,
                                    string& ret_pkey_value,
                                    named_relation* &ret_chosen_table)
{
    named_relation *chosen_table;
    vector<vector<string>> chosen_content;
    vector<named_relation *> not_empty_ref;
    for (auto& table: ref_scope) {
        if (db_content[table->name].empty() == false)
            not_empty_ref.push_back(table);
    }
    if (not_empty_ref.empty())
        return false;

    while (1) {
        chosen_table = random_pick<>(not_empty_ref);
        chosen_content = db_content[chosen_table->name];
        if (chosen_content.empty() == false)
            break;
    }
    auto chosen_row_idx = dx(chosen_content.size()) - 1;
    auto original_row_idx = chosen_row_idx;
    auto chosen_row = chosen_content[chosen_row_idx];
    while (1) {
        auto pkey_value = chosen_row[PKEY_INDEX];
        if (specified_rows.count(stmt) == 0 ||
                specified_rows[stmt].count(pkey_value) == 0) 
            break;
        chosen_row_idx = (chosen_row_idx + 1) % chosen_content.size();
        chosen_row = chosen_content[chosen_row_idx];
        if (chosen_row_idx == original_row_idx) // all row has been picked, just choose the original one
            break;
    }
    auto pkey_value = chosen_row[PKEY_INDEX];

    if (specified_rows.count(stmt) > 0)
        specified_rows[stmt].insert(pkey_value);
    else {
        set<string> new_set;
        new_set.insert(pkey_value);
        specified_rows[stmt] = new_set;
    }

    ret_pkey_value = pkey_value;
    ret_chosen_table = chosen_table;

    return true;
}