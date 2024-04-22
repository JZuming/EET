#include "in_query.hh"
#include "grammar.hh"

extern int use_group;    // 0->no group, 1->use group, 2->to_be_define
extern int in_in_clause; // 0-> not in "in" clause, 1-> in "in" clause (cannot use limit)

in_query::in_query(prod *p) : bool_expr(p), myscope(scope)
{
    myscope.tables = scope->tables;
    scope = &myscope;

    lhs = value_expr::factory(this);

    auto tmp_in_state = in_in_clause;
    in_in_clause = 1;
    auto tmp_use_group = use_group;
    use_group = 0;

    // clickhouse does not support correlated subqueries.
    // it use seperated my scope, do not need to restore the refs
    if (schema::target_dbms == "clickhouse")
        scope->refs.clear(); // dont use the ref of parent select

    vector<sqltype *> pointed_type;
    pointed_type.push_back(lhs->type);
    if (d6() < 4)
        in_subquery = make_shared<unioned_query>(this, scope, false, &pointed_type);
    else
        in_subquery = make_shared<query_spec>(this, scope, false, &pointed_type);

    use_group = tmp_use_group;
    in_in_clause = tmp_in_state;
}

in_query::in_query(prod *p, shared_ptr<value_expr> expr, shared_ptr<prod> subquery)
    : bool_expr(p), myscope(scope)
{
    myscope.tables = scope->tables;
    scope = &myscope;

    lhs = expr;

    in_subquery = subquery;
}

void in_query::out(ostream &out)
{
    OUTPUT_EQ_BOOL_EXPR(out);

    if (is_transformed && schema::target_dbms != "clickhouse")
        out << *eq_expr;
    else
        out << "(" << *lhs << ") in (" << *in_subquery << ")";
}

void in_query::accept(prod_visitor *v)
{
    v->visit(this);
    lhs->accept(v);
    in_subquery->accept(v);
}

void in_query::equivalent_transform()
{
    bool_expr::equivalent_transform();
    lhs->equivalent_transform();

    auto tmp1 = dynamic_pointer_cast<query_spec>(in_subquery);
    if (tmp1)
    {
        tmp1->search->equivalent_transform();
        // clickhouse does not support subqueries in select list
        if (schema::target_dbms != "clickhouse") {
            for (auto expr : tmp1->select_list->value_exprs)
                expr->equivalent_transform();
        }
    }
    auto tmp2 = dynamic_pointer_cast<unioned_query>(in_subquery);
    if (tmp2)
    {
        tmp2->lhs->search->equivalent_transform();
        tmp2->rhs->search->equivalent_transform();
        // clickhouse does not support subqueries in select list
        if (schema::target_dbms != "clickhouse") {
            for (auto expr : tmp2->lhs->select_list->value_exprs)
                expr->equivalent_transform();
            for (auto expr : tmp2->rhs->select_list->value_exprs)
                expr->equivalent_transform();
        }
    }

    if (schema::target_dbms == "clickhouse")
        return;

    // transform in query to exists predicate
    auto old_in_query = make_shared<in_query>(this, this->lhs, this->in_subquery);
    auto is_not_null = make_shared<null_predicate>(this, old_in_query, false); // in_query is not null
    auto null_value = make_shared<const_bool>(this, -1);

    op *eq = NULL;
    for (auto &op : scope->schema->operators)
    {
        if (op.name == "=")
            eq = &op;
    }
    assert(eq != NULL);

    if (tmp1)
    {
        auto selected = tmp1->select_list->value_exprs.back();
        auto check_equal = make_shared<comparison_op>(this, eq, lhs, selected); // selected = lhs
        auto new_predicate = make_shared<bool_term>(this, false, check_equal, tmp1->search); // (selected = lhs) and old_predicate
        auto new_query = make_shared<query_spec>(*tmp1);
        
        new_query->search = new_predicate;
        auto exists_expr = make_shared<exists_predicate>(this, new_query);

        eq_expr = make_shared<case_expr>(this, is_not_null, exists_expr, null_value);
    }
    else if (tmp2)
    {
        auto lhs_selected = tmp2->lhs->select_list->value_exprs.back();
        auto lhs_bin_operation = make_shared<comparison_op>(this, eq, lhs, lhs_selected);
        auto lhs_new_predicate = make_shared<bool_term>(this, false, lhs_bin_operation, tmp2->lhs->search);
        auto new_query_1 = make_shared<query_spec>(*tmp2->lhs);
        new_query_1->search = lhs_new_predicate;

        auto rhs_selected = tmp2->rhs->select_list->value_exprs.back();
        auto rhs_bin_operation = make_shared<comparison_op>(this, eq, lhs, rhs_selected);
        auto rhs_new_predicate = make_shared<bool_term>(this, false, rhs_bin_operation, tmp2->rhs->search);
        auto new_query_2 = make_shared<query_spec>(*tmp2->rhs);
        new_query_2->search = rhs_new_predicate;

        auto union_query = make_shared<unioned_query>(this, this->scope, new_query_1, new_query_2, tmp2->type);

        union_query->equivalent_transform();
        auto exists_expr = make_shared<exists_predicate>(this, union_query);
        eq_expr = make_shared<case_expr>(this, is_not_null, exists_expr, null_value);
    }
}

void in_query::back_transform()
{
    auto tmp1 = dynamic_pointer_cast<query_spec>(in_subquery);
    if (tmp1)
    {
        // clickhouse does not support subqueries in select list
        if (schema::target_dbms != "clickhouse") {
            for (auto expr : tmp1->select_list->value_exprs)
                expr->back_transform();
        }
        tmp1->search->back_transform();
    }
    auto tmp2 = dynamic_pointer_cast<unioned_query>(in_subquery);
    if (tmp2)
    {
        if (schema::target_dbms != "clickhouse") {
            auto union_query = dynamic_pointer_cast<unioned_query>(
                dynamic_pointer_cast<exists_predicate>(eq_expr->true_expr)->subquery);
            union_query->back_transform();
        }

        // clickhouse does not support subqueries in select list
        if (schema::target_dbms != "clickhouse") {
            for (auto expr : tmp2->lhs->select_list->value_exprs)
                expr->back_transform();
            for (auto expr : tmp2->rhs->select_list->value_exprs)
                expr->back_transform();
        }
        tmp2->lhs->search->back_transform();
        tmp2->rhs->search->back_transform();
    }

    lhs->back_transform();
    bool_expr::back_transform();
}

void in_query::set_component_id(int &id)
{
    bool_expr::set_component_id(id);
    lhs->set_component_id(id);

    auto tmp1 = dynamic_pointer_cast<query_spec>(in_subquery);
    if (tmp1)
    {
        tmp1->search->set_component_id(id);
    }
    auto tmp2 = dynamic_pointer_cast<unioned_query>(in_subquery);
    if (tmp2)
    {
        tmp2->lhs->search->set_component_id(id);
        tmp2->rhs->search->set_component_id(id);
    }
}

bool in_query::get_component_from_id(int id, shared_ptr<value_expr> &component)
{
    auto tmp1 = dynamic_pointer_cast<query_spec>(in_subquery);
    if (tmp1)
    {
        GET_COMPONENT_FROM_ID_CHILD(id, component, tmp1->search);
    }
    auto tmp2 = dynamic_pointer_cast<unioned_query>(in_subquery);
    if (tmp2)
    {
        GET_COMPONENT_FROM_ID_CHILD(id, component, tmp2->lhs->search);
        GET_COMPONENT_FROM_ID_CHILD(id, component, tmp2->rhs->search);
    }

    GET_COMPONENT_FROM_ID_CHILD(id, component, lhs);
    return bool_expr::get_component_from_id(id, component);
}

bool in_query::set_component_from_id(int id, shared_ptr<value_expr> component)
{
    auto bool_value = dynamic_pointer_cast<bool_expr>(component);
    auto tmp1 = dynamic_pointer_cast<query_spec>(in_subquery);
    auto tmp2 = dynamic_pointer_cast<unioned_query>(in_subquery);
    if (bool_value)
    {
        if (tmp1)
        {
            SET_COMPONENT_FROM_ID_CHILD(id, bool_value, tmp1->search);
        }
        if (tmp2)
        {
            SET_COMPONENT_FROM_ID_CHILD(id, bool_value, tmp2->lhs->search);
            SET_COMPONENT_FROM_ID_CHILD(id, bool_value, tmp2->rhs->search);
        }
    }
    else
    {
        if (tmp1)
        {
            if (tmp1->search->set_component_from_id(id, component))
                return true;
        }
        if (tmp2)
        {
            if (tmp2->lhs->search->set_component_from_id(id, component))
                return true;
            if (tmp2->rhs->search->set_component_from_id(id, component))
                return true;
        }
    }

    SET_COMPONENT_FROM_ID_CHILD(id, component, lhs);
    return bool_expr::set_component_from_id(id, component);
}