#include "exists_predicate.hh"
#include "grammar.hh"

exists_predicate::exists_predicate(prod *p) : bool_expr(p), myscope(scope)
{
    myscope.tables = scope->tables;
    scope = &myscope;

    // clickhouse does not support correlated subqueries.
    // it use seperated my scope, do not need to restore the refs
    if (schema::target_dbms == "clickhouse")
        scope->refs.clear();
    
    // disable order and limit to make sure that exists clause can be accurately transformed to in clause
    if (d6() < 4)
    {
        auto tmp = make_shared<unioned_query>(this, scope);
        tmp->lhs->has_order = false;
        tmp->lhs->has_limit = false;
        tmp->rhs->has_order = false;
        tmp->rhs->has_limit = false;
        subquery = tmp;
    }
    else
    {
        auto tmp = make_shared<query_spec>(this, scope);
        tmp->has_order = false; // disable order
        tmp->has_limit = false; // disable limit
        subquery = tmp;
    }
}

exists_predicate::exists_predicate(prod *p, shared_ptr<prod> subquery)
    : bool_expr(p)
{
    this->subquery = subquery;
}

void exists_predicate::accept(prod_visitor *v)
{
    v->visit(this);
    subquery->accept(v);
}

void exists_predicate::out(std::ostream &out)
{
    OUTPUT_EQ_BOOL_EXPR(out);
    
    if (is_transformed)
    {
        out << *eq_exer;
        return;
    }

    out << "exists (";
    indent(out);
    out << *subquery << ")";
}

void exists_predicate::equivalent_transform()
{
    bool_expr::equivalent_transform();
    auto tmp1 = dynamic_pointer_cast<query_spec>(subquery);
    auto tmp2 = dynamic_pointer_cast<unioned_query>(subquery);

    assert(tmp1 || tmp2);

    if (tmp1)
    {
        tmp1->search->equivalent_transform();
        // clickhouse does not support subqueries in select list
        if (schema::target_dbms != "clickhouse") {
            for (auto expr : tmp1->select_list->value_exprs)
                expr->equivalent_transform();
        }
    }
    else if (tmp2)
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

    assert(ori_columns.empty());
    assert(ori_derived_table.empty());
    assert(ori_value_exprs.empty());

    if (tmp1)
    {
        if (tmp1->has_group || tmp1->has_window)
        { // do nothing
            eq_exer = make_shared<exists_predicate>(this, tmp1);
        }
        else
        {
            auto true_value = make_shared<const_bool>(this, true);
            auto predicate = tmp1->search;
            // should use "case when p is null then false else p end". Because when p is null,
            // exists (select a from b where null) => false, but true in (select null from b) => null,
            // the result are inconsistent. 
            // However true in (select case when p is null then false else p end is true from b) => false,
            // the results are consistent
            auto is_null = make_shared<null_predicate>(this, predicate, true);
            auto false_value = make_shared<const_bool>(this, 0);
            auto new_predicate = make_shared<case_expr>(this, is_null, false_value, predicate);
            tmp1->search = true_value;
            auto slist = tmp1->select_list;

            ori_columns.push_back(slist->columns);
            ori_derived_table.push_back(slist->derived_table);
            ori_value_exprs.push_back(slist->value_exprs);

            slist->columns = 0;
            slist->derived_table.columns().clear();
            slist->value_exprs.clear();
            slist->value_exprs.push_back(new_predicate);
            ostringstream name;
            name << "c" << slist->columns++;
            sqltype *t = new_predicate->type;
            assert(t);
            slist->derived_table.columns().push_back(column(name.str(), t));

            eq_exer = make_shared<in_query>(this, true_value, tmp1);
        }
    }
    else if (tmp2)
    {
        // cannot handle intersect or except, so just keep it the same
        if (schema::target_dbms != "clickhouse")
            tmp2->equivalent_transform();
        eq_exer = make_shared<exists_predicate>(this, tmp2);
    }
}

void exists_predicate::back_transform()
{
    auto tmp1 = dynamic_pointer_cast<query_spec>(subquery);
    auto tmp2 = dynamic_pointer_cast<unioned_query>(subquery);

    if (tmp1 && tmp1->has_group == false && tmp1->has_window == false)
    {
        auto new_predicate = tmp1->select_list->value_exprs.back();
        auto case_expr_ = dynamic_pointer_cast<case_expr>(new_predicate);
        assert(case_expr_);
        auto bool_predicate = dynamic_pointer_cast<bool_expr>(case_expr_->false_expr);
        assert(bool_predicate);

        tmp1->search = dynamic_pointer_cast<bool_expr>(bool_predicate);
        auto slist = tmp1->select_list;
        slist->columns = ori_columns.back();
        slist->derived_table = ori_derived_table.back();
        slist->value_exprs = ori_value_exprs.back();

        ori_columns.pop_back();
        ori_derived_table.pop_back();
        ori_value_exprs.pop_back();
    }
    else if (tmp2)
    {
        if (schema::target_dbms != "clickhouse")
            tmp2->back_transform();
        // do nothing
    }

    if (tmp1)
    {
        // clickhouse does not support subqueries in select list
        if (schema::target_dbms != "clickhouse") {
            for (auto expr : tmp1->select_list->value_exprs)
                expr->back_transform();
        }
        tmp1->search->back_transform();
    }
    else if (tmp2)
    {
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

    bool_expr::back_transform();
}

void exists_predicate::set_component_id(int &id)
{
    bool_expr::set_component_id(id);
    auto tmp1 = dynamic_pointer_cast<query_spec>(subquery);
    if (tmp1)
    {
        tmp1->search->set_component_id(id);
    }
    auto tmp2 = dynamic_pointer_cast<unioned_query>(subquery);
    if (tmp2)
    {
        tmp2->lhs->search->set_component_id(id);
        tmp2->rhs->search->set_component_id(id);
    }
}

bool exists_predicate::get_component_from_id(int id, shared_ptr<value_expr> &component)
{
    auto tmp1 = dynamic_pointer_cast<query_spec>(subquery);
    if (tmp1)
    {
        GET_COMPONENT_FROM_ID_CHILD(id, component, tmp1->search);
    }
    auto tmp2 = dynamic_pointer_cast<unioned_query>(subquery);
    if (tmp2)
    {
        GET_COMPONENT_FROM_ID_CHILD(id, component, tmp2->lhs->search);
        GET_COMPONENT_FROM_ID_CHILD(id, component, tmp2->rhs->search);
    }
    return bool_expr::get_component_from_id(id, component);
}

bool exists_predicate::set_component_from_id(int id, shared_ptr<value_expr> component)
{
    auto bool_value = dynamic_pointer_cast<bool_expr>(component);
    auto tmp1 = dynamic_pointer_cast<query_spec>(subquery);
    auto tmp2 = dynamic_pointer_cast<unioned_query>(subquery);
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
    return bool_expr::set_component_from_id(id, component);
}
