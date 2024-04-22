#include "comp_subquery.hh"
#include "grammar.hh"

comp_subquery::comp_subquery(prod *p) : bool_expr(p), myscope(scope)
{
    myscope.tables = scope->tables;
    scope = &myscope;

    lhs = value_expr::factory(this);

    auto chosen_comp = d6(); // =  >  <  >=  <=  <>
    switch (chosen_comp)
    {
    case 1:
        comp_op = "=";
        break;
    case 2:
        comp_op = "<>";
        break;
    case 3:
        comp_op = ">";
        break;
    case 4:
        comp_op = "<";
        break;
    case 5:
        comp_op = ">=";
        break;
    case 6:
        comp_op = "<=";
        break;
    default:
        comp_op = "<>";
        break;
    }

    vector<sqltype *> pointed_type;
    pointed_type.push_back(lhs->type);

    // clickhouse does not support correlated subqueries.
    // it use seperated my scope, do not need to restore the refs
    if (schema::target_dbms == "clickhouse")
        scope->refs.clear();

    auto subquery = make_shared<query_spec>(this, scope, false, &pointed_type);
    if (lhs->type != subquery->select_list->value_exprs.front()->type) {
        cerr << "lhs: " << lhs->type->name << endl;
        cerr << "subquery: " << subquery->select_list->value_exprs.front()->type->name << endl;
        cerr << "has group: " << subquery->has_group << endl;
        abort();
    }

    subquery->has_group = false;
    if (subquery->has_order == false)
    {
        subquery->has_order = true;
        auto &selected_columns = subquery->select_list->derived_table.columns();
        for (auto &col : selected_columns)
        {
            auto col_name = col.name;
            auto is_asc = d6() > 3 ? true : false;
            subquery->order_clause.push_back(make_pair<>(col_name, is_asc));
        }
    }
    subquery->has_limit = true;
    subquery->limit_num = 1;
    target_subquery = subquery;
}

void comp_subquery::out(std::ostream &out)
{
    OUTPUT_EQ_BOOL_EXPR(out);
    
    out << "(" << *lhs << ") " << comp_op << " ( ";
    indent(out);
    out << *target_subquery << ")";
}

void comp_subquery::accept(prod_visitor *v)
{
    v->visit(this);
    lhs->accept(v);
    target_subquery->accept(v);
};

void comp_subquery::equivalent_transform()
{
    bool_expr::equivalent_transform();
    lhs->equivalent_transform();
    auto tmp1 = dynamic_pointer_cast<query_spec>(target_subquery);
    if (tmp1)
    {
        tmp1->search->equivalent_transform();
    }
    auto tmp2 = dynamic_pointer_cast<unioned_query>(target_subquery);
    if (tmp2)
    {
        tmp2->lhs->search->equivalent_transform();
        tmp2->rhs->search->equivalent_transform();
    }
}

void comp_subquery::back_transform()
{
    auto tmp1 = dynamic_pointer_cast<query_spec>(target_subquery);
    if (tmp1)
    {
        tmp1->search->back_transform();
    }
    auto tmp2 = dynamic_pointer_cast<unioned_query>(target_subquery);
    if (tmp2)
    {
        tmp2->lhs->search->back_transform();
        tmp2->rhs->search->back_transform();
    }
    lhs->back_transform();
    bool_expr::back_transform();
}

void comp_subquery::set_component_id(int &id)
{
    bool_expr::set_component_id(id);
    lhs->set_component_id(id);
    auto tmp1 = dynamic_pointer_cast<query_spec>(target_subquery);
    if (tmp1)
    {
        tmp1->search->set_component_id(id);
    }
    auto tmp2 = dynamic_pointer_cast<unioned_query>(target_subquery);
    if (tmp2)
    {
        tmp2->lhs->search->set_component_id(id);
        tmp2->rhs->search->set_component_id(id);
    }
}

bool comp_subquery::get_component_from_id(int id, shared_ptr<value_expr> &component)
{
    auto tmp1 = dynamic_pointer_cast<query_spec>(target_subquery);
    if (tmp1)
    {
        GET_COMPONENT_FROM_ID_CHILD(id, component, tmp1->search);
    }
    auto tmp2 = dynamic_pointer_cast<unioned_query>(target_subquery);
    if (tmp2)
    {
        GET_COMPONENT_FROM_ID_CHILD(id, component, tmp2->lhs->search);
        GET_COMPONENT_FROM_ID_CHILD(id, component, tmp2->rhs->search);
    }
    GET_COMPONENT_FROM_ID_CHILD(id, component, lhs);
    return bool_expr::get_component_from_id(id, component);
}

bool comp_subquery::set_component_from_id(int id, shared_ptr<value_expr> component)
{
    auto bool_value = dynamic_pointer_cast<bool_expr>(component);
    auto tmp1 = dynamic_pointer_cast<query_spec>(target_subquery);
    auto tmp2 = dynamic_pointer_cast<unioned_query>(target_subquery);
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
    { // if component is not bool_expr, do consider tmp1->search itself
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