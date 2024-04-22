#include "bool_binop.hh"

void bool_binop::accept(prod_visitor *v)
{
    v->visit(this);
    lhs->accept(v);
    rhs->accept(v);
}

void bool_binop::equivalent_transform()
{
    bool_expr::equivalent_transform();
    lhs->equivalent_transform();
    rhs->equivalent_transform();
}

void bool_binop::back_transform()
{
    rhs->back_transform();
    lhs->back_transform();
    bool_expr::back_transform();
}

void bool_binop::set_component_id(int &id)
{
    bool_expr::set_component_id(id);
    lhs->set_component_id(id);
    rhs->set_component_id(id);
}

bool bool_binop::get_component_from_id(int id, shared_ptr<value_expr> &component)
{
    GET_COMPONENT_FROM_ID_CHILD(id, component, lhs);
    GET_COMPONENT_FROM_ID_CHILD(id, component, rhs);
    return bool_expr::get_component_from_id(id, component);
}

bool bool_binop::set_component_from_id(int id, shared_ptr<value_expr> component)
{
    SET_COMPONENT_FROM_ID_CHILD(id, component, lhs);
    SET_COMPONENT_FROM_ID_CHILD(id, component, rhs);
    return bool_expr::set_component_from_id(id, component);
}