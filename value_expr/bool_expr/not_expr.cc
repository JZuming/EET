#include "not_expr.hh"

void not_expr::out(ostream &out)
{
    OUTPUT_EQ_BOOL_EXPR(out);
    out << "not (" << *inner_expr << ")";
}

void not_expr::equivalent_transform()
{
    bool_expr::equivalent_transform();
    inner_expr->equivalent_transform();
}

void not_expr::back_transform()
{
    inner_expr->back_transform();
    bool_expr::back_transform();
}

bool not_expr::set_component_from_id(int id, shared_ptr<value_expr> component)
{
    auto bool_value = dynamic_pointer_cast<bool_expr>(component);
    if (bool_value)
        SET_COMPONENT_FROM_ID_CHILD(id, bool_value, inner_expr);
    else
    {
        if (inner_expr->set_component_from_id(id, component))
            return true;
    }
    return bool_expr::set_component_from_id(id, component);
}

void not_expr::set_component_id(int &id)
{
    bool_expr::set_component_id(id);
    inner_expr->set_component_id(id);
}

bool not_expr::get_component_from_id(int id, shared_ptr<value_expr> &component)
{
    GET_COMPONENT_FROM_ID_CHILD(id, component, inner_expr);
    return bool_expr::get_component_from_id(id, component);
}