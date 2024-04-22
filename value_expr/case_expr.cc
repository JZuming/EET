#include "case_expr.hh"
#include "expr.hh"

case_expr::case_expr(prod *p, sqltype *type_constraint)
    : value_expr(p)
{
    condition = bool_expr::factory(this);
    true_expr = value_expr::factory(this, type_constraint);
    false_expr = value_expr::factory(this, true_expr->type);

    if (false_expr->type != true_expr->type)
    {
        /* Types are consistent but not identical.  Try to find a more
           concrete one for a better match. */
        if (true_expr->type->consistent(false_expr->type))
            true_expr = value_expr::factory(this, false_expr->type);
        else
            false_expr = value_expr::factory(this, true_expr->type);
    }
    type = true_expr->type;
}

case_expr::case_expr(prod *p, shared_ptr<bool_expr> c, shared_ptr<value_expr> t, shared_ptr<value_expr> f)
    : value_expr(p)
{
    condition = c;
    true_expr = t;
    false_expr = f;
    type = true_expr->type;
    assert(type == false_expr->type);
}

void case_expr::out(std::ostream &out)
{
    if (is_transformed && !has_print_eq_expr)
    {
        out_eq_value_expr(out);
        return;
    }
    
    out << "case when (" << *condition;
    out << ") then (" << *true_expr;
    out << ") else (" << *false_expr;
    out << ") end";
    indent(out);
}

void case_expr::accept(prod_visitor *v)
{
    v->visit(this);
    condition->accept(v);
    true_expr->accept(v);
    false_expr->accept(v);
}

void case_expr::equivalent_transform()
{
    value_expr::equivalent_transform();
    condition->equivalent_transform();
    true_expr->equivalent_transform();
    false_expr->equivalent_transform();
}

void case_expr::back_transform()
{
    false_expr->back_transform();
    true_expr->back_transform();
    condition->back_transform();
    value_expr::back_transform();
}

void case_expr::set_component_id(int &id)
{
    value_expr::set_component_id(id);
    condition->set_component_id(id);
    true_expr->set_component_id(id);
    false_expr->set_component_id(id);
}

bool case_expr::get_component_from_id(int id, shared_ptr<value_expr> &component)
{
    GET_COMPONENT_FROM_ID_CHILD(id, component, condition);
    GET_COMPONENT_FROM_ID_CHILD(id, component, true_expr);
    GET_COMPONENT_FROM_ID_CHILD(id, component, false_expr);
    return value_expr::get_component_from_id(id, component);
}

bool case_expr::set_component_from_id(int id, shared_ptr<value_expr> component)
{
    auto bool_value = dynamic_pointer_cast<bool_expr>(component);
    if (bool_value)
    {
        SET_COMPONENT_FROM_ID_CHILD(id, bool_value, condition);
    }
    else
    {
        if (condition->set_component_from_id(id, component))
            return true;
    }
    SET_COMPONENT_FROM_ID_CHILD(id, component, true_expr);
    SET_COMPONENT_FROM_ID_CHILD(id, component, false_expr);
    return value_expr::set_component_from_id(id, component);
}