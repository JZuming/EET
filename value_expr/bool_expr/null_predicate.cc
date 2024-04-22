#include "null_predicate.hh"

null_predicate::null_predicate(prod *p) : bool_expr(p)
{
    negate = ((d6() < 4) ? "not " : "");
    expr = value_expr::factory(this);
}

null_predicate::null_predicate(prod *p, shared_ptr<value_expr> value, bool is_null) : bool_expr(p)
{
    negate = is_null ? "" : "not ";
    expr = value;
}

void null_predicate::out(ostream &out)
{
    OUTPUT_EQ_BOOL_EXPR(out);
    out << "(" << *expr << ") is " << negate << scope->schema->null_literal;
}

void null_predicate::accept(prod_visitor *v)
{
    v->visit(this);
    expr->accept(v);
}

void null_predicate::equivalent_transform()
{
    bool_expr::equivalent_transform();
    expr->equivalent_transform();
}

void null_predicate::back_transform()
{
    expr->back_transform();
    bool_expr::back_transform();
}

void null_predicate::set_component_id(int &id)
{
    bool_expr::set_component_id(id);
    expr->set_component_id(id);
}

bool null_predicate::get_component_from_id(int id, shared_ptr<value_expr> &component)
{
    GET_COMPONENT_FROM_ID_CHILD(id, component, expr);
    return bool_expr::get_component_from_id(id, component);
}

bool null_predicate::set_component_from_id(int id, shared_ptr<value_expr> component)
{
    SET_COMPONENT_FROM_ID_CHILD(id, component, expr);
    return bool_expr::set_component_from_id(id, component);
}