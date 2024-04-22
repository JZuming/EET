#include "coalesce.hh"

coalesce::coalesce(prod *p, sqltype *type_constraint, const char *abbrev)
    : value_expr(p), abbrev_(abbrev)
{
    auto first_expr = value_expr::factory(this, type_constraint);
    auto second_expr = value_expr::factory(this, first_expr->type);

    retry_limit = 20;
    while (first_expr->type != second_expr->type)
    {
        retry();
        if (first_expr->type->consistent(second_expr->type))
            first_expr = value_expr::factory(this, second_expr->type);
        else
            second_expr = value_expr::factory(this, first_expr->type);
    }
    type = second_expr->type;

    value_exprs.push_back(first_expr);
    value_exprs.push_back(second_expr);
}

void coalesce::out(std::ostream &out)
{
    if (is_transformed && !has_print_eq_expr)
    {
        out_eq_value_expr(out);
        return;
    }

    out << abbrev_ << "(";
    for (auto expr = value_exprs.begin(); expr != value_exprs.end(); expr++)
    {
        out << **expr;
        if (expr + 1 != value_exprs.end())
            out << ", ";
    }
    out << ")";
}

void coalesce::accept(prod_visitor *v)
{
    v->visit(this);
    for (auto p : value_exprs)
        p->accept(v);
}

void coalesce::equivalent_transform()
{
    value_expr::equivalent_transform();
    for (auto &expr : value_exprs)
        expr->equivalent_transform();
}

void coalesce::back_transform()
{
    for (auto &expr : value_exprs)
        expr->back_transform();
    value_expr::back_transform();
}

void coalesce::set_component_id(int &id)
{
    value_expr::set_component_id(id);
    for (auto &expr : value_exprs)
        expr->set_component_id(id);
}
bool coalesce::get_component_from_id(int id, shared_ptr<value_expr> &component)
{
    for (auto &expr : value_exprs)
        GET_COMPONENT_FROM_ID_CHILD(id, component, expr);
    return value_expr::get_component_from_id(id, component);
}

bool coalesce::set_component_from_id(int id, shared_ptr<value_expr> component)
{
    for (auto &expr : value_exprs)
        SET_COMPONENT_FROM_ID_CHILD(id, component, expr);
    return value_expr::set_component_from_id(id, component);
}