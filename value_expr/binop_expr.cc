#include "binop_expr.hh"

binop_expr::binop_expr(prod *p, sqltype *type_constraint) : value_expr(p)
{
    auto &idx = p->scope->schema->operators_returning_type;
retry:
    if (type_constraint)
    {
        auto iters = idx.equal_range(type_constraint);
        oper = random_pick<>(iters)->second;
        if (oper && !type_constraint->consistent(oper->result))
        {
            retry();
            goto retry;
        }
    }
    else
    {
        oper = random_pick(idx.begin(), idx.end())->second;
    }
    type = oper->result;

    lhs = value_expr::factory(this, oper->left);
    rhs = value_expr::factory(this, oper->right);

    if (oper->left == oper->right && lhs->type != rhs->type)
    {
        if (lhs->type->consistent(rhs->type))
            lhs = value_expr::factory(this, rhs->type);
        else
            rhs = value_expr::factory(this, lhs->type);
    }
}

binop_expr::binop_expr(prod *p, op *operation, shared_ptr<value_expr> l_operand, shared_ptr<value_expr> r_operand)
    : value_expr(p)
{
    oper = operation;
    lhs = l_operand;
    rhs = r_operand;
}

void binop_expr::out(std::ostream &out)
{
    if (is_transformed && !has_print_eq_expr)
    {
        out_eq_value_expr(out);
        return;
    }
    out << "(" << *lhs << ") " << oper->name << " (" << *rhs << ")";
}

void binop_expr::accept(prod_visitor *v)
{
    v->visit(this);
    lhs->accept(v);
    rhs->accept(v);
}

void binop_expr::equivalent_transform()
{
    value_expr::equivalent_transform();
    lhs->equivalent_transform();
    rhs->equivalent_transform();
}

void binop_expr::back_transform()
{
    rhs->back_transform();
    lhs->back_transform();
    value_expr::back_transform();
}

void binop_expr::set_component_id(int &id)
{
    value_expr::set_component_id(id);
    lhs->set_component_id(id);
    rhs->set_component_id(id);
}

bool binop_expr::get_component_from_id(int id, shared_ptr<value_expr> &component)
{
    GET_COMPONENT_FROM_ID_CHILD(id, component, lhs);
    GET_COMPONENT_FROM_ID_CHILD(id, component, rhs);
    return value_expr::get_component_from_id(id, component);
}

bool binop_expr::set_component_from_id(int id, shared_ptr<value_expr> component)
{
    SET_COMPONENT_FROM_ID_CHILD(id, component, lhs);
    SET_COMPONENT_FROM_ID_CHILD(id, component, rhs);
    return value_expr::set_component_from_id(id, component);
}