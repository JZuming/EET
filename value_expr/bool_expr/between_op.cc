#include "between_op.hh"
#include "bool_binop/comparison_op.hh"
#include "bool_binop/bool_term.hh"

between_op::between_op(prod *p) : bool_expr(p)
{
    mhs = value_expr::factory(this, scope->schema->inttype);
    lhs = value_expr::factory(this, scope->schema->inttype);
    rhs = value_expr::factory(this, scope->schema->inttype);
}

void between_op::equivalent_transform()
{
    bool_expr::equivalent_transform();
    lhs->equivalent_transform();
    mhs->equivalent_transform();
    rhs->equivalent_transform();

    bool has_ge = false;
    op *ge_oper;
    op *le_oper;
    bool has_le = false;
    for (auto &op : scope->schema->operators)
    {
        if (op.name == ">=")
        {
            ge_oper = &op;
            has_ge = true;
        }
        if (op.name == "<=")
        {
            le_oper = &op;
            has_le = true;
        }
    }

    if (!has_ge || !has_le)
        return;

    use_eq_expr = true;
    auto mhs_ge_lhs = make_shared<comparison_op>(this, ge_oper, mhs, lhs);
    auto mhs_le_rhs = make_shared<comparison_op>(this, le_oper, mhs, rhs);
    eq_expr = make_shared<bool_term>(this, false, mhs_ge_lhs, mhs_le_rhs);
}

void between_op::back_transform()
{
    use_eq_expr = false;

    rhs->back_transform();
    mhs->back_transform();
    lhs->back_transform();
    bool_expr::back_transform();
}

void between_op::out(ostream &o)
{
    OUTPUT_EQ_BOOL_EXPR(o);
    
    if (use_eq_expr == false)
        o << "(" << *mhs << ") between (" << *lhs << ") and (" << *rhs << ")";
    else
        o << *eq_expr;
}

void between_op::accept(prod_visitor *v)
{
    v->visit(this);
    mhs->accept(v);
    lhs->accept(v);
    rhs->accept(v);
}

void between_op::set_component_id(int &id)
{
    bool_expr::set_component_id(id);
    lhs->set_component_id(id);
    mhs->set_component_id(id);
    rhs->set_component_id(id);
}

bool between_op::get_component_from_id(int id, shared_ptr<value_expr> &component)
{
    GET_COMPONENT_FROM_ID_CHILD(id, component, lhs);
    GET_COMPONENT_FROM_ID_CHILD(id, component, mhs);
    GET_COMPONENT_FROM_ID_CHILD(id, component, rhs);
    return bool_expr::get_component_from_id(id, component);
}

bool between_op::set_component_from_id(int id, shared_ptr<value_expr> component)
{
    SET_COMPONENT_FROM_ID_CHILD(id, component, lhs);
    SET_COMPONENT_FROM_ID_CHILD(id, component, mhs);
    SET_COMPONENT_FROM_ID_CHILD(id, component, rhs);
    return bool_expr::set_component_from_id(id, component);
}