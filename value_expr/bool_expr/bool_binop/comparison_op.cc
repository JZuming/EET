#include "comparison_op.hh"

comparison_op::comparison_op(prod *p) : bool_binop(p)
{
    auto &idx = p->scope->schema->operators_returning_type;

    auto iters = idx.equal_range(scope->schema->booltype);
    oper = random_pick<>(iters)->second;

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

comparison_op::comparison_op(prod *p, op *target_op,
                             shared_ptr<value_expr> left_operand,
                             shared_ptr<value_expr> right_operand) : bool_binop(p)
{
    oper = target_op;
    lhs = left_operand;
    rhs = right_operand;
}

void comparison_op::out(std::ostream &o)
{
    OUTPUT_EQ_BOOL_EXPR(o);
    o << "(" << *lhs << ") " << oper->name << " (" << *rhs << ")";
}