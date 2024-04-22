#pragma once

#include "bool_binop.hh"

struct comparison_op : bool_binop
{
    op *oper;
    comparison_op(prod *p);
    comparison_op(prod *p, op *target_op,
                  shared_ptr<value_expr> left_operand,
                  shared_ptr<value_expr> right_operand);
    virtual ~comparison_op(){};
    virtual void out(std::ostream &o);
};