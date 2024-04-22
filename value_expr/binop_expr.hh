#pragma once

#include "value_expr.hh"

struct binop_expr : value_expr
{
    shared_ptr<value_expr> lhs, rhs;
    op *oper;
    binop_expr(prod *p, sqltype *type_constraint = 0);
    binop_expr(prod *p, op *operation, shared_ptr<value_expr> l_operand, shared_ptr<value_expr> r_operand);
    virtual ~binop_expr() {}
    virtual void out(std::ostream &out);
    virtual void accept(prod_visitor *v);
    virtual void equivalent_transform();
    virtual void back_transform();
    virtual void set_component_id(int &id);
    virtual bool get_component_from_id(int id, shared_ptr<value_expr> &component);
    virtual bool set_component_from_id(int id, shared_ptr<value_expr> component);
};