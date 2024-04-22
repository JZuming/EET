#pragma once

#include "../bool_expr.hh"

struct bool_binop : bool_expr
{
    shared_ptr<value_expr> lhs, rhs;
    bool_binop(prod *p) : bool_expr(p) {}
    virtual void out(std::ostream &out) = 0;
    virtual void accept(prod_visitor *v);
    virtual void equivalent_transform();
    virtual void back_transform();
    virtual void set_component_id(int &id);
    virtual bool get_component_from_id(int id, shared_ptr<value_expr> &component);
    virtual bool set_component_from_id(int id, shared_ptr<value_expr> component);
};