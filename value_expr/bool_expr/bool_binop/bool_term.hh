#pragma once

#include "bool_binop.hh"

struct bool_term : bool_binop
{
    virtual ~bool_term() {}
    string op;
    bool has_equal_expr = false;
    shared_ptr<bool_expr> equal_expr;
    virtual void out(ostream &out);
    bool_term(prod *p);
    bool_term(prod *p, bool is_or, shared_ptr<bool_expr> given_lhs, shared_ptr<bool_expr> given_rhs);
    virtual void equivalent_transform();
    virtual void back_transform();
    virtual void set_component_id(int &id);
    virtual bool get_component_from_id(int id, shared_ptr<value_expr> &component);
    virtual bool set_component_from_id(int id, shared_ptr<value_expr> component);
};