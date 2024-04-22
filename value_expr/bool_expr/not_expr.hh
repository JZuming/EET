#pragma once

#include "bool_expr.hh"

struct not_expr : bool_expr
{
    shared_ptr<bool_expr> inner_expr;
    virtual ~not_expr() {}
    not_expr(prod *p) : bool_expr(p) { inner_expr = bool_expr::factory(p); }
    not_expr(prod *p, shared_ptr<bool_expr> bexpr) : bool_expr(p) { inner_expr = bexpr; }
    virtual void out(ostream &out);
    virtual void equivalent_transform();
    virtual void back_transform();
    virtual void set_component_id(int &id);
    virtual bool get_component_from_id(int id, shared_ptr<value_expr> &component);
    virtual bool set_component_from_id(int id, shared_ptr<value_expr> component);
};