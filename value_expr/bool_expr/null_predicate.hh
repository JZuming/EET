#pragma once

#include "bool_expr.hh"

struct null_predicate : bool_expr
{
    const char *negate;
    shared_ptr<value_expr> expr;
    virtual ~null_predicate() {}
    null_predicate(prod *p);
    null_predicate(prod *p, shared_ptr<value_expr> value, bool is_null);

    virtual void out(std::ostream &out);
    virtual void accept(prod_visitor *v);
    virtual void equivalent_transform();
    virtual void back_transform();
    virtual void set_component_id(int &id);
    virtual bool get_component_from_id(int id, shared_ptr<value_expr> &component);
    virtual bool set_component_from_id(int id, shared_ptr<value_expr> component);
};