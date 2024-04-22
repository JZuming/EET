#pragma once

#include "value_expr.hh"

struct coalesce : value_expr
{
    const char *abbrev_;
    vector<shared_ptr<value_expr>> value_exprs;
    virtual ~coalesce(){};
    coalesce(prod *p, sqltype *type_constraint = 0, const char *abbrev = "coalesce");
    virtual void out(std::ostream &out);
    virtual void accept(prod_visitor *v);
    virtual void equivalent_transform();
    virtual void back_transform();
    virtual void set_component_id(int &id);
    virtual bool get_component_from_id(int id, shared_ptr<value_expr> &component);
    virtual bool set_component_from_id(int id, shared_ptr<value_expr> component);
};

struct nullif : coalesce
{
    virtual ~nullif(){};
    nullif(prod *p, sqltype *type_constraint = 0)
        : coalesce(p, type_constraint, "nullif"){};
};