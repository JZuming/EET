#pragma once

#include "value_expr.hh"

struct funcall : value_expr
{
    routine *proc;
    vector<shared_ptr<value_expr>> parms;
    bool is_aggregate;
    virtual void out(std::ostream &out);
    virtual ~funcall() {}
    funcall(prod *p, sqltype *type_constraint = 0, bool agg = 0);
    funcall(prod *p, routine *r, vector<shared_ptr<value_expr>> in_parms, bool agg = false) : value_expr(p), proc(r), parms(in_parms), is_aggregate(agg) {}

    virtual void accept(prod_visitor *v);
    
    virtual void equivalent_transform();
    virtual void back_transform();

    virtual void set_component_id(int &id);
    virtual bool get_component_from_id(int id, shared_ptr<value_expr> &component);
    virtual bool set_component_from_id(int id, shared_ptr<value_expr> component);
};
