#pragma once

#include "../value_expr.hh"

#define OUTPUT_EQ_BOOL_EXPR(out) \
    do {\
        if (is_transformed && !has_print_eq_expr) \
        {\
            out_eq_bool_expr(out);\
            return;\
        }\
    } while(0);

struct bool_expr : value_expr
{
    virtual ~bool_expr() {}
    bool_expr(prod *p) : value_expr(p) { type = scope->schema->booltype; }
    static shared_ptr<bool_expr> factory(prod *p);

    shared_ptr<bool_expr> eq_bool_expr;
    virtual void equivalent_transform();
    virtual void back_transform();
    void out_eq_bool_expr(ostream &out);
};