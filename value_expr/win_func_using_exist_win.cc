#include "win_func_using_exist_win.hh"

void win_func_using_exist_win::out(std::ostream &out)
{
    if (is_transformed && !has_print_eq_expr)
    {
        out_eq_value_expr(out);
        return;
    }
    
    out << *aggregate << " over " + exist_window;
}

win_func_using_exist_win::win_func_using_exist_win(prod *p, sqltype *type_constraint, string exist_win)
    : value_expr(p)
{
    aggregate = make_shared<win_funcall>(this, type_constraint);
    exist_window = exist_win;
    type = aggregate->type;
}

void win_func_using_exist_win::accept(prod_visitor *v)
{
    v->visit(this);
    aggregate->accept(v);
}