#include "window_function.hh"
#include "grammar.hh"

void window_function::out(ostream &out)
{
    if (is_transformed && !has_print_eq_expr)
    {
        out_eq_value_expr(out);
        return;
    }

    out << *aggregate << " over (partition by ";
    for (auto ref = partition_by.begin(); ref != partition_by.end(); ref++)
    {
        out << **ref;
        if (ref + 1 != partition_by.end())
            out << ",";
    }

    out << " order by ";
    for (auto ref = order_by.begin(); ref != order_by.end(); ref++)
    {
        auto &order_pair = *ref;
        out << *(order_pair.first) << " ";
        out << (order_pair.second ? "asc" : "desc");
        if (ref + 1 != order_by.end())
            out << ", ";
    }

    out << ")";
}

window_function::window_function(prod *p, sqltype *type_constraint)
    : value_expr(p)
{
    match();
    aggregate = make_shared<win_funcall>(this, type_constraint);

    type = aggregate->type;
    partition_by.push_back(make_shared<column_reference>(this));
    while (d6() > 4)
        partition_by.push_back(make_shared<column_reference>(this));

    // order by all possible col ref, make the result determined
    for (auto r : scope->refs)
    {
        for (auto &c : (*r).columns())
        {
            auto col = make_shared<column_reference>(this, c.type, c.name, r->name);
            auto is_asc = d6() <= 3 ? true : false;
            order_by.push_back(make_pair<>(col, is_asc));
        }
    }
}

bool window_function::disabled = false;
bool window_function::allowed(prod *p)
{
    if (disabled)
        return false;
    if (dynamic_cast<select_list *>(p))
        return dynamic_cast<query_spec *>(p->pprod) ? true : false;
    if (dynamic_cast<window_function *>(p))
        return false;
    if (dynamic_cast<value_expr *>(p))
        return allowed(p->pprod);
    return false;
}

void window_function::accept(prod_visitor *v)
{
    v->visit(this);
    aggregate->accept(v);
    for (auto p : partition_by)
        p->accept(v);
    for (auto p : order_by)
        p.first->accept(v);
}