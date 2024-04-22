#include "column_reference.hh"
#include "funcall.hh"

extern table* handle_table;

column_reference::column_reference(prod *p, sqltype *type_constraint,
                                   vector<shared_ptr<named_relation>> *prefer_refs) : value_expr(p)
{
    named_relation *table_ref_ptr;
    column column_ref("", scope->schema->inttype);

    if (type_constraint)
    {
        auto pairs = scope->refs_of_type(type_constraint);
        auto picked = random_pick(pairs);
        type = picked.second.type;
        assert(type_constraint->consistent(type));

        table_ref_ptr = picked.first;
        column_ref = picked.second;
    }
    else
    {
        named_relation *r;
        if (prefer_refs != 0)
            r = &*random_pick(*prefer_refs);
        else
            r = random_pick(scope->refs);
        column &c = random_pick(r->columns());
        type = c.type;

        table_ref_ptr = r;
        column_ref = c;
    }
    assert(column_ref.name != "" && column_ref.type);

    table_ref = table_ref_ptr->ident();
    if (column_ref.agg_used == NULL) {
        if (schema::target_dbms == "clickhouse"
            && handle_table != NULL
            && table_ref_ptr->ident() == handle_table->ident()) {
            reference = column_ref.name;
        } else
            reference = table_ref_ptr->ident() + "." + column_ref.name;
    }
    else
    {
        vector<shared_ptr<value_expr>> parms;
        parms.push_back(make_shared<column_reference>(this, column_ref.type, column_ref.name, table_ref_ptr->ident()));
        auto agg_func = make_shared<funcall>(this, column_ref.agg_used, parms, true);
        ostringstream s;
        agg_func->out(s);
        reference = s.str();
    }
}

column_reference::column_reference(prod *p, sqltype *column_type,
                                   string column_name, string table_name) : value_expr(p)
{
    type = column_type;
    if (schema::target_dbms == "clickhouse" 
        && handle_table != NULL
        && table_name == handle_table->ident()) {
        reference = column_name;
    } else
        reference = table_name + "." + column_name;
    
    table_ref = table_name;
}

void column_reference::out(ostream &o)
{
    if (is_transformed && !has_print_eq_expr)
    {
        out_eq_value_expr(o);
        return;
    }
    o << reference;
}