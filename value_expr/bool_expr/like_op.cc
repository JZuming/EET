#include "like_op.hh"

like_op::like_op(prod *p) : bool_expr(p)
{
#define LIKE_FORMAT_LENGTH 3

    lhs = value_expr::factory(this, scope->schema->texttype);
    like_format = random_string(LIKE_FORMAT_LENGTH); // fix length 3
    for (int pos = 0; pos < like_format.size(); pos++)
    {
        if (like_format[pos] == '\\')
            continue;
        auto choice = d12();
        if (choice <= 3)
            like_format[pos] = '%';
        else if (choice <= 6)
            like_format[pos] = '_';
    }
    like_format = "'" + like_format + "'";
    if (d6() < 4)
        like_operator = " like ";
    else
        like_operator = " not like ";
}

void like_op::out(std::ostream &o)
{
    OUTPUT_EQ_BOOL_EXPR(o);
    o << "(" << *lhs << ")" << like_operator << like_format;
}

void like_op::accept(prod_visitor *v)
{
    v->visit(this);
    lhs->accept(v);
}

void like_op::equivalent_transform()
{
    bool_expr::equivalent_transform();
    lhs->equivalent_transform();
}

void like_op::back_transform()
{
    lhs->back_transform();
    bool_expr::back_transform();
}

void like_op::set_component_id(int &id)
{
    bool_expr::set_component_id(id);
    lhs->set_component_id(id);
}

bool like_op::get_component_from_id(int id, shared_ptr<value_expr> &component)
{
    GET_COMPONENT_FROM_ID_CHILD(id, component, lhs);
    return bool_expr::get_component_from_id(id, component);
}

bool like_op::set_component_from_id(int id, shared_ptr<value_expr> component)
{
    SET_COMPONENT_FROM_ID_CHILD(id, component, lhs);
    return bool_expr::set_component_from_id(id, component);
}