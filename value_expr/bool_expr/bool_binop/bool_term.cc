#include "bool_term.hh"
#include "../../expr.hh"

bool_term::bool_term(prod *p) : bool_binop(p)
{
    op = ((d6() < 4) ? "or" : "and");
    lhs = bool_expr::factory(this);
    rhs = bool_expr::factory(this);
}

bool_term::bool_term(prod *p, bool is_or,
                     shared_ptr<bool_expr> given_lhs,
                     shared_ptr<bool_expr> given_rhs) : bool_binop(p)
{
    op = (is_or ? "or" : "and");
    lhs = given_lhs;
    rhs = given_rhs;
}

void bool_term::out(ostream &out)
{
    OUTPUT_EQ_BOOL_EXPR(out);
    
    if (has_equal_expr == false)
    {
        out << "(" << *lhs << ") ";
        indent(out);
        out << op << " (" << *rhs << ")";
    }
    else
    {
        out << *equal_expr;
    }
}

void bool_term::equivalent_transform()
{
    bool_binop::equivalent_transform();
    if (op == "or")
        op = "and";
    else
        op = "or";

    auto bool_lhs = dynamic_pointer_cast<bool_expr>(lhs);
    auto bool_rhs = dynamic_pointer_cast<bool_expr>(rhs);
    auto tmp1 = make_shared<not_expr>(this, bool_lhs);
    auto tmp2 = make_shared<not_expr>(this, bool_rhs);
    auto new_bool_term = make_shared<bool_term>(this, op == "or", tmp2, tmp1);
    equal_expr = make_shared<not_expr>(this, new_bool_term);
    has_equal_expr = true;
}

void bool_term::back_transform()
{
    has_equal_expr = false;
    auto not_expression = dynamic_pointer_cast<not_expr>(equal_expr);
    auto new_bool_term = dynamic_pointer_cast<bool_term>(not_expression->inner_expr);
    equal_expr.reset();
    auto tmp2 = dynamic_pointer_cast<not_expr>(new_bool_term->lhs);
    auto tmp1 = dynamic_pointer_cast<not_expr>(new_bool_term->rhs);
    lhs = tmp1->inner_expr;
    rhs = tmp2->inner_expr;
    if (op == "or")
        op = "and";
    else
        op = "or";
    bool_binop::back_transform();
}

void bool_term::set_component_id(int &id)
{
    bool_binop::set_component_id(id);
    if (has_equal_expr)
        equal_expr->set_component_id(id);
}

bool bool_term::get_component_from_id(int id, shared_ptr<value_expr> &component)
{
    if (has_equal_expr)
        GET_COMPONENT_FROM_ID_CHILD(id, component, equal_expr);
    return bool_binop::get_component_from_id(id, component);
}

bool bool_term::set_component_from_id(int id, shared_ptr<value_expr> component)
{
    if (has_equal_expr)
    {
        auto bool_value = dynamic_pointer_cast<bool_expr>(component);
        if (bool_value)
            SET_COMPONENT_FROM_ID_CHILD(id, bool_value, equal_expr);
        else
        {
            if (equal_expr->set_component_from_id(id, bool_value)) 
                return true;
        }
    }
    return bool_binop::set_component_from_id(id, component);
}