#include "const_bool.hh"
#include "../expr.hh"

const_bool::const_bool(prod *p) : bool_expr(p)
{
    auto choice = d6();
    if (choice <= 2)
        op = scope->schema->true_literal;
    else if (choice <= 4)
        op = scope->schema->false_literal;
    else
        op = scope->schema->null_literal;
}

// 1: true_literal
// 0: false_literal
// -1: null_literal
const_bool::const_bool(prod *p, int is_true) : bool_expr(p)
{
    if (is_true > 0)
        op = scope->schema->true_literal;
    else if (is_true == 0)
        op = scope->schema->false_literal;
    else
        op = scope->schema->null_literal;
}

void const_bool::out(std::ostream &out)
{
    if (!is_transformed) {
        if (scope->schema->target_dbms == "postgres")
            out << op << "::" << scope->schema->booltype->name;
        else
            out << op;
    }
    else
        out << *eq_expr;
}

void const_bool::equivalent_transform()
{
    mark_transformed();

    // if it already has equal expression, just skip
    if (eq_expr)
        return;

    if (op == scope->schema->null_literal)
    {
        eq_expr = make_shared<const_bool>(this, -1);
        return;
    }

    shared_ptr<bool_expr> extend_expr;
    while (1)
    {
        try
        {
            extend_expr = bool_expr::factory(this);
            break;
        }
        catch (exception &e)
        {
            cerr << "exception in const_bool::equivalent_transform [" << e.what() << "]" << endl;
            continue;
        }
    }

    if (op == scope->schema->false_literal)
    { // anything and false is false
        auto new_itself = make_shared<const_bool>(this, 0);
        eq_expr = make_shared<bool_term>(this, false, extend_expr, new_itself);
        return;
    }
    if (op == scope->schema->true_literal)
    { // anything or true is true
        auto new_itself = make_shared<const_bool>(this, 1);
        eq_expr = make_shared<bool_term>(this, true, extend_expr, new_itself);
        return;
    }
}