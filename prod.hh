/// @file
/// @brief Base class for grammar productions

#ifndef PROD_HH
#define PROD_HH

#include <string>
#include <iostream>
#include <vector>

#define TEST_MYSQL
#define TEST_TIDB

#define MAX_TRY_TIME 1
#define TEST_TIME_FOR_EACH_DB 10
#define TRANSACTION_TIMEOUT 360000 // 100 hour == no timeout
#define NOT_TEST_TXN

#define WKEY_INDEX 0
#define PKEY_INDEX 1
#define PKEY_IDENT "pkey"
#define VKEY_IDENT "vkey"

using namespace std;

typedef vector<string> row_output; // a row consists of several field(string)
typedef vector<row_output> stmt_output; // one output consits of several rows

enum txn_status {NOT_DEFINED, TXN_COMMIT, TXN_ABORT};

extern int cpu_affinity;

/// Base class for walking the AST
struct prod_visitor {
  virtual void visit(struct prod *p) = 0;
  virtual ~prod_visitor() { }
};

/// Base class for AST nodes
struct prod {
  /// Parent production that instanciated this one.  May be NULL for
  /// top-level productions.
  struct prod *pprod;
  /// Scope object to model column/table reference visibility.
  struct scope *scope;
  /// Level of this production in the AST.  0 for root node.
  int level;
  /// Number of retries in this production.  Child productions are
  /// generated speculatively and may fail.
  long retries = 0;
  /// Maximum number of retries allowed before reporting a failure to
  /// the Parent prod.
  long retry_limit = 100;
  prod(prod *parent);
  /// Newline and indent according to tree level.
  virtual void indent(std::ostream &out);
  /// Emit SQL for this production.
  virtual void out(std::ostream &out) = 0;
  /// Check with the impedance matching code whether this production
  /// has been blacklisted and throw an exception.
  virtual void match();
  /// Visitor pattern for walking the AST.  Make sure you visit all
  /// child production when deriving classes.
  virtual void accept(prod_visitor *v) { v->visit(this); }
  /// Report a "failed to generate" error.
  virtual void fail(const char *reason);
  /// Increase the retry count and throw an exception when retry_limit
  /// is exceeded.
  void retry();
};

inline std::ostream& operator<<(std::ostream& s, prod& p)
{
  p.out(s); return s;
}

#endif
