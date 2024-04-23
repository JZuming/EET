## Reported Bugs in MySQL
1. Incorrect query results caused by subqueries and JOIN
    - Link: https://bugs.mysql.com/bug.php?id=110156
    - Status: Confirmed
2. SEGV (Item_ref::update_used_tables() at sql/item.h:5800)
    - Email to Oracle Security Alerts: Confidential
    - Status: Fixed
    - [CVE-2023-22112](https://nvd.nist.gov/vuln/detail/CVE-2023-22112)
3. SEGV (Item_ref::walk() at sql/item.h:5836)
    - Email to Oracle Security Alerts: Confidential
    - Status: Fixed
4. SEGV (Item_subselect::exec() at sql/item_subselect.cc:660)
    - Email to Oracle Security Alerts: Confidential
    - Status: Fixed
    - [CVE-2024-21008](https://nvd.nist.gov/vuln/detail/CVE-2024-21008)
5. Inconsistent results caused by SPACE() function and ''||'' operations
    - Link: https://bugs.mysql.com/bug.php?id=112149
    - Status: Confirmed
6. Another SEGV (Item_subselect::exec() at sql/item_subselect.cc:660)
    - Email to Oracle Security Alerts: Confidential
    - Status: Fixed
    - [CVE-2024-21009](https://nvd.nist.gov/vuln/detail/CVE-2024-21009)
7. SEGV (Item_ref::real_item() at sql/item.h:5825)
    - Email to Oracle Security Alerts: Confidential
    - Status: Fixed
    - [CVE-2024-20982](https://nvd.nist.gov/vuln/detail/CVE-2024-20982)
8. SEGV (Item_subselect::print() at sql/item_subselect.cc:835)
    - Email to Oracle Security Alerts: Confidential
    - Status: Fixed
    - [CVE-2024-21013](https://nvd.nist.gov/vuln/detail/CVE-2024-21013)
9. Inconsistent results caused by FIELD()
    - Link: https://bugs.mysql.com/bug.php?id=112238
    - Status: Confirmed
10. Inconsistent results caused by subqueries in FROM and EXISTS
    - Link: https://bugs.mysql.com/bug.php?id=112394
    - Status: Confirmed
11. Inconsistent results of SELECT statement with window functions
    - Link: https://bugs.mysql.com/bug.php?id=112460
    - Status: Fixed
12. Inconsistent results of UNIX_TIMESTAMP with NULLIF and COALESCE
    - Link: https://bugs.mysql.com/bug.php?id=112524
    - Status: Confirmed
13. Inconsistent results caused by REPEAT function
    - Link: https://bugs.mysql.com/bug.php?id=112527
    - Status: Confirmed
14. Inconsistent results of SELECT statement with EXISTS clause
    - Link: https://bugs.mysql.com/bug.php?id=112557
    - Status: Confirmed
15. Inconsistent results of SELECT statement with HEX, REVERSE, and RPAD functions
    - Link: https://bugs.mysql.com/bug.php?id=112572
    - Status: Confirmed
16. Inconsistent results of SELECT with <=> and NULLIF
    - Link: https://bugs.mysql.com/bug.php?id=112579
    - Status: Confirmed