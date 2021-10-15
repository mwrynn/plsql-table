# plsql-table
The mwrynn PL/SQL Table API - Provides a "table_obj" that is linked to a table; the object has many functions that perform convenient operations, such as disabling/enabling all indexes, generation of random dummy data (random garbage as well as randomly select parent keys for foreign key columns), dynamic query generation - especially useful to handle burdensome column lists.

For further explanations and examples please check my blog at:

https://mwrynn.blogspot.com/2015/03/the-mwrynn-plsql-table-api.html

https://mwrynn.blogspot.com/2018/02/the-mwrynn-plsql-table-api-diff-util.html


# How to create:
To create the object type TABLE_OBJ and its dependencies, you only need to run the script create_all.sql from a client such as sqlplus, sqlcl or SQL Developer. This script will in turn run the scripts create_table_obj_helpers.sql, create_table_obj.sql and create_table_obj_body.sql in that order.