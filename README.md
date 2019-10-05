# plsql-table
The mwrynn PL/SQL Table API - Provides a "table_obj" that is linked to a table; the object has many functions that perform convenient operations, such as disabling/enabling all indexes, generation of random dummy data (random garbage as well as randomly select parent keys for foreign key columns), dynamic query generation - especially useful to handle burdensome column lists.

For further explanations and examples please check my blog at:

https://mwrynn.blogspot.com/2015/03/the-mwrynn-plsql-table-api.html

https://mwrynn.blogspot.com/2018/02/the-mwrynn-plsql-table-api-diff-util.html


# How to create:
First run the script: create_table_obj_helpers.sql

Next, run the script: create_table_obj.sql

Finally, run script:  create_table_obj_body.sql
