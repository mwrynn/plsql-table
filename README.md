# plsql-table
The PL/SQL Table Object - An object type that when instantiated is linked to a table; the object has many functions that perform convenient operations, such as disabling/enabling all indexes, generation of random dummy data (random garbage as well as randomly select parent keys for foreign key columns), dynamic query generation - especially useful to handle burdensome column lists.

For further explanations and examples please check my blog at (may be out of date; TODO: examples here):

https://mwrynn.blogspot.com/2015/03/the-mwrynn-plsql-table-api.html

https://mwrynn.blogspot.com/2018/02/the-mwrynn-plsql-table-api-diff-util.html


# How to create table_obj type:
To create the object type TABLE_OBJ and its dependencies, you only need to run the script create_all.sql from a client such as sqlplus, sqlcl or SQL Developer. This script will in turn run the scripts create_table_obj_helpers.sql, create_table_obj.sql and create_table_obj_body.sql in that order.

# How to run unit tests:
1. Install utPLSQL: https://www.utplsql.org/utPLSQL/v3.1.12/userguide/install.html#
2. Run script test/create_all.sql
3. Run the unit tests: test/run_all_unit_tests.sql

# Tips on installing utPLSQL on Amazon RDS:
    This can be tricky to get right, as you can't log into RDS as SYSDBA. But the following script should create user `ut3` and set it up with the correct privileges granted:

```
create role UT_REPO_ADMINISTRATOR;
create role UT_REPO_USER;
grant create public synonym,drop public synonym to UT_REPO_ADMINISTRATOR;
exec rdsadmin.rdsadmin_util.grant_sys_object('DBA_ROLE_PRIVS', 'UT_REPO_ADMINISTRATOR');
exec rdsadmin.rdsadmin_util.grant_sys_object('DBA_ROLE_PRIVS', 'UT_REPO_USER');
exec rdsadmin.rdsadmin_util.grant_sys_object('DBA_ROLES', 'UT_REPO_ADMINISTRATOR');
exec rdsadmin.rdsadmin_util.grant_sys_object('DBA_ROLES', 'UT_REPO_USER');
exec rdsadmin.rdsadmin_util.grant_sys_object('DBA_TAB_PRIVS', 'UT_REPO_ADMINISTRATOR');
exec rdsadmin.rdsadmin_util.grant_sys_object('DBA_TAB_PRIVS', 'UT_REPO_USER');
exec rdsadmin.rdsadmin_util.grant_sys_object('DBMS_LOCK', 'UT_REPO_ADMINISTRATOR');
exec rdsadmin.rdsadmin_util.grant_sys_object('DBMS_LOCK', 'UT_REPO_USER');
grant UT_REPO_USER to UT_REPO_ADMINISTRATOR with admin option;
create user UT3;
exec rdsadmin.rdsadmin_util.grant_sys_object('DBMS_CRYPTO', 'UT3');
grant UT_REPO_ADMINISTRATOR to UT3 with admin option;
grant unlimited tablespace to UT3;
```
Once this is done, run install.sql as instructed in the utPLSQL documentation. This command worked for me (using sqlcl):
```
sql admin/**********@blah.blahblah.us-east-1.rds.amazonaws.com/ORCL @install.sql ut3
```