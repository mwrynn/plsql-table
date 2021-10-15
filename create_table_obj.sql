EXEC drop_type_if_not_exists(type_name => 'table_obj', force => true);
/
CREATE TYPE table_obj AS OBJECT (
    table_name VARCHAR2(255),
    schema_name VARCHAR2(255),
    dblink VARCHAR2(255),

/* TODO: group functions into groups: gen for sql generation functions, maybe "sub" group for cols vs. array cols,
util for enable/disable indexes, random rows etc. How best to group? could use more object types? So something like my_table_obj.gen.arr.all_cols? 
can these be done in a static manner? yes static methods exist if not the type
*/

    MEMBER FUNCTION qual_table_name RETURN VARCHAR2, --returns fully qualified table name, including "@dblink" if applicable 
    
    MEMBER FUNCTION upper_table_name RETURN VARCHAR2, --returns uppercase table name, not fully qualified (just the table name)
    
    MEMBER FUNCTION upper_schema_name RETURN VARCHAR2, --returns uppercase schema name
    
    /* returns comma-delimited string of all columns in the table, each prefixed with alias || '.' if alias specified, excluding any columns specified in exclude_list */
    MEMBER FUNCTION all_cols(alias IN VARCHAR2 DEFAULT NULL, exclude_list IN COLS_ARR DEFAULT NULL) RETURN VARCHAR2, 
    
    /* returns comma-delimited string of primary key columns in the table, each prefixed with alias || '.' if alias specified, excluding any columns specified in exclude_list */
    MEMBER FUNCTION pk_cols(alias IN VARCHAR2 DEFAULT NULL, exclude_list IN COLS_ARR DEFAULT NULL) RETURN VARCHAR2,
    
    /* returns comma-delimited string of all non-primary key columns in the table, each prefixed with alias || '.' if alias specified, excluding any columns specified in exclude_list */
    MEMBER FUNCTION non_pk_cols(alias IN VARCHAR2 DEFAULT NULL, exclude_list IN COLS_ARR DEFAULT NULL) RETURN VARCHAR2,
    
    /* returns COLS_ARR of all columns in the table, each prefixed with alias || '.' if alias specified, excluding any columns specified in exclude_list */
    MEMBER FUNCTION all_cols_arr(alias IN VARCHAR2 DEFAULT NULL, exclude_list IN COLS_ARR DEFAULT NULL) RETURN COLS_ARR,
    
    /* returns COL_TYPES_ARR of all columns in the table, excluding any columns specified in exclude_list */
    MEMBER FUNCTION all_col_types_arr(exclude_list IN COLS_ARR DEFAULT NULL) RETURN COL_TYPES_ARR,
    
    /* returns COLS_ARR of primary key columns in the table, each prefixed with alias || '.' if alias specified, excluding any columns specified in exclude_list */
    MEMBER FUNCTION pk_cols_arr(alias IN VARCHAR2 DEFAULT NULL, exclude_list IN COLS_ARR DEFAULT NULL) RETURN COLS_ARR,
    
    /* returns COLS_ARR of all non-primary key columns in the table, each prefixed with alias || '.' if alias specified, excluding any columns specified in exclude_list */
    MEMBER FUNCTION non_pk_cols_arr(alias IN VARCHAR2 DEFAULT NULL, exclude_list IN COLS_ARR DEFAULT NULL) RETURN COLS_ARR,
    
    /* enables all non-unique indexes on the table (uniques are needed to enforce unique/primary keys, TODO: handle PK/Unique optionally) */
    MEMBER FUNCTION enable_indexes RETURN INT,
    
    /* disables all non-unique indexes on the table (uniques are needed to enforce unique/primary keys, TODO: handle PK/Unique optionally) */
    MEMBER FUNCTION disable_indexes RETURN INT,
    
    /*
       given other_table (name of another table in the same schema), returns string of join columns between the two tables. Joins on primary key columns having the same name only. self_alias and other_alias are optional aliases for this table and other_table, respectively
       HEADS-UP: this depends on MATCHING column names, e.g. both department and employee tables would have to have employee_id to join the two, so the convention of employee having just plain "id" is not supported
       perhaps a TODO: support other matching methods, perhaps by looking at foreign keys
    */
    MEMBER FUNCTION join_list(other_table IN table_obj, self_alias IN VARCHAR2 DEFAULT 'a', other_alias IN VARCHAR2 DEFAULT 'b') RETURN VARCHAR2,
    
    /*
       given this table and other_table, return string representing setting each column in self table to each column in other_table, matched by names, PK cols excluded
       aliases must be specified, or else self is assumed to be 'a' and other_table's alias 'b'
       This is useful for generating merge or update statements, setting a.mycol=b.mycol etc.
    */
    MEMBER FUNCTION matched_update_list(other_table IN table_obj, self_alias IN VARCHAR2 DEFAULT 'a', other_alias IN VARCHAR2 DEFAULT 'b') RETURN VARCHAR2,
    
    /* converts a COLS_ARR to a comma-delimited string */
    MEMBER FUNCTION cols_arr_to_commalist(p_cols_arr IN COLS_ARR, quoted IN BOOLEAN DEFAULT false) RETURN VARCHAR2,
    
    /* generates an ALTER TABLE statement to add a column to table */
    MEMBER FUNCTION gen_add_col_ddl(name IN VARCHAR2, type IN VARCHAR2, constraint IN VARCHAR2 DEFAULT NULL) RETURN VARCHAR2,
    
    /* adds a column to self table */
    MEMBER FUNCTION add_col(name IN VARCHAR2, type IN VARCHAR2, constraint IN VARCHAR2 DEFAULT NULL, ignore_if_exists IN BOOLEAN DEFAULT true) RETURN INT,
    
    /*
       generates "CREATE TABLE" DDL stsatement given column names, types, constraints and keys passed in. extra_stuff is appended, which is useful for special features
       note this is probably better for creating new tables. For existing tables you can call DBMS_METADATA.GET_DDL which is very complete
    */
    MEMBER FUNCTION gen_create_ddl(cols_name_arr IN cols_arr, cols_type_arr IN varchar2_arr, cols_constraints IN varchar2_arr, keys IN varchar2_arr, extra_stuff IN VARCHAR2 DEFAULT NULL) RETURN VARCHAR2,
    
    /* generates an INSERT statement that inserts n rows of random data. min_date and max_date define the range of dates and timestamps allowed. Foreign key cols are generated from selecting a random value from the parent table */
    MEMBER FUNCTION gen_insert_random_rows_stmt(n IN INT, min_date IN DATE DEFAULT NULL, max_date IN DATE DEFAULT NULL) RETURN LONG,
    
    /* inserts n rows of random data. min_date and max_date define the range of dates and timestamps allowed. Foreign key cols are generated from selecting a random value from the parent table */
    MEMBER FUNCTION insert_random_rows(n IN INT, min_date IN DATE, max_date IN DATE) RETURN INT,
    
    /* returns true if column name specified is part of a foreign key, false otherwise */
    MEMBER FUNCTION col_is_fk(col_name IN VARCHAR2) RETURN BOOLEAN,
    
    /* returns a random referenced value in a parent table, given a foreign key column name - NOTE: only works on NUMBER and VARCHAR2 types at this time, does not work on composite foreign keys at this time, NUMBERs are encoded as VARCHAR2 */
    MEMBER FUNCTION rand_ref_val(fk_col_name IN VARCHAR2, data_type OUT VARCHAR2) RETURN VARCHAR2,
    
    /* returns a COLS_ARR of columns in this table that intersect with columns in other_cols, parameters nulls and nulls_exclude_list are used to exclude specified column names with the string 'NULL' */
    MEMBER FUNCTION intersecting_cols_arr(other_cols IN COLS_ARR, nulls_exclude_list IN COLS_ARR DEFAULT NULL, nulls IN BOOLEAN DEFAULT false) RETURN COLS_ARR,
    
    /* returns a comma-delimited string of columns in this table that intersect with columns in other_cols, parameters nulls and nulls_exclude_list are used to exclude specified column names with the string 'NULL' */
    MEMBER FUNCTION intersecting_cols(other_cols IN COLS_ARR, nulls_exclude_list IN COLS_ARR DEFAULT NULL, nulls IN BOOLEAN DEFAULT false) RETURN VARCHAR2,
    
    /* returns true if table indicated by table_name, schema_name and dblink exists, false otherwise */
    MEMBER FUNCTION table_exists RETURN BOOLEAN,
    
    /* drop the table - mostly handy to avoid repeatedly writing manual checks for whether the table exists before issuing DROP TABLE */
    MEMBER PROCEDURE drop_table(ignore_if_not_exists IN BOOLEAN DEFAULT true),

    /* diffs two tables either at a schema level or data level, as determined by the various parameters. use_diff_results_table indicates
       whether the detailed results should be put in a table. Whether this is used or not, the function returns 1 if the tables differ
       according to the specified parameters, 0 if not
    */
    MEMBER FUNCTION diff(other_table IN TABLE_OBJ, 
                         use_diff_results_table IN BOOLEAN DEFAULT true,
                         compare_columns IN BOOLEAN DEFAULT true,
                         compare_constraints IN BOOLEAN DEFAULT true,
                         compare_indexes IN BOOLEAN DEFAULT true,
                         compare_data IN BOOLEAN default false) RETURN INT,

    MEMBER FUNCTION diff_indexes(other_table IN TABLE_OBJ, use_diff_results_table IN BOOLEAN DEFAULT true, diff_results_table IN TABLE_OBJ) RETURN INT,
    
    MEMBER FUNCTION diff_columns(other_table IN TABLE_OBJ, use_diff_results_table IN BOOLEAN DEFAULT true, diff_results_table IN TABLE_OBJ) RETURN INT,

    MEMBER FUNCTION gen_diff_result_str(table1_name IN VARCHAR2, table2_name IN VARCHAR2, type1 IN COL_TYPE, type2 IN COL_TYPE) RETURN VARCHAR2,

    MEMBER FUNCTION create_diff_results_table(diff_results_table IN table_obj) RETURN BOOLEAN
  ) NOT FINAL;
/