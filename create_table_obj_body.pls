CREATE OR REPLACE TYPE BODY table_obj AS
  CONSTRUCTOR FUNCTION table_obj(
    table_name VARCHAR2,
    schema_name VARCHAR2,
    dblink VARCHAR2 DEFAULT NULL,
    existence_check BOOLEAN DEFAULT false
  ) RETURN SELF AS RESULT
  AS
    l_cnt INT;
    l_qry VARCHAR2(500);
  BEGIN
    SELF.table_name := table_name;
    SELF.schema_name := schema_name;
    SELF.dblink := dblink;
    
    IF existence_check = false THEN
      RETURN;
    END IF;
    
    IF dblink IS NULL THEN
      SELECT COUNT(*) INTO l_cnt
      FROM all_tables
      WHERE owner=schema_name AND table_name=SELF.table_name;
    ELSE
      l_qry := 'SELECT COUNT(*)' || chr(10) ||
      'FROM all_tables@:1' || chr(10) ||
      'WHERE owner=:2 AND table_name=:3';
      
      EXECUTE IMMEDIATE l_qry INTO l_cnt USING SELF.dblink, SELF.schema_name, SELF.table_name;
    END IF;
    
    IF l_cnt = 0 THEN
      raise_application_error( -20006, 'table must exist in order to create table_obj instance');
    END IF;
    
    RETURN;
  END;

  ---
      
  MEMBER FUNCTION qual_table_name RETURN VARCHAR2 IS
  BEGIN
    IF dblink IS NOT NULL THEN
      RETURN self.upper_table_name() || '@' || dblink;
    ELSE
      RETURN schema_name || '.' || self.upper_table_name;
    END IF;
  END qual_table_name;

  ---

  MEMBER FUNCTION upper_table_name RETURN VARCHAR2 IS
  BEGIN
    RETURN upper(table_name);
  END upper_table_name;

  ---

  MEMBER FUNCTION upper_schema_name RETURN VARCHAR2 IS
  BEGIN
    RETURN upper(schema_name);
  END upper_schema_name;

  ---

  MEMBER FUNCTION all_cols(alias IN VARCHAR2 DEFAULT NULL, exclude_list IN COLS_ARR DEFAULT NULL) RETURN VARCHAR2 IS
    col_list VARCHAR2(32767) := ''; 
    l_cols_arr COLS_ARR;
  BEGIN
    l_cols_arr := all_cols_arr(alias, exclude_list);
    
    FOR i IN 1 .. l_cols_arr.count LOOP
      col_list := col_list || l_cols_arr(i) || CASE WHEN i < l_cols_arr.count THEN ',' END;
    END LOOP;

    RETURN col_list;
  END all_cols;

  ---

  MEMBER FUNCTION pk_cols(alias IN VARCHAR2 DEFAULT NULL, exclude_list IN COLS_ARR DEFAULT NULL) RETURN VARCHAR2 IS
    col_list VARCHAR2(32767);
    l_cols_arr COLS_ARR;
  BEGIN
    l_cols_arr := pk_cols_arr(alias, exclude_list);

    FOR i IN 1 .. l_cols_arr.count LOOP
      col_list := col_list || l_cols_arr(i) || CASE WHEN i < l_cols_arr.count THEN ',' END;
    END LOOP;

    RETURN col_list;
  END pk_cols;

  ---

  MEMBER FUNCTION non_pk_cols(alias IN VARCHAR2 DEFAULT NULL, exclude_list IN COLS_ARR DEFAULT NULL) RETURN VARCHAR2 IS
    col_list VARCHAR2(32767);
    l_cols_arr COLS_ARR;
  BEGIN

    l_cols_arr := non_pk_cols_arr(alias, exclude_list);

    FOR i IN 1 .. l_cols_arr.count LOOP
      col_list := col_list || l_cols_arr(i) || CASE WHEN i < l_cols_arr.count THEN ',' END;
    END LOOP;

    RETURN col_list;
  END non_pk_cols;

  ---

  MEMBER FUNCTION pk_cols_arr(alias IN VARCHAR2 DEFAULT NULL, exclude_list IN COLS_ARR DEFAULT NULL) RETURN COLS_ARR IS
    ret_cols_arr cols_arr;
    qry VARCHAR2(32767);
  BEGIN

    qry := 'SELECT ' || CASE WHEN alias IS NOT NULL THEN '''' || alias || '.'' || ' END || 'column_name ' ||
     'FROM ' || CASE WHEN dblink IS NOT NULL THEN 'user_constraints@' || dblink || ' cons'
         ELSE 'all_constraints cons' END || ', ' ||
    CASE WHEN dblink IS NOT NULL THEN 'user_cons_columns@' || dblink || ' cols'
         ELSE 'all_cons_columns cols' END || ' ' ||
     'WHERE cols.table_name=:1' || CASE WHEN dblink IS NULL AND schema_name IS NOT NULL THEN ' AND cols.owner=:2 ' END ||
     'AND cons.constraint_type=''P'' ' ||
     'AND cons.constraint_name = cols.constraint_name ' ||
     'AND cons.owner = cols.owner ' ||
           CASE WHEN exclude_list IS NOT NULL THEN 'AND column_name NOT IN (' || upper(self.cols_arr_to_commalist(exclude_list, true)) || ') ' END || 
     'ORDER BY cols.position';

    IF dblink IS NULL AND schema_name IS NOT NULL THEN
      EXECUTE IMMEDIATE qry BULK COLLECT INTO ret_cols_arr USING self.upper_table_name(), self.upper_schema_name();
    ELSE
      EXECUTE IMMEDIATE qry BULK COLLECT INTO ret_cols_arr USING self.upper_table_name();
    END IF;

    RETURN ret_cols_arr;
  END pk_cols_arr;

  ---

  MEMBER FUNCTION all_cols_arr(alias IN VARCHAR2 DEFAULT NULL, exclude_list IN COLS_ARR DEFAULT NULL) RETURN COLS_ARR IS
    ret_cols_arr cols_arr;
    qry VARCHAR2(32767);
  BEGIN
    qry := 'SELECT ' || CASE WHEN alias IS NOT NULL THEN '''' || alias || '.'' || ' END || 'column_name ' ||
     'FROM ' || CASE WHEN dblink IS NOT NULL THEN 'user_tab_columns@' || dblink ELSE 'all_tab_columns' END || ' ' ||
     'WHERE table_name=:1' || CASE WHEN dblink IS NULL AND schema_name IS NOT NULL THEN ' AND owner=:2' END || ' ' ||
           CASE WHEN exclude_list IS NOT NULL AND exclude_list.count>0  THEN 'AND column_name NOT IN ( ' || upper(self.cols_arr_to_commalist(exclude_list, true)) || ') ' END ||
     'ORDER BY column_id';

    IF dblink IS NULL AND schema_name IS NOT NULL then
      EXECUTE IMMEDIATE qry BULK COLLECT INTO ret_cols_arr USING self.upper_table_name(), self.upper_schema_name();
    ELSE
      EXECUTE IMMEDIATE qry BULK COLLECT INTO ret_cols_arr USING self.upper_table_name();
    END IF;

    RETURN ret_cols_arr;
  END all_cols_arr;

  ---

  MEMBER FUNCTION all_col_types_arr(exclude_list IN COLS_ARR DEFAULT NULL) RETURN COL_TYPES_ARR IS
    ret_col_types_arr COL_TYPES_ARR := COL_TYPES_ARR();
    qry VARCHAR2(32767);
  BEGIN
    qry := 'SELECT COL_TYPE(column_name, data_type, data_type_mod, data_type_owner, data_length, data_precision, data_scale, column_id, default_length) /*, data_default*/ ' ||
    'FROM ' || CASE WHEN dblink IS NOT NULL THEN 'user_tab_columns@' || dblink ELSE 'all_tab_columns' END || ' ' ||
    'WHERE table_name=:1' || CASE WHEN dblink IS NULL AND schema_name IS NOT NULL THEN ' AND owner=:2' END || ' ' ||
    CASE WHEN exclude_list IS NOT NULL AND exclude_list.count>0  THEN 'AND column_name NOT IN ( ' || upper(self.cols_arr_to_commalist(exclude_list, true)) || ') ' END ||
    'ORDER BY column_id';

    IF dblink IS NULL AND schema_name IS NOT NULL then
      EXECUTE IMMEDIATE qry BULK COLLECT INTO ret_col_types_arr USING self.upper_table_name(), self.upper_schema_name();
    ELSE
      EXECUTE IMMEDIATE qry BULK COLLECT INTO ret_col_types_arr USING self.upper_table_name();
    END IF;

    RETURN ret_col_types_arr;
  END all_col_types_arr;
  ---

  MEMBER FUNCTION non_pk_cols_arr(alias IN VARCHAR2 DEFAULT NULL, exclude_list IN COLS_ARR DEFAULT NULL) RETURN COLS_ARR IS
    ret_cols_arr cols_arr;
    qry VARCHAR2(32767);
    l_exclude_list cols_arr;
  BEGIN
    IF exclude_list IS NULL THEN 
      l_exclude_list := pk_cols_arr();
    ELSE
      l_exclude_list := pk_cols_arr() MULTISET UNION exclude_list;
    END IF;

    ret_cols_arr := all_cols_arr(alias, l_exclude_list); 
    RETURN ret_cols_arr;
  END non_pk_cols_arr;

  ---

  MEMBER FUNCTION enable_indexes RETURN INT IS
    rebuild_str LONG;
    index_count INT := 0;
  BEGIN
    FOR rec IN (SELECT * FROM all_indexes WHERE owner=self.upper_schema_name() AND table_name=self.upper_table_name() AND uniqueness='NONUNIQUE') LOOP
      IF rec.index_type IN ('NORMAL', 'NORMAL/REV', 'FUNCTION-BASED DOMAIN', 'FUNCTION-BASED NORMAL') THEN
        rebuild_str := 'ALTER INDEX ' || self.upper_schema_name() || '.' || rec.index_name || ' REBUILD';
      END IF;
      EXECUTE IMMEDIATE rebuild_str;
      index_count := index_count + 1;
    END LOOP;
    RETURN index_count;
  END enable_indexes;

  ---

  MEMBER FUNCTION disable_indexes RETURN INT IS
    disable_str LONG;
    index_count INT := 0;
  BEGIN
    FOR rec IN (SELECT * FROM all_indexes WHERE owner=self.upper_schema_name() AND table_name=self.upper_table_name() AND uniqueness='NONUNIQUE') LOOP
      IF rec.index_type IN ('NORMAL', 'NORMAL/REV', 'FUNCTION-BASED DOMAIN', 'FUNCTION-BASED NORMAL') THEN
        disable_str := 'ALTER INDEX ' || self.upper_schema_name() || '.' || rec.index_name || ' UNUSABLE';
      END IF; 
      EXECUTE IMMEDIATE disable_str;
      index_count := index_count + 1;
    END LOOP;
    RETURN index_count;
  END disable_indexes;

  ---

  --returns join conditions for joining this table with other_table on intersecting primary keys only
  MEMBER FUNCTION join_list(other_table IN table_obj, self_alias IN VARCHAR2 DEFAULT 'a', other_alias IN VARCHAR2 DEFAULT 'b') RETURN VARCHAR2 IS
    other_pk_cols cols_arr;
    self_pk_cols cols_arr;
    join_str VARCHAR2(32767) := '';
  BEGIN
    other_pk_cols := other_table.pk_cols_arr();
    self_pk_cols  := self.pk_cols_arr();

    FOR i IN 1 .. self_pk_cols.count LOOP
      FOR j IN 1 .. other_pk_cols.count LOOP
        IF self_pk_cols(i)=other_pk_cols(j) THEN
          join_str := join_str || self_alias || '.' || self_pk_cols(i) || '=' || other_alias || '.' || other_pk_cols(j) || ' AND ';
        END IF;
      END LOOP;
    END LOOP;

    --remove trailing ' AND '
    join_str := substr(join_str, 1, instr(join_str, ' AND ', -1)-1);

    return join_str;
  END join_list;

  ---
  /* given this table and other_table, return string representing setting each column in self table to each column in other_table, matched by names, PK cols excluded 
     aliases must be specified, or else self is assumed to be 'a' and other_table's alias 'b'
     This is useful for generating merge or update statements, setting a.mycol=b.mycol etc.
  */
  MEMBER FUNCTION matched_update_list(other_table IN table_obj, self_alias IN VARCHAR2 DEFAULT 'a',other_alias IN VARCHAR2 DEFAULT 'b') RETURN VARCHAR2 IS
    ret_update_list VARCHAR2(32767) := ''; 
    self_non_pk_cols COLS_ARR;
    other_non_pk_cols COLS_ARR;
  BEGIN
    --get all non-PK cols in self that match all non-PK cols in other, map them
    self_non_pk_cols := self.non_pk_cols_arr();
    other_non_pk_cols := other_table.non_pk_cols_arr();

    FOR i IN 1 .. self_non_pk_cols.count LOOP
      FOR j IN 1 .. other_non_pk_cols.count LOOP
        IF self_non_pk_cols(i) = other_non_pk_cols(j) THEN
          ret_update_list := ret_update_list || self_alias || '.' || self_non_pk_cols(i) || '=' ||
            other_alias || '.' || other_non_pk_cols(j) || ',';
        END IF;
      END LOOP;
    END LOOP;

    ret_update_list := substr(ret_update_list, 1, length(ret_update_list)-1); --trim trailing comma
    RETURN ret_update_list;
  END matched_update_list;

  ---

  MEMBER FUNCTION cols_arr_to_commalist(p_cols_arr IN COLS_ARR, quoted IN BOOLEAN DEFAULT false) RETURN VARCHAR2 IS
    ret_list VARCHAR2(32767) := '';
  BEGIN
    FOR i IN 1 .. p_cols_arr.count LOOP
      ret_list := ret_list || CASE WHEN quoted THEN '''' END || p_cols_arr(i) || CASE WHEN quoted THEN '''' END || CASE WHEN i < p_cols_arr.count THEN ',' END;
    END LOOP;

    RETURN ret_list;
  END cols_arr_to_commalist;

  ---


  MEMBER FUNCTION gen_add_col_ddl(name IN VARCHAR2, type IN VARCHAR2, constraint IN VARCHAR2 DEFAULT NULL) RETURN VARCHAR2 IS
    col_already_exists EXCEPTION;
    PRAGMA exception_init(col_already_exists, -1430);
    add_col_str VARCHAR2(32767);
  BEGIN
    add_col_str := 'ALTER TABLE ' || self.qual_table_name() || ' ADD ( ' || name || ' ' || type || CASE WHEN constraint IS NOT NULL THEN ' ' || constraint END || ')';
    RETURN add_col_str;
  END gen_add_col_ddl;

  ---

  MEMBER FUNCTION add_col(name IN VARCHAR2, type IN VARCHAR2, constraint IN VARCHAR2 DEFAULT NULL, ignore_if_exists IN BOOLEAN DEFAULT true) RETURN INT IS
    col_already_exists EXCEPTION;
    PRAGMA exception_init(col_already_exists, -1430);
    add_col_str VARCHAR2(32767);
  BEGIN
    add_col_str := gen_add_col_ddl(name, type, constraint);

    EXECUTE IMMEDIATE add_col_str;
    RETURN 0;

    EXCEPTION WHEN col_already_exists THEN
      IF ignore_if_exists THEN
        NULL;
      ELSE
        --reraise the error
        RAISE col_already_exists;
      END IF;
    RETURN 0;
  END add_col;

  ---

  MEMBER FUNCTION gen_create_ddl(cols_name_arr IN cols_arr, cols_type_arr IN varchar2_arr, cols_constraints IN varchar2_arr, keys IN varchar2_arr, extra_stuff IN VARCHAR2) RETURN VARCHAR2 IS
    ddl_str LONG;
  BEGIN
    ddl_str := 'CREATE TABLE ' || qual_table_name || '(' || chr(10);
    FOR i IN 1 .. cols_name_arr.count LOOP 
      ddl_str := ddl_str || cols_name_arr(i) || ' ' || cols_type_arr(i) || CASE WHEN cols_constraints(i) IS NOT NULL THEN ' ' || cols_constraints(i) END ||
                 CASE WHEN keys IS NULL AND i=cols_name_arr.count THEN '' ELSE ',' END || chr(10);
    END LOOP;

    FOR i IN 1 .. keys.count LOOP
      ddl_str := ddl_str || keys(i) || CASE WHEN i=keys.count THEN '' ELSE ',' END || chr(10);
    END LOOP;
             
    ddl_str := ddl_str || ')' || chr(10);   

    ddl_str := ddl_str || extra_stuff;
    RETURN ddl_str;
  END gen_create_ddl;

  ---

  MEMBER FUNCTION gen_insert_random_rows_stmt(n IN INT, min_date IN DATE DEFAULT NULL, max_date IN DATE DEFAULT NULL) RETURN LONG IS
    qry LONG;
    rand_fk_ref VARCHAR2(32767);
    data_type VARCHAR2(32767);
    rand_val VARCHAR2(32767);

    TYPE col_metadata_arr_type IS TABLE OF all_tab_cols%ROWTYPE INDEX BY PLS_INTEGER;

    col_metadata_arr col_metadata_arr_type;
  BEGIN
    IF self.dblink IS NOT NULL THEN 
      raise_application_error( -20001, 'dblink not supported by insert_random_rows function at this time');
    END IF;

    qry := 'INSERT INTO ' || self.qual_table_name() || '(' || self.all_cols() || ')' || chr(10);

    /* collect the column metadata to loop over */
    SELECT *
    BULK COLLECT INTO col_metadata_arr
    FROM all_tab_cols
    WHERE table_name=self.upper_table_name()
      AND owner=self.upper_schema_name()
    ORDER by column_id;
    
    /* loop n times, where n is number of rows to generate */
    FOR i in 1 .. n LOOP
      qry := qry || 'SELECT ';

      /* loop over each column in col_metadata_arr */
      FOR col_i IN 1 .. col_metadata_arr.count LOOP
        IF col_is_fk(col_metadata_arr(col_i).column_name) THEN
          rand_val := self.rand_ref_val(col_metadata_arr(col_i).column_name, data_type);
          IF data_type = 'VARCHAR2' THEN
            qry := qry || '''' || self.rand_ref_val(col_metadata_arr(col_i).column_name, data_type) || '''';
          ELSE
            qry := qry || self.rand_ref_val(col_metadata_arr(col_i).column_name, data_type);
          END IF;
        ELSIF col_metadata_arr(col_i).data_type='NUMBER' THEN
          qry := qry || to_char(trunc(power(10, CASE WHEN col_metadata_arr(col_i).data_precision IS NULL THEN 10 ELSE col_metadata_arr(col_i).data_precision-col_metadata_arr(col_i).data_scale END)*dbms_random.value, col_metadata_arr(col_i).data_scale));
        ELSIF col_metadata_arr(col_i).data_type IN ('VARCHAR2', 'CLOB') THEN
          qry := qry || '''' || dbms_random.string('A', col_metadata_arr(col_i).data_length) || ''''; --A means mixed case letters
        ELSIF col_metadata_arr(col_i).data_type='DATE' THEN
          IF min_date IS NULL OR max_date IS NULL THEN
            raise_application_error( -20005, 'to generate random dates or timestamps, min_date and max_date must be specified');
          END IF;
          qry := qry || 'to_date(''' || to_char(min_date+trunc(dbms_random.value*(max_date-min_date), 0), 'yyyymmdd hh24:mi:ss') ||
                           ''', ''yyyymmdd hh24:mi:ss'')';
        ELSIF col_metadata_arr(col_i).data_type LIKE 'TIMESTAMP%' THEN
          IF min_date IS NULL OR max_date IS NULL THEN
            raise_application_error( -20005, 'to generate random dates or timestamps, min_date and max_date must be specified');
          END IF;
          qry := qry || 'to_timestamp(''' || to_char(cast(min_date+dbms_random.value*(max_date-min_date) as timestamp), 'yyyymmdd hh24:mi:ss.ff') ||
                           ''', ''yyyymmdd hh24:mi:ss.ff'')';
        ELSE
          raise_application_error( -20002, 'data type not supported at this time: ' || col_metadata_arr(col_i).data_type);
        END IF;
              qry := qry || ',';
      END LOOP; 
  
      qry := substr(qry, 1, length(qry)-1) || ' FROM dual' || chr(10);
  
      IF i < n THEN
        qry := qry || ' UNION ALL' || chr(10);
      END IF;
    END LOOP;      
    
    RETURN qry;
  END gen_insert_random_rows_stmt;

  ---

  MEMBER FUNCTION insert_random_rows(n IN INT, min_date IN DATE, max_date IN DATE) RETURN INT IS
  BEGIN
    EXECUTE IMMEDIATE gen_insert_random_rows_stmt(n, min_date, max_date);
    RETURN 0;
  END insert_random_rows;

  ---

  MEMBER FUNCTION col_is_fk(col_name IN VARCHAR2) RETURN BOOLEAN IS
    cnt INT;
  BEGIN
    FOR rec IN (SELECT 1 FROM DUAL WHERE EXISTS (
      SELECT 1
      FROM all_cons_columns col, all_constraints cons
      WHERE (cons.table_name=col.table_name AND cons.constraint_name=col.constraint_name)
      AND col.owner=self.upper_schema_name()
      AND cons.owner=self.upper_schema_name()
      AND cons.table_name=self.upper_table_name()
      AND column_name=col_name
      AND constraint_type='R'
    )) LOOP

      RETURN true;     
    END LOOP;

    RETURN false;
  END col_is_fk;

  ---

  MEMBER FUNCTION rand_ref_val(fk_col_name IN VARCHAR2, data_type OUT VARCHAR2) RETURN VARCHAR2 IS
    refd_col VARCHAR2(32767);
    refd_table VARCHAR2(32767);
    ret_val VARCHAR2(32767);
    qry LONG;
  BEGIN
    --get referenced column
    SELECT pk_col.column_name, pk_col.table_name, tab_col.data_type INTO refd_col, refd_table, data_type
      FROM all_constraints cons, all_cons_columns col, all_constraints pk_cons, all_cons_columns pk_col, all_tab_columns tab_col
      WHERE
          (cons.owner=col.owner AND cons.table_name=col.table_name AND cons.constraint_name=col.constraint_name)
      AND (cons.r_constraint_name=pk_cons.constraint_name AND cons.r_owner=pk_cons.owner)
      AND (pk_cons.owner=pk_col.owner AND pk_cons.table_name=pk_col.table_name AND pk_cons.constraint_name=pk_col.constraint_name)
      AND (pk_col.owner=tab_col.owner AND pk_col.table_name=tab_col.table_name AND pk_col.column_name=tab_col.column_name)
      AND cons.table_name=self.upper_table_name()
      AND cons.owner=self.upper_schema_name()
      AND col.owner=self.upper_schema_name()
      AND pk_cons.owner=self.upper_schema_name()
      AND pk_col.owner=self.upper_schema_name()
      AND tab_col.owner=self.upper_schema_name()
      AND col.column_name=fk_col_name
      AND cons.constraint_type='R';

    --TODO: will this work simplified to just the to_char qry?
    IF data_type = 'VARCHAR2' THEN
      qry := 'SELECT ' || refd_col || ' FROM (SELECT '  || refd_col || ' FROM ' || refd_table || ' ORDER BY dbms_random.random) WHERE rownum=1';
    ELSIF data_type = 'NUMBER' THEN
      qry := 'SELECT to_char(' || refd_col || ') FROM (SELECT '  || refd_col || ' FROM ' || refd_table || ' ORDER BY dbms_random.random) WHERE rownum=1';
    ELSE
      null;--TODO: handle this case one way or another
    END IF;
    
    EXECUTE IMMEDIATE qry INTO ret_val;
    RETURN ret_val;

  END rand_ref_val;

  ---

  /* gets list of intersecting columns, if nulls is true it will replace the columns in other_cols that are missing with 'null' */

  MEMBER FUNCTION intersecting_cols_arr(other_cols IN COLS_ARR, nulls_exclude_list IN COLS_ARR DEFAULT NULL, nulls IN BOOLEAN DEFAULT false) RETURN COLS_ARR IS
    my_cols COLS_ARR;
    ret_cols COLS_ARR := COLS_ARR();
    match_found BOOLEAN := false;
    exclude_found BOOLEAN := false;
  BEGIN
    my_cols := self.all_cols_arr();

    FOR i IN 1 .. my_cols.count LOOP
      match_found := false;
      exclude_found := false;
      FOR j IN 1 .. other_cols.count LOOP

        IF my_cols(i)=other_cols(j) THEN
          ret_cols.extend;
          ret_cols(ret_cols.count) := my_cols(i);
          match_found := true;
        ELSIF i=j AND NOT match_found AND NULLS THEN --last iteration and no match found and nulls
          IF nulls_exclude_list IS NOT NULL THEN
            FOR k IN 1 .. nulls_exclude_list.count LOOP
              IF my_cols(i)=nulls_exclude_list(k) THEN
                exclude_found := true;
              END IF;
            END LOOP;
          END IF;
          IF NOT exclude_found THEN
            ret_cols.extend;
            ret_cols(ret_cols.count) := 'NULL';
          END IF;
        END IF;          
      END LOOP;
    END LOOP;

    RETURN ret_cols;
  END intersecting_cols_arr;

  ---

  MEMBER FUNCTION intersecting_cols(other_cols IN COLS_ARR, nulls_exclude_list IN COLS_ARR DEFAULT NULL, nulls IN BOOLEAN DEFAULT false) RETURN VARCHAR2 IS 
    l_cols_arr COLS_ARR;
    col_list VARCHAR2(32767) := '';
  BEGIN
    l_cols_arr := intersecting_cols_arr(other_cols, nulls_exclude_list, nulls);

    FOR i IN 1 .. l_cols_arr.count LOOP
      col_list := col_list || l_cols_arr(i) || CASE WHEN i < l_cols_arr.count THEN ',' END;
    END LOOP;

    RETURN col_list;
  END intersecting_cols;

  --

  MEMBER FUNCTION table_exists RETURN BOOLEAN IS      
    cols COLS_ARR;
  BEGIN
    cols := self.all_cols_arr();

    IF cols.count=0 OR cols IS NULL THEN
      RETURN false;
    END IF;

    RETURN true;
  END table_exists;

  ---

  MEMBER PROCEDURE drop_table(ignore_if_not_exists IN BOOLEAN DEFAULT true) IS
  BEGIN
    IF ignore_if_not_exists THEN
      IF self.table_exists THEN
        EXECUTE IMMEDIATE 'DROP TABLE ' || self.qual_table_name;
      END IF;
    ELSE
      EXECUTE IMMEDIATE 'DROP TABLE ' || self.qual_table_name;
    END IF;
  END drop_table;

  --TODO: refactor this
  MEMBER FUNCTION diff(other_table IN TABLE_OBJ,
                       use_diff_results_table IN BOOLEAN DEFAULT true,
                       compare_columns IN BOOLEAN DEFAULT true,
                       compare_constraints IN BOOLEAN DEFAULT true,
                       compare_indexes IN BOOLEAN DEFAULT true,
                       compare_data IN BOOLEAN DEFAULT false) RETURN INT IS
    is_diff BOOLEAN := false;
    is_columns_diff BOOLEAN := false;
    diff_results_table TABLE_OBJ;
    diff_str VARCHAR2(32767) := '';
    dummy BOOLEAN;
    insert_stmt VARCHAR(32767);
    data_diff_stmt VARCHAR(32767);
    self_but_not_other_cnt INT;
    other_but_not_self_cnt INT;
    mismatched_rows EXCEPTION;
    PRAGMA EXCEPTION_INIT(mismatched_rows, -01790);
  BEGIN
    -- first check for existence of both tables. if one does not exist, throw exception
    IF NOT self.table_exists THEN
      raise_application_error( -20003, 'table does not exist: ' || self.qual_table_name);
    END IF;
    
    IF NOT other_table.table_exists THEN
      raise_application_error( -20003, 'table does not exist: ' || other_table.qual_table_name);
    END IF;

    -- handle diff_results creaton - if table exists, don't create, else create
    -- TODO: handle dblink case
    diff_results_table := table_obj('DIFF_RESULTS', self.schema_name, null);
    IF NOT diff_results_table.table_exists THEN
      dummy := create_diff_results_table(diff_results_table);
    END IF;
    -- handle compare_columns
    IF compare_columns THEN
      IF diff_columns(other_table, use_diff_results_table, diff_results_table) = 1 THEN is_diff := true; END IF;
    END IF; 

    -- TODO: handle compare_constraints

    -- handle compare_indexes
    IF compare_indexes THEN
      IF diff_indexes(other_table, use_diff_results_table, diff_results_table) = 1 THEN is_diff := true; END IF;
    END IF;

    -- handle compare_data
    -- special case : if already diffed columns and they are different, don't compare data
    
    IF is_columns_diff THEN
      BEGIN 
        IF compare_data THEN
          data_diff_stmt := 'SELECT COUNT(*) FROM (SELECT * FROM ' || self.qual_table_name || ' MINUS SELECT * FROM ' || other_table.qual_table_name || ')';
          EXECUTE IMMEDIATE data_diff_stmt INTO self_but_not_other_cnt;
        END IF;
      
        IF compare_data THEN
          data_diff_stmt := 'SELECT COUNT(*) FROM (SELECT * FROM ' || other_table.qual_table_name || ' MINUS SELECT * FROM ' || self.qual_table_name || ')';
          EXECUTE IMMEDIATE data_diff_stmt INTO other_but_not_self_cnt;
        END IF;
      EXCEPTION WHEN mismatched_rows THEN
        raise_application_error(-20004, 'Cannot data diff tables ' || self.qual_table_name || ' and ' || other_table.qual_table_name || ' as the columns differ');
      END;
      
      IF self_but_not_other_cnt > 0 THEN
        diff_str := to_char(self_but_not_other_cnt) || ' rows in ' || self.qual_table_name || ' not found in ' || other_table.qual_table_name || ';';
      END IF;
      
      IF other_but_not_self_cnt > 0 THEN
        diff_str := diff_str || to_char(other_but_not_self_cnt) || ' rows in ' || other_table.qual_table_name || ' not found in ' || self.qual_table_name;
      ELSE
        diff_str := rtrim(diff_str, ';');
      END IF;
  
      IF self_but_not_other_cnt > 0 OR other_but_not_self_cnt > 0 THEN 
        --insert into diff_results dynamically because the function to create table diff_results is part of this TYPE BODY code, and this TYPE BODY won't
        --even compile if the INSERT below is static (because table diff_results does not yet exist) - should probably rethink this (TODO)
        insert_stmt := 'INSERT INTO diff_results (table1, table2, diff_type, result) ' ||
                       'VALUES(:table1, :table2, ''D'', :result)';

        EXECUTE IMMEDIATE insert_stmt USING self.qual_table_name, other_table.qual_table_name, diff_str;

        is_diff := true;
      END IF;
    END IF;
    IF is_diff THEN RETURN 1; ELSE RETURN 0; END IF;

  END diff;

  ---
  
  MEMBER FUNCTION diff_indexes(other_table IN TABLE_OBJ, use_diff_results_table IN BOOLEAN DEFAULT true, diff_results_table IN TABLE_OBJ) RETURN INT IS
    idx_diff_stmt VARCHAR(32767);
    idx_self_but_not_other_cnt INT;
    idx_other_but_not_self_cnt INT;
  BEGIN
    idx_diff_stmt := 'SELECT COUNT(*) FROM (' ||
                     'SELECT INDEX_NAME, INDEX_TYPE, UNIQUENESS, COMPRESSION, COLUMN_NAME, COLUMN_POSITION, COLUMN_LENGTH, CHAR_LENGTH, DESCEND ' ||
                     'FROM ALL_INDEXES i JOIN ALL_IND_COLUMNS c ON (i.OWNER = c.INDEX_OWNER AND i.INDEX_NAME = c.INDEX_NAME AND ' ||
                     'i.TABLE_OWNER = c.TABLE_OWNER AND i.TABLE_NAME = c.TABLE_NAME) ' ||
                     'WHERE I.TABLE_NAME = ''' || self.upper_table_name || ''' AND i.TABLE_OWNER = ''' || self.upper_schema_name || ''' ' ||
                     'ORDER BY INDEX_NAME, INDEX_TYPE, UNIQUENESS, COMPRESSION, COLUMN_NAME, COLUMN_POSITION, COLUMN_LENGTH, CHAR_LENGTH, DESCEND ' ||
                     'MINUS ' ||
                     'SELECT INDEX_NAME, INDEX_TYPE, UNIQUENESS, COMPRESSION, COLUMN_NAME, COLUMN_POSITION, COLUMN_LENGTH, CHAR_LENGTH, DESCEND ' ||
                     'FROM ALL_INDEXES i JOIN ALL_IND_COLUMNS c ON (i.OWNER = c.INDEX_OWNER AND i.INDEX_NAME = c.INDEX_NAME AND ' ||
                     'i.TABLE_OWNER = c.TABLE_OWNER AND i.TABLE_NAME = c.TABLE_NAME) ' ||
                     'WHERE I.TABLE_NAME = ''' || other_table.upper_table_name || ''' AND i.TABLE_OWNER = ''' || other_table.upper_schema_name || ''' ' ||
                     ') sub';

    EXECUTE IMMEDIATE idx_diff_stmt INTO idx_self_but_not_other_cnt;

    idx_diff_stmt := 'SELECT COUNT(*) FROM (' ||
                     'SELECT INDEX_NAME, INDEX_TYPE, UNIQUENESS, COMPRESSION, COLUMN_NAME, COLUMN_POSITION, COLUMN_LENGTH, CHAR_LENGTH, DESCEND ' ||
                     'FROM ALL_INDEXES i JOIN ALL_IND_COLUMNS c ON (i.OWNER = c.INDEX_OWNER AND i.INDEX_NAME = c.INDEX_NAME AND ' ||
                     'i.TABLE_OWNER = c.TABLE_OWNER AND i.TABLE_NAME = c.TABLE_NAME) ' ||
                     'WHERE I.TABLE_NAME = ''' || other_table.upper_table_name || ''' AND i.TABLE_OWNER = ''' || other_Table.upper_schema_name || ''' ' ||
                     'ORDER BY INDEX_NAME, INDEX_TYPE, UNIQUENESS, COMPRESSION, COLUMN_NAME, COLUMN_POSITION, COLUMN_LENGTH, CHAR_LENGTH, DESCEND ' ||
                     'MINUS ' ||
                     'SELECT INDEX_NAME, INDEX_TYPE, UNIQUENESS, COMPRESSION, COLUMN_NAME, COLUMN_POSITION, COLUMN_LENGTH, CHAR_LENGTH, DESCEND ' ||
                     'FROM ALL_INDEXES i JOIN ALL_IND_COLUMNS c ON (i.OWNER = c.INDEX_OWNER AND i.INDEX_NAME = c.INDEX_NAME AND ' ||
                     'i.TABLE_OWNER = c.TABLE_OWNER AND i.TABLE_NAME = c.TABLE_NAME) ' ||
                     'WHERE I.TABLE_NAME = ''' || self.upper_table_name || ''' AND i.TABLE_OWNER = ''' || self.upper_schema_name || ''' ' ||
                     ') sub';
    EXECUTE IMMEDIATE idx_diff_stmt INTO idx_other_but_not_self_cnt;

    -- TODO: improve on this by stating the specific differences
    IF idx_self_but_not_other_cnt != 0 OR idx_other_but_not_self_cnt != 0 THEN
      IF use_diff_results_table THEN
        EXECUTE IMMEDIATE 'INSERT INTO ' || diff_results_table.qual_table_name || '(table1, table2, diff_type, result) ' ||
          'VALUES(' || self.qual_table_name || ', ' || other_table.qual_table_name || ', ''I'', ' || 'Index differences found.' || ')';
      ELSE
        dbms_output.put_line('Index differences found.');
      END IF;
    END IF;

  END diff_indexes;

  ---

  MEMBER FUNCTION diff_columns(other_table IN TABLE_OBJ, use_diff_results_table IN BOOLEAN DEFAULT true, diff_results_table IN TABLE_OBJ) RETURN INT IS
    self_col_types_arr COL_TYPES_ARR;
    other_col_types_arr COL_TYPES_ARR;
    diff_str VARCHAR2(32767);
    is_diff BOOLEAN;
  BEGIN
    self_col_types_arr := self.all_col_types_arr();
    other_col_types_arr := other_table.all_col_types_arr();

    IF self_col_types_arr != other_col_types_arr THEN
      is_diff := true;
      -- have to get the differences - for each col in self, compare by name first. if name matches, compare all other fields,
      -- keeping the accumulation of diffs in a string format
      FOR i IN 1 .. self_col_types_arr.count LOOP
        DECLARE
          col_name_found BOOLEAN := false;
          diff_result_str VARCHAR2(32767);
        BEGIN
          FOR j IN 1 .. other_col_types_arr.count LOOP
            IF self_col_types_arr(i).column_name = other_col_types_arr(j).column_name THEN
              col_name_found := true;
              diff_result_str := gen_diff_result_str(self.qual_table_name, other_table.qual_table_name, self_col_types_arr(i), other_col_types_arr(j));
              diff_str := diff_str || diff_result_str || CASE WHEN diff_result_str IS NOT NULL THEN ';' END;
            END IF;
          END LOOP;
          IF NOT col_name_found THEN
            diff_str := diff_str || 'column ' || self.qual_table_name || '.' || self_col_types_arr(i).column_name ||
              ' not found in ' || other_table.qual_table_name || ', ';
          END IF;
        END;
      END LOOP;

      -- this pair of loops is just to find columns that exist in other but are not found in self
      FOR i IN 1 .. other_col_types_arr.count LOOP
        DECLARE
          col_name_found BOOLEAN := false;
        BEGIN
          FOR j IN 1 .. self_col_types_arr.count LOOP
            IF other_col_types_arr(i).column_name = self_col_types_arr(j).column_name THEN
              col_name_found := true;
            END IF;
          END LOOP;

          IF NOT col_name_found THEN
            diff_str := diff_str || 'column ' || other_table.qual_table_name || '.' || other_col_types_arr(i).column_name ||
              ' not found in ' || self.qual_table_name || ', ';
          END IF;
        END;
      END LOOP;

      IF length(diff_str) > 0 THEN --trim the trailing ', '
        diff_str := rtrim(diff_str, ', ');
      END IF;
    END IF;

    IF length(diff_str) > 0 THEN
      IF use_diff_results_table THEN
        --TODO: bind!!
        EXECUTE IMMEDIATE 'INSERT INTO ' || diff_results_table.qual_table_name || '(table1, table2, diff_type, result) ' ||
          'VALUES(''' || self.qual_table_name || ''', ''' || other_table.qual_table_name || ''', ''C'', ''' || diff_str ||  ''')';
      ELSE
        dbms_output.put_line(diff_str);
      END IF;
    END IF;

    IF is_diff THEN RETURN 1; ELSE RETURN 0; END IF;
  END diff_columns;
  ---

  -- TODO: only works for column diffing. should probably add other types or rename this to something like gen_col_diff_result_str
  MEMBER FUNCTION gen_diff_result_str(table1_name IN VARCHAR2, table2_name IN VARCHAR2, type1 IN COL_TYPE, type2 IN COL_TYPE) RETURN VARCHAR2 IS
    diff_str VARCHAR2(32767) := '';
  BEGIN
    IF type1.column_name != type2.column_name THEN --exit out if column_names don't match - caller will handle column names missing in one
      RETURN '';
    END IF;
    
    IF type1.data_type != type2.data_type AND NOT (type1.data_type IS NULL AND type2.data_type IS NULL) THEN
      diff_str := table1_name || '.' || type1.column_name || ' has data_type=' || type1.data_type || ' but ' ||
                  table2_name || '.' || type2.column_name || ' has data_type=' || type2.data_type || ', ';
    END IF;
    
    IF type1.data_type_mod != type2.data_type_mod AND NOT (type1.data_type_mod IS NULL AND type2.data_type_mod IS NULL) THEN
      diff_str := diff_str || table1_name || '.' || type1.column_name || ' has data_type_mod=' || type1.data_type_mod || ' but ' ||
                  table2_name || '.' || type2.column_name || ' has data_type_mod=' || type2.data_type_mod || ', ';
    END IF;

    IF type1.data_type_owner != type2.data_type_owner AND NOT (type1.data_type_owner IS NULL AND type2.data_type_owner IS NULL) THEN
      diff_str := diff_str || table1_name || '.' || type1.column_name || ' has data_type_owner=' || type1.data_type_owner || ' but ' ||
                  table2_name || '.' || type2.column_name || ' has data_type_owner=' || type2.data_type_owner || ', ';
    END IF;

    IF type1.data_length != type2.data_length AND NOT (type1.data_length IS NULL AND type2.data_length IS NULL) THEN
      diff_str := diff_str || table1_name || '.' || type1.column_name || ' has data_length=' || type1.data_length || ' but ' ||
                  table2_name || '.' || type2.column_name || ' has data_length=' || type2.data_length || ', ';
    END IF;
    
    IF type1.data_precision != type2.data_precision AND NOT (type1.data_precision IS NULL AND type2.data_precision IS NULL) THEN
      diff_str := diff_str || table1_name || '.' || type1.column_name || ' has data_precision=' || type1.data_precision || ' but ' ||
                  table2_name || '.' || type2.column_name || ' has data_precision=' || type2.data_precision || ', ';
    END IF;

    IF type1.data_scale != type2.data_scale AND NOT (type1.data_scale IS NULL AND type2.data_scale IS NULL) THEN
      diff_str := diff_str || table1_name || '.' || type1.column_name || ' has data_scale=' || type1.data_scale || ' but ' ||
                  table2_name || '.' || type2.column_name || ' has data_scale=' || type2.data_scale || ', ';
    END IF;
    
    IF type1.column_id != type2.column_id AND NOT (type1.column_id IS NULL AND type2.column_id IS NULL) THEN
      diff_str := diff_str || table1_name || '.' || type1.column_name || ' has column_id=' || type1.column_id || ' but ' ||
                  table2_name || '.' || type2.column_name || ' has column_id=' || type2.column_id || ', ';
    END IF;

    IF type1.default_length != type2.default_length AND NOT (type1.default_length IS NULL AND type2.default_length IS NULL) THEN
      diff_str := diff_str || table1_name || '.' || type1.column_name || ' has default_length=' || type1.default_length || ' but ' ||
                  table2_name || '.' || type2.column_name || ' has default_length=' || type2.default_length || ', ';
    END IF;

    IF length(diff_str) > 0 THEN --trim the trailing ', '
      diff_str := rtrim(diff_str, ', ');
    END IF;
    
    RETURN diff_str;
  END gen_diff_result_str;

  --

  MEMBER FUNCTION create_diff_results_table(diff_results_table IN table_obj) RETURN BOOLEAN IS
    create_stmt VARCHAR2(32767);
  BEGIN
    create_stmt := 'CREATE TABLE ' || diff_results_table.schema_name || '.DIFF_RESULTS (' ||
                   'ID NUMBER GENERATED AS IDENTITY,' ||
                   'DIFF_RAN_ON TIMESTAMP DEFAULT SYSTIMESTAMP,' ||
                   'TABLE1 VARCHAR2(767),' || --767 for 255+'.'+255+'@'+255
                   'TABLE2 VARCHAR2(767),' || 
                   'DIFF_TYPE VARCHAR2(1),' ||
                   'RESULT CLOB,' || 
                   'CONSTRAINT chk_diff_type CHECK (DIFF_TYPE IN (''C'', ''O'', ''I'', ''D''))' || --Columns, cOnstraints, Indexes, Data
                   ')'; 
    dbms_output.put_line(create_stmt);
    EXECUTE IMMEDIATE create_stmt;
    RETURN true;
  END create_diff_results_table;
END;
/