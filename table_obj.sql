\ -- -*- plsql -*-
DROP TYPE cols_arr;
/
CREATE TYPE cols_arr IS TABLE OF VARCHAR2(255);
/
DROP TYPE varchar2_arr;
/
CREATE TYPE varchar2_arr IS TABLE OF VARCHAR2(32767);
/


DROP TYPE table_obj FORCE;
/
CREATE TYPE table_obj AS OBJECT (
    table_name VARCHAR2(255),
    schema_name VARCHAR2(255),
    dblink VARCHAR2(255),

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
    /* returns COLS_ARR of primary key columns in the table, each prefixed with alias || '.' if alias specified, excluding any columns specified in exclude_list */
    MEMBER FUNCTION pk_cols_arr(alias IN VARCHAR2 DEFAULT NULL, exclude_list IN COLS_ARR DEFAULT NULL) RETURN COLS_ARR,
    /* returns COLS_ARR of all non-primary key columns in the table, each prefixed with alias || '.' if alias specified, excluding any columns specified in exclude_list */
    MEMBER FUNCTION non_pk_cols_arr(alias IN VARCHAR2 DEFAULT NULL, exclude_list IN COLS_ARR DEFAULT NULL) RETURN COLS_ARR,
    /* enables all non-unique indexes on the table (uniques are needed to enforce unique/primary keys */
    MEMBER FUNCTION enable_indexes RETURN INT,
    /* disables all non-unique indexes on the table (uniques are needed to enforce unique/primary keys */
    MEMBER FUNCTION disable_indexes RETURN INT,
    /* given other_table (name of another table in the same schema), returns string of join columns between the two tables. Joins on primary key columns having the same name only. self_alias and other_alias are optional aliases for this table and other_table, respectively */
    MEMBER FUNCTION join_list(other_table IN table_obj, self_alias IN VARCHAR2 DEFAULT 'a', other_alias IN VARCHAR2 DEFAULT 'b') RETURN VARCHAR2,
    /* given this table and other_table, return string representing setting each column in self table to each column in other_table, matched by names, PK cols excluded
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
    /* generates "CREATE TABLE" DDL stsatement given column names, types, constraints and keys passed in. extra_stuff is appended, which is useful for special features */
    MEMBER FUNCTION gen_create_ddl(cols_name_arr IN cols_arr, cols_type_arr IN varchar2_arr, cols_constraints IN varchar2_arr, keys IN varchar2_arr, extra_stuff IN VARCHAR2 DEFAULT NULL) RETURN VARCHAR2,
    /* generates an INSERT statement that inserts n rows of random data. min_date and max_date define the range of dates and timestamps allowed. Foreign key cols are generated from selecting a random value from the parent table */
    MEMBER FUNCTION gen_insert_random_rows_stmt(n IN INT, min_date IN DATE, max_date IN DATE) RETURN LONG,
    /* inserts n rows of random data. min_date and max_date define the range of dates and timestamps allowed. Foreign key cols are generated from selecting a random value from the parent table */
    MEMBER FUNCTION insert_random_rows(n IN INT, min_date IN DATE, max_date IN DATE) RETURN INT,
    /* returns true if column name specified is part of a foreign key, false otherwise */
    MEMBER FUNCTION col_is_fk(col_name IN VARCHAR2) RETURN BOOLEAN,
    /* returns a random referenced value in a parent table, given a foreign key column name - NOTE: only works on NUMBER type at this time, does not work on composite foreign keys at this time */
    MEMBER FUNCTION rand_ref_val(fk_col_name IN VARCHAR2) RETURN NUMBER,
    /* returns a COLS_ARR of columns in this table that intersect with columns in other_cols, parameters nulls and nulls_exclude_list are used to exclude specified column names with the string 'NULL' */
    MEMBER FUNCTION intersecting_cols_arr(other_cols IN COLS_ARR, nulls_exclude_list IN COLS_ARR DEFAULT NULL, nulls IN BOOLEAN DEFAULT false) RETURN COLS_ARR,

    /* returns a comma-delimited string of columns in this table that intersect with columns in other_cols, parameters nulls and nulls_exclude_list are used to exclude specified column names with the string 'NULL' */
    MEMBER FUNCTION intersecting_cols(other_cols IN COLS_ARR, nulls_exclude_list IN COLS_ARR DEFAULT NULL, nulls IN BOOLEAN DEFAULT false) RETURN VARCHAR2,
    /* returns true if table indicated by table_name, schema_name and dblink exists, false otherwise */
    MEMBER FUNCTION table_exists RETURN BOOLEAN
  ) NOT FINAL;
/

  CREATE OR REPLACE TYPE BODY table_obj AS
    MEMBER FUNCTION qual_table_name RETURN VARCHAR2 IS
    BEGIN
      --dbms_output.put_line('qual_table_name called');
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
      col_list VARCHAR2(4000) := ''; 
      l_cols_arr COLS_ARR;
    BEGIN
      --dbms_output.put_line('all_cols called');
      l_cols_arr := all_cols_arr(alias, exclude_list);
      
      FOR i IN 1 .. l_cols_arr.count LOOP
        col_list := col_list || l_cols_arr(i) || CASE WHEN i < l_cols_arr.count THEN ',' END;
      END LOOP;

      RETURN col_list;
    END all_cols;

    ---

    MEMBER FUNCTION pk_cols(alias IN VARCHAR2 DEFAULT NULL, exclude_list IN COLS_ARR DEFAULT NULL) RETURN VARCHAR2 IS
      col_list VARCHAR2(4000);
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
      col_list VARCHAR2(4000);
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
      qry VARCHAR2(4000);
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

      --dbms_output.put_line(qry);
      --dbms_output.put_line(self.upper_table_name() || ',' || self.upper_schema_name());

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
      qry VARCHAR2(4000);
    BEGIN
      qry := 'SELECT ' || CASE WHEN alias IS NOT NULL THEN '''' || alias || '.'' || ' END || 'column_name ' ||
	     'FROM ' || CASE WHEN dblink IS NOT NULL THEN 'user_tab_columns@' || dblink ELSE 'all_tab_columns' END || ' ' ||
	     'WHERE table_name=:1' || CASE WHEN dblink IS NULL AND schema_name IS NOT NULL THEN ' AND owner=:2' END || ' ' ||
             CASE WHEN exclude_list IS NOT NULL AND exclude_list.count>0  THEN 'AND column_name NOT IN ( ' || upper(self.cols_arr_to_commalist(exclude_list, true)) || ') ' END ||
	     'ORDER BY column_id';

      --dbms_output.put_line(qry);

      IF dblink IS NULL AND schema_name IS NOT NULL then
	EXECUTE IMMEDIATE qry BULK COLLECT INTO ret_cols_arr USING self.upper_table_name(), self.upper_schema_name();
      ELSE
	EXECUTE IMMEDIATE qry BULK COLLECT INTO ret_cols_arr USING self.upper_table_name();
      END IF;

      RETURN ret_cols_arr;
    END all_cols_arr;

    ---

    MEMBER FUNCTION non_pk_cols_arr(alias IN VARCHAR2 DEFAULT NULL, exclude_list IN COLS_ARR DEFAULT NULL) RETURN COLS_ARR IS
      ret_cols_arr cols_arr;
      qry VARCHAR2(4000);
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
      --dbms_output.put_line('enable_indexes called');
      FOR rec IN (SELECT * FROM all_indexes WHERE owner=self.upper_schema_name() AND table_name=self.upper_schema_name() AND uniqueness='NONUNIQUE') LOOP
	IF rec.index_type IN ('NORMAL', 'NORMAL/REV', 'FUNCTION-BASED DOMAIN', 'FUNCTION-BASED NORMAL') THEN
	  rebuild_str := 'ALTER INDEX ' || rec.index_name || ' REBUILD';
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
      --dbms_output.put_line('disable_indexes called');
      FOR rec IN (SELECT * FROM all_indexes WHERE owner=self.upper_schema_name() AND table_name=self.upper_schema_name() AND uniqueness='NONUNIQUE') LOOP
	IF rec.index_type IN ('NORMAL', 'NORMAL/REV', 'FUNCTION-BASED DOMAIN', 'FUNCTION-BASED NORMAL') THEN
	  disable_str := 'ALTER INDEX ' || rec.index_name || ' UNUSABLE';
	END IF; 
        --dbms_output.put_line('disable_str=' || disable_str);
	EXECUTE IMMEDIATE disable_str;
        index_count := index_count + 1;
      END LOOP;
      --dbms_output.put_line('disable_indexed finished');
      RETURN index_count;
    END disable_indexes;

    ---

    --returns join conditions for joining this table with other_table on intersecting primary keys only
    MEMBER FUNCTION join_list(other_table IN table_obj, self_alias IN VARCHAR2 DEFAULT 'a', other_alias IN VARCHAR2 DEFAULT 'b') RETURN VARCHAR2 IS
      other_pk_cols cols_arr;
      self_pk_cols cols_arr;
      join_str VARCHAR2(4000) := '';
    BEGIN
      --dbms_output.put_line('join_list called');
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
      ret_update_list VARCHAR2(4000) := ''; 
      self_non_pk_cols COLS_ARR;
      other_non_pk_cols COLS_ARR;
    BEGIN
      --dbms_output.put_line('matched_update_list called');
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
      --dbms_output.put_line(add_col_str);
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

    MEMBER FUNCTION gen_insert_random_rows_stmt(n IN INT, min_date IN DATE, max_date IN DATE) RETURN LONG IS
      qry LONG;
      rand_fk_ref VARCHAR2(32767);
    BEGIN
      qry := 'INSERT INTO ' || self.qual_table_name() || '(' || self.all_cols() || ')';

      IF self.dblink IS NOT NULL THEN 
        raise_application_error( -20001, 'dblink not supported by insert_random_rows function at this time');
      END IF;
      
      /* loop n times, where n is number of rows to generate */
      FOR i in 1 .. n LOOP
        qry := qry || 'SELECT ';

        /* loop over each column */
	FOR rec IN (SELECT column_name, data_type, data_length, data_precision, data_scale
		    FROM user_tab_cols
		    WHERE table_name=self.table_name
                    ORDER by column_id) LOOP
          IF col_is_fk(rec.column_name) THEN --assumes number for now
            qry := qry || to_char(self.rand_ref_val(rec.column_name));
	  ELSIF rec.data_type='NUMBER' THEN
            --dbms_output.put_line('column is number: data_precision=' || rec.data_precision || 'data_scale=' || rec.data_scale);
	    qry := qry || to_char(trunc(power(10, CASE WHEN rec.data_precision IS NULL THEN 10 ELSE rec.data_precision-rec.data_scale END)*dbms_random.value, rec.data_scale));
	  ELSIF rec.data_type IN ('VARCHAR', 'CLOB') THEN
            --dbms_output.put_line('column is varchar/clob');
	    qry := qry || '''' || dbms_random.string('A', rec.data_length) || ''''; --A means mixed case letters
	  ELSIF rec.data_type='DATE' THEN
            --dbms_output.put_line('column is date');
	    qry := qry || 'to_date(''' || to_char(min_date+trunc(dbms_random.value*(max_date-min_date), 0), 'yyyymmdd hh24:mi:ss') ||
                       ''', ''yyyymmdd hh24:mi:ss'')';
	  ELSIF rec.data_type LIKE 'TIMESTAMP%' THEN
            --dbms_output.put_line('column is timestamp');
	    qry := qry || 'to_timestamp(''' || to_char(cast(min_date+dbms_random.value*(max_date-min_date) as timestamp), 'yyyymmdd hh24:mi:ss.ff') ||
                       ''', ''yyyymmdd hh24:mi:ss.ff'')';
            --qry := qry || to_char(cast(min_date+dbms_random.value*(max_date-min_date) as timestamp), 'yyyymmdd hh24:mi:ss.ff');
	  ELSE
	    raise_application_error( -20002, 'data type not supported at this time: ' || rec.data_type);
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
        FROM user_cons_columns col, user_constraints cons
        WHERE (cons.table_name=col.table_name AND cons.constraint_name=col.constraint_name)
        AND cons.table_name=self.upper_table_name()
        AND column_name=col_name
        AND constraint_type='R'
      )) LOOP

        RETURN true;     
      END LOOP;

      RETURN false;
    END col_is_fk;
 
    ---

    MEMBER FUNCTION rand_ref_val(fk_col_name IN VARCHAR2) RETURN NUMBER IS
      refd_col VARCHAR2(32767);
      refd_table VARCHAR2(32767);
      ret_val NUMBER;
      qry LONG;
    BEGIN
      --dbms_output.put_line('rand_ref_val called: fk_col_name=' || fk_col_name || ', self.upper_table_name()=' || self.upper_table_name());

      --get referenced column
      SELECT pk_col.column_name, pk_col.table_name INTO refd_col, refd_table
        FROM user_constraints cons, user_cons_columns col, user_constraints pk_cons, user_cons_columns pk_col
        WHERE
            (cons.owner=col.owner AND cons.table_name=col.table_name AND cons.constraint_name=col.constraint_name)
        AND (cons.r_constraint_name=pk_cons.constraint_name AND cons.r_owner=pk_cons.owner)
        AND (pk_cons.owner=pk_col.owner AND pk_cons.table_name=pk_col.table_name AND pk_cons.constraint_name=pk_col.constraint_name)
        AND cons.table_name=self.upper_table_name()
        AND col.column_name=fk_col_name
        AND cons.constraint_type='R';

      qry := 'SELECT ' || refd_col || ' FROM (SELECT '  || refd_col || ' FROM ' || refd_table || ' ORDER BY dbms_random.random) WHERE rownum=1';
  
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
      --dbms_output.put_line('intersecting_cols_arr called');
      my_cols := self.all_cols_arr();
 
      FOR i IN 1 .. my_cols.count LOOP
        --dbms_output.put_line('intersecting_cols_arr: i=' || i);
        match_found := false;
        exclude_found := false;
        FOR j IN 1 .. other_cols.count LOOP
          --dbms_output.put_line('intersecting_cols_arr: j=' || j);

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
        --dbms_output.put_line('intersecting_cols: i=' || i);
        col_list := col_list || l_cols_arr(i) || CASE WHEN i < l_cols_arr.count THEN ',' END;
      END LOOP;

      RETURN col_list;
    END intersecting_cols;

    --

    MEMBER FUNCTION table_exists RETURN BOOLEAN IS      
      cols COLS_ARR;
    BEGIN
      --dbms_output.put_line('table_exists called');
      cols := self.all_cols_arr();

      --dbms_output.put_line('cols.count=' || cols.count);

      IF cols.count=0 OR cols IS NULL THEN
        RETURN false;
      END IF;

      RETURN true;
    END table_exists;
  END;

/
---
