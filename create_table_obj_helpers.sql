CREATE OR REPLACE PROCEDURE drop_type_if_not_exists(type_name IN VARCHAR2, force IN BOOLEAN DEFAULT false) IS
  exec_str VARCHAR2(255);
BEGIN
  exec_str := 'DROP TYPE ' || type_name;
  IF force THEN
    exec_str := exec_str || ' FORCE';
  END IF;

  EXECUTE IMMEDIATE exec_str;
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE != -4043 THEN --code for does not exist
        RAISE;
      END IF;
END;
/

EXEC drop_type_if_not_exists(type_name => 'cols_arr', force => true);
/

CREATE TYPE cols_arr IS TABLE OF VARCHAR2(255);
/

EXEC drop_type_if_not_exists(type_name => 'col_type', force => true);
/

CREATE OR REPLACE TYPE col_type AS OBJECT ( --these fields come from *_tab_cols
  column_name VARCHAR2(128),
  data_type VARCHAR2(106),
  data_type_mod VARCHAR2(3),
  data_type_owner VARCHAR2(30),
  data_length NUMBER,
  data_precision NUMBER,
  data_scale NUMBER,
  column_id NUMBER,
  default_length NUMBER,
  -- data_default NUMBER, -- this is long in *_tab_cols, but this is an illegal type here
  MAP MEMBER FUNCTION equals RETURN RAW
);
/

CREATE OR REPLACE TYPE BODY col_type AS
  MAP MEMBER FUNCTION equals RETURN RAW AS
  BEGIN
  -- Return concatenated RAW string of
  -- all attributes of the object
  return
       -- NVL() to avoid NULLS being treated
       -- as equal. NVL default values: choose
       -- carefully!
       utl_raw.cast_to_raw(
          nvl(self.column_name, '***')
       || nvl(self.data_type, '***')
       || nvl(self.data_type_mod, '***')
       || nvl(self.data_type_owner, '***')
       || nvl(self.data_length, -1)
       || nvl(self.data_precision, -1)
       || nvl(self.data_scale, -1) 
       || nvl(self.column_id, -1) 
       || nvl(self.default_length, -1)
       -- || nvl(self.data_default, -1)
        );
     END equals;
   END;
/

EXEC drop_type_if_not_exists(type_name => 'varchar2_arr', force => true);
/
CREATE TYPE varchar2_arr IS TABLE OF VARCHAR2(32767);
/

EXEC drop_type_if_not_exists(type_name => 'col_types_arr', force => true);
/
CREATE TYPE col_types_arr IS TABLE OF COL_TYPE;
/
