------------------------------------------------------------------------------
--
--  Store dictionary table as inMemory collection...
--
--  Define dedicated type and  collection in package specification
--  Loaded/populated early, at the very first use of the package
--  Whenever package function/procedure is executed, collection exist in memory,
--  disk reads eliminated
--    Flexible use as indexed table or SQL function

--
--  https://github.com/Krzysztof-Rogoz/Oracle-samples
--
--  Created: 6-Feb-2024
--  Author: Krzysztof Rogoz
--  https://www.linkedin.com/in/krzysztof-rogoz-19b6781/
------------------------------------------------------------------------------

--
-- Create sample dictionary table
CREATE TABLE table_of_items (
          item_id VARCHAR2(10) NOT NULL,
          item_name VARCHAR2(100),
          further_details_fat_col VARCHAR2(4000),
          CONSTRAINT item_pk PRIMARY KEY (item_id)
);


--
--  Create package, in specification declare types
CREATE OR REPLACE PACKAGE myPkg
AS
   -- Define package variables for local use
   TYPE list_of_items_t IS TABLE OF table_of_items.item_name%TYPE
         INDEX BY table_of_items.item_id%TYPE;
   v_items_arr  list_of_items_t;
   --
   FUNCTION f_use_items_collection( p_id IN SMALLINT )
      RETURN VARCHAR2;
   PROCEDURE p_populate_items_collection;
END myPkg;
/


CREATE OR REPLACE PACKAGE BODY myPkg
AS
  --
  PROCEDURE p_populate_items_collection IS
    BEGIN
      v_items_arr.DELETE;  -- cleanup - just in case
      --
      FOR rec_items_coll IN (
        SELECT item_id, item_name FROM table_of_items )
      LOOP
       v_items_arr( rec_items_coll.item_id ) := rec_items_coll.item_name;
     END LOOP;
   END p_populate_items_collection;
   --
   --
    FUNCTION f_use_items_collection( p_id IN SMALLINT )
      RETURN VARCHAR2
    IS
    BEGIN
      -- Use in complex logic...
      RETURN v_items_arr( p_id );
   END f_use_items_collection;
   --
BEGIN
  dbms_output.enable(buffer_size => NULL );   -- useful for debugging
  p_populate_items_collection;  -- early init, at the first pkg usage
  --
  -- Execute business logic here....
  --
END myPkg;
/



--
-- unit tests
--
DECLARE
  v_ret_val  VARCHAR(100) := 'not used so far';
BEGIN
  -- truncate is DDL, in anonymous block can be executed as dynamic SQL only
  EXECUTE IMMEDIATE 'TRUNCATE TABLE table_of_items';
  INSERT INTO table_of_items (item_id, item_name) VALUES (1,'item1');
  INSERT INTO table_of_items (item_id, item_name) VALUES (2,'item2');
  INSERT INTO table_of_items (item_id, item_name) VALUES (3,'item3');
  --
  CASE v_ret_val
    WHEN 'item2' THEN dbms_output.put_line('Strage, not initiated yet...');
    WHEN 'not used so far'  THEN dbms_output.put_line('Correct, not initiated yet...');
    ELSE dbms_output.put_line('Fail');
  END CASE;
  dbms_output.put_line('Executing function..');
  SELECT myPkg.f_use_items_collection(2)
    INTO v_ret_val
    FROM DUAL;
  CASE v_ret_val
    WHEN 'item2' THEN dbms_output.put_line('Pass');
    ELSE dbms_output.put_line('Fail');
  END CASE;
END;
