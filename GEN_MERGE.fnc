CREATE OR REPLACE FUNCTION GCE_META.GEN_MERGE(P_RECORDS IN VARCHAR2)
   RETURN CLOB IS
  LV_OWNER            VARCHAR2(100);
  LV_TABLE_NAME       VARCHAR2(100);
  LV_POSITION         NUMBER;
  LV_LINE_COUNT       NUMBER;
  CURSOR_NAME         INTEGER;
  ROWS_PROCESSED      INTEGER;
  COL_CNT             NUMBER;
  T_INT               NUMBER;
  T_DATE              DATE;
  T_RAW               VARCHAR2(32);
  T_VARCHAR           VARCHAR2(32767);
  T_CLOB              CLOB;
  MERGE_SQL_ROW       VARCHAR2(32767);
  MERGE_SQL           CLOB;
  MERGE_INTO          VARCHAR2(2000);
  SELECT_FROM         VARCHAR2(32767);
  USING_ON            VARCHAR2(2000);
  UPDATE_SET          VARCHAR2(32767);
  UPDATE_WHERE_CLAUSE VARCHAR2(32767);
  INSERT_COLUMN       VARCHAR2(32767);
  VALUE_COLUMN        VARCHAR2(32767);
  REC                 DBMS_SQL.DESC_REC;
  REC_TAB             DBMS_SQL.DESC_TAB;
  COL_CONS_TAB        DBMS_SQL.NUMBER_TABLE;
  LV_RECORDS          VARCHAR2(4000);
  lv_RECORDS_ORI      VARCHAR2(4000);
  ENTER_SYMBOL        VARCHAR2(10);
  LV_AUDIT_COL_FLG    CHAR(1);
  LV_SCHEMA_NAME      VARCHAR2(50);
  LV_IS_NULL_NUM      CHAR(1);
  LV_IS_NULL_CHAR     CHAR(1);
  LV_IS_NULL_RAW      CHAR(1);
  LV_IS_NULL_DATE     CHAR(1);
  lv_pk_exist		  pls_integer := 0;
  lv_NLS_DATE_FORMAT  VARCHAR2(100);
BEGIN
  lv_RECORDS := upper(P_RECORDS);
  select count(1)
    into LV_LINE_COUNT
    from (select regexp_substr(lv_RECORDS, '.+', 1, level)
            from dual
          connect by length(regexp_substr(lv_RECORDS, '.+', 1, level)) > 0);

  IF LV_LINE_COUNT = 1 THEN
    LV_POSITION := INSTR(lv_RECORDS,'WHERE',1,1); -- Check if the sql contains where clause
    IF LV_POSITION = 0 THEN
       if INSTR(lv_RECORDS,'ORDER BY',1,1) = 0 then
       LV_TABLE_NAME := SUBSTR(lv_RECORDS,INSTR(lv_RECORDS,'FROM',1,1)+5);
       else
       LV_TABLE_NAME := SUBSTR(lv_RECORDS,INSTR(lv_RECORDS,'FROM',1,1)+5,INSTR(lv_RECORDS,'ORDER BY',1,1) -INSTR(lv_RECORDS,'FROM',1,1)-6);
       end if;
    ELSE
       LV_TABLE_NAME := SUBSTR(lv_RECORDS,INSTR(lv_RECORDS,'FROM',1,1)+5,LV_POSITION-INSTR(lv_RECORDS,'FROM',1,1)-6);
    END IF;

    LV_TABLE_NAME := TRIM(LV_TABLE_NAME);
    LV_POSITION := INSTR(LV_TABLE_NAME,'.',1,1);
    IF LV_POSITION = 0 THEN
      LV_OWNER := 'GCE_META';
    ELSE
      LV_OWNER := SUBSTR(LV_TABLE_NAME,1,LV_POSITION-1);
	  if LV_OWNER = 'OPGCEP2' then
		LV_OWNER := 'GCE_META';
	  end if;
      LV_TABLE_NAME := SUBSTR(LV_TABLE_NAME,LV_POSITION+1);
    END IF;

    --DBMS_OUTPUT.PUT_LINE(LV_OWNER);
    --DBMS_OUTPUT.PUT_LINE(lv_RECORDS);

    --remove the ';' at the end of sql string
    IF SUBSTR(P_RECORDS, LENGTH(P_RECORDS), 1) = ';' THEN
      lv_RECORDS_ORI := SUBSTR(P_RECORDS, 1, LENGTH(P_RECORDS) - 1);
    else
    lv_RECORDS_ORI := P_RECORDS;
    END IF;
    --DBMS_OUTPUT.PUT_LINE(lv_RECORDS_ORI);

    --CHR(10) means 'ENTER'
    ENTER_SYMBOL := CHR(10);
    MERGE_SQL    := TO_CLOB('--GENERATE MERGE INTO FOR TABLE ' ||
                            LV_TABLE_NAME || CHR(10));
    --open a new cursor, When you no longer need this cursor, you must close it explicitly by calling CLOSE_CURSOR.
    CURSOR_NAME := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(CURSOR_NAME, lv_RECORDS_ORI, DBMS_SQL.NATIVE);
    ROWS_PROCESSED := DBMS_SQL.EXECUTE(CURSOR_NAME);
    --DBMS_OUTPUT.PUT_LINE('ROWS_PROCESSED = ' || ROWS_PROCESSED);
    DBMS_SQL.DESCRIBE_COLUMNS(CURSOR_NAME, COL_CNT, REC_TAB);
    --COL_CNT = the number of columns,
    FOR I IN 1 .. COL_CNT LOOP
      REC := REC_TAB(I);
      --DEFINE_COLUMN: COL_TYPE = 2(NUMBER), COL_TYPE IN (1, 96)(CHAR), COL_TYPE = 23(RAW), COL_TYPE = 12(DATE), COL_TYPE = 112(CLOB)
      IF REC_TAB(I).COL_TYPE = 2 THEN
        DBMS_SQL.DEFINE_COLUMN(CURSOR_NAME, I, T_INT);
      ELSIF REC_TAB(I).COL_TYPE IN (1, 96) THEN
        DBMS_SQL.DEFINE_COLUMN(CURSOR_NAME, I, T_VARCHAR, 32767);
      ELSIF REC_TAB(I).COL_TYPE = 23 THEN
        DBMS_SQL.DEFINE_COLUMN(CURSOR_NAME, I, T_VARCHAR, 32);
      ELSIF REC_TAB(I).COL_TYPE IN (12, 180, 181) THEN
        DBMS_SQL.DEFINE_COLUMN(CURSOR_NAME, I, T_DATE);
      ELSIF REC_TAB(I).COL_TYPE IN (112) THEN
        DBMS_SQL.DEFINE_COLUMN(CURSOR_NAME, I, T_CLOB);
      END IF;
      --check primary key
      SELECT COUNT(*)
        INTO COL_CONS_TAB(I)
        FROM ALL_CONSTRAINTS CONS, ALL_CONS_COLUMNS COLS
       WHERE COLS.TABLE_NAME = UPPER(LV_TABLE_NAME)
         AND CONS.CONSTRAINT_TYPE = UPPER('P')
         AND CONS.CONSTRAINT_NAME = COLS.CONSTRAINT_NAME
         AND CONS.OWNER = COLS.OWNER
         AND CONS.OWNER = UPPER(LV_OWNER)
         AND COLS.COLUMN_NAME = UPPER(REC_TAB(I).COL_NAME)
       ORDER BY COLS.TABLE_NAME, COLS.POSITION;
    END LOOP;

	for pk_cur in 1..COL_CONS_TAB.count
	loop
		lv_pk_exist := lv_pk_exist + COL_CONS_TAB(pk_cur);
	end loop;

	if lv_pk_exist = 0 then
		raise_application_error(-20001, 'primary key is not exist.', true);
	end if;

    --loop cursor to generate merge into for each row
    WHILE DBMS_SQL.FETCH_ROWS(CURSOR_NAME) > 0 LOOP

      SELECT DECODE(LV_OWNER,
                    'GCE_META',
                    '&&' || 'SCHEMA_METADATA..',
                    'GCE_TRANS',
                    '&&' || 'SCHEMA_TRANSACTION..',
                    'CSW_AUDIT',
                    '&&' || 'SCHEMA_CSW_AUDIT..',
                    '&&' || 'SCHEMA_METADATA..')
        INTO LV_SCHEMA_NAME
        FROM DUAL;
      MERGE_INTO          := 'MERGE INTO ' || LV_SCHEMA_NAME ||
                             UPPER(LV_TABLE_NAME) || ' T0' || ENTER_SYMBOL ||
                             'USING (';
      SELECT_FROM         := 'SELECT ';
      UPDATE_SET          := 'UPDATE SET ';
      USING_ON            := '';
      INSERT_COLUMN       := 'INSERT (';
      VALUE_COLUMN        := '';
      UPDATE_WHERE_CLAUSE := 'WHERE ';

      FOR I IN 1 .. COL_CNT LOOP
        CONTINUE WHEN REC_TAB(I).COL_NAME IN('CREATED_AT',
                                             'CREATED_FROM',
                                             'UPDATED_BY',
                                             'UPDATED_AT',
                                             'UPDATED_FROM');

        LV_IS_NULL_NUM  := 'N';
        LV_IS_NULL_CHAR := 'N';
        LV_IS_NULL_RAW  := 'N';
        LV_IS_NULL_DATE := 'N';
        --get column value according to different datatype
        IF (REC_TAB(I).COL_TYPE = 2) THEN
          DBMS_SQL.COLUMN_VALUE(CURSOR_NAME, I, T_INT);
          IF (T_INT IS NULL) THEN
            SELECT_FROM    := SELECT_FROM || 'NULL';
            LV_IS_NULL_NUM := 'Y';
          ELSE
            SELECT_FROM := SELECT_FROM || T_INT;
          END IF;
        ELSIF (REC_TAB(I).COL_TYPE IN (1, 96)) THEN
          DBMS_SQL.COLUMN_VALUE(CURSOR_NAME, I, T_VARCHAR);
          IF ((REC_TAB(I).COL_NAME = 'CREATED_BY') AND
             (LENGTH(T_VARCHAR) <> 7 OR UPPER(SUBSTR(T_VARCHAR, 1, 1)) NOT IN
             ('A', 'E', 'P', 'V', 'T'))) THEN
            SELECT_FROM := SELECT_FROM || '''' ||
                           SYS_CONTEXT('USERENV', 'OS_USER') || '''';
          ELSE
            IF (T_VARCHAR IS NULL) THEN
              SELECT_FROM     := SELECT_FROM || 'NULL';
              LV_IS_NULL_CHAR := 'Y';
            ELSE
              T_VARCHAR   := REPLACE(T_VARCHAR, '''', '''''');
              SELECT_FROM := SELECT_FROM || '''' ||
                             REPLACE(T_VARCHAR, '&', '''||''&''||''') || '''';
            END IF;
          END IF;
        ELSIF (REC_TAB(I).COL_TYPE = 23) THEN
          DBMS_SQL.COLUMN_VALUE(CURSOR_NAME, I, T_RAW);
          IF (T_RAW IS NULL) THEN
            SELECT_FROM    := SELECT_FROM || 'NULL';
            LV_IS_NULL_RAW := 'Y';
          ELSE
            SELECT_FROM := SELECT_FROM || 'HEXTORAW(''' || T_RAW || ''')';
          END IF;
        ELSIF (REC_TAB(I).COL_TYPE IN (12, 180, 181)) THEN
          DBMS_SQL.COLUMN_VALUE(CURSOR_NAME, I, T_DATE);
          IF (T_DATE IS NULL) THEN
            SELECT_FROM     := SELECT_FROM || 'NULL';
            LV_IS_NULL_DATE := 'Y';
          ELSE
			select SYS_CONTEXT('USERENV','NLS_DATE_FORMAT') into lv_NLS_DATE_FORMAT
				from dual;
            SELECT_FROM := SELECT_FROM || 'TO_DATE(''' || T_DATE ||
                           ''', '''||lv_NLS_DATE_FORMAT||''')';
          END IF;
        ELSIF (REC_TAB(I).COL_TYPE IN (112)) THEN
          DBMS_SQL.COLUMN_VALUE(CURSOR_NAME, I, T_CLOB);
          IF (DBMS_LOB.COMPARE(T_CLOB, EMPTY_CLOB()) = 0) THEN
            SELECT_FROM := SELECT_FROM || 'EMPTY_CLOB()';
          ELSE
            SELECT_FROM := SELECT_FROM || 'TO_CLOB(''' ||
                           REPLACE(TO_CHAR(T_CLOB), '''', '''''') || ''')';
          END IF;
        END IF;

        SELECT_FROM := SELECT_FROM || ' AS ' || REC_TAB(I).COL_NAME || ',' ||
                       ENTER_SYMBOL;
        IF COL_CONS_TAB(I) = 1 THEN
          IF USING_ON IS NULL THEN
            USING_ON := 'ON (';
          ELSE
            USING_ON := USING_ON || ' AND ';
          END IF;
          USING_ON := USING_ON || 'T0.' || REC_TAB(I).COL_NAME || ' = ' ||
                      'T1.' || REC_TAB(I).COL_NAME;
        ELSIF (REC_TAB(I).COL_NAME <> 'CREATED_BY') THEN
          UPDATE_SET := UPDATE_SET || 'T0.' || REC_TAB(I).COL_NAME || ' = ' ||
                        'T1.' || REC_TAB(I).COL_NAME || ',';
          IF REC_TAB(I).COL_TYPE IN (112) THEN
            UPDATE_WHERE_CLAUSE := UPDATE_WHERE_CLAUSE ||
                                   'DBMS_LOB.COMPARE(T0.' || REC_TAB(I)
                                  .COL_NAME || ', ' || 'T1.' || REC_TAB(I)
                                  .COL_NAME || ') <> 0 OR ';
          ELSIF (REC_TAB(I).COL_TYPE IN (2)) THEN
            UPDATE_WHERE_CLAUSE := UPDATE_WHERE_CLAUSE || 'NVL(T0.' || REC_TAB(I)
                                  .COL_NAME || ', 0.001) != ' || 'NVL(T1.' || REC_TAB(I)
                                  .COL_NAME || ', 0.001) OR ';
          ELSIF (REC_TAB(I).COL_TYPE IN (1, 96)) THEN
            UPDATE_WHERE_CLAUSE := UPDATE_WHERE_CLAUSE || 'NVL(T0.' || REC_TAB(I)
                                  .COL_NAME || ', ''NULL'') != ' ||
                                   'NVL(T1.' || REC_TAB(I).COL_NAME ||
                                   ', ''NULL'') OR ';
          ELSIF (REC_TAB(I).COL_TYPE IN (23)) THEN
            UPDATE_WHERE_CLAUSE := UPDATE_WHERE_CLAUSE || 'NVL(T0.' || REC_TAB(I)
                                  .COL_NAME || ', HEXTORAW(''FFFF'')) != ' ||
                                   'NVL(T1.' || REC_TAB(I).COL_NAME ||
                                   ', HEXTORAW(''FFFF'')) OR ';
          ELSIF (REC_TAB(I).COL_TYPE IN (12, 180, 181)) THEN
            UPDATE_WHERE_CLAUSE := UPDATE_WHERE_CLAUSE || 'NVL(T0.' || REC_TAB(I)
                                  .COL_NAME ||
                                   ', TO_DATE(''01-JAN-9999'', ''DD-MON-YYYY'')) != ' ||
                                   'NVL(T1.' || REC_TAB(I).COL_NAME ||
                                   ', TO_DATE(''01-JAN-9999'', ''DD-MON-YYYY'')) OR ';
          ELSE
            UPDATE_WHERE_CLAUSE := UPDATE_WHERE_CLAUSE || 'NVL(T0.' || REC_TAB(I)
                                  .COL_NAME || ', ''NULL'') != ' ||
                                   'NVL(T1.' || REC_TAB(I).COL_NAME ||
                                   ', ''NULL'') OR ';
          END IF;
        END IF;

        INSERT_COLUMN := INSERT_COLUMN || REC_TAB(I).COL_NAME || ',';
        VALUE_COLUMN  := VALUE_COLUMN || 'T1.' || REC_TAB(I).COL_NAME || ',';
      END LOOP;

      USING_ON := USING_ON || ') ';
      --truncate ',' at end of string
      SELECT_FROM         := SUBSTR(SELECT_FROM, 1, LENGTH(SELECT_FROM) - 2) ||
                             ' FROM DUAL) T1 ';
      UPDATE_WHERE_CLAUSE := SUBSTR(UPDATE_WHERE_CLAUSE,
                                    1,
                                    LENGTH(UPDATE_WHERE_CLAUSE) - 3);

      SELECT (CASE
               WHEN COUNT(1) > 0 THEN
                'Y'
               ELSE
                'N'
             END)
        INTO LV_AUDIT_COL_FLG
        FROM ALL_TAB_COLUMNS
       WHERE OWNER = UPPER(LV_OWNER)
         AND TABLE_NAME = UPPER(LV_TABLE_NAME)
         AND COLUMN_NAME IN ('CREATED_AT', 'UPDATED_AT');

      IF LV_AUDIT_COL_FLG = 'Y' THEN
        UPDATE_SET    := UPDATE_SET || ENTER_SYMBOL || 'T0.UPDATED_BY = ''' ||
                         SYS_CONTEXT('USERENV', 'OS_USER') || ''', ' ||
                         ENTER_SYMBOL || 'T0.UPDATED_AT = SYSTIMESTAMP, ' ||
                         ENTER_SYMBOL ||
                         'T0.UPDATED_FROM = SYS_CONTEXT(''USERENV'',''HOST'') ';
        INSERT_COLUMN := INSERT_COLUMN || 'CREATED_AT,CREATED_FROM) ';
        VALUE_COLUMN  := VALUE_COLUMN ||
                         'SYSTIMESTAMP,SYS_CONTEXT(''USERENV'',''HOST'')';
      ELSE
        UPDATE_SET    := SUBSTR(UPDATE_SET, 1, LENGTH(UPDATE_SET) - 2);
        INSERT_COLUMN := SUBSTR(INSERT_COLUMN, 1, LENGTH(INSERT_COLUMN) - 1);
        VALUE_COLUMN  := SUBSTR(VALUE_COLUMN, 1, LENGTH(VALUE_COLUMN) - 1);
      END IF;

      MERGE_SQL_ROW := MERGE_INTO || SELECT_FROM || ENTER_SYMBOL ||
                       USING_ON || ENTER_SYMBOL || 'WHEN MATCHED THEN ' ||
                       ENTER_SYMBOL || UPDATE_SET || ENTER_SYMBOL ||
                       UPDATE_WHERE_CLAUSE || ENTER_SYMBOL ||
                       'WHEN NOT MATCHED THEN ' || ENTER_SYMBOL ||
                       INSERT_COLUMN || ENTER_SYMBOL || 'VALUES(' ||
                       VALUE_COLUMN || ');' || ENTER_SYMBOL || ENTER_SYMBOL;

      DBMS_LOB.WRITEAPPEND(MERGE_SQL, LENGTH(MERGE_SQL_ROW), MERGE_SQL_ROW);
    END LOOP;

    DBMS_LOB.WRITEAPPEND(MERGE_SQL, LENGTH('COMMIT;'), 'COMMIT;');
    DBMS_SQL.CLOSE_CURSOR(CURSOR_NAME);
    RETURN(MERGE_SQL);
  ELSE
    MERGE_SQL := TO_CLOB('Please put all sql into one line!!');
    RETURN(MERGE_SQL);
  END IF;

EXCEPTION
  WHEN OTHERS THEN
    IF DBMS_SQL.IS_OPEN(CURSOR_NAME) THEN
      DBMS_SQL.CLOSE_CURSOR(CURSOR_NAME);
    END IF;
    DBMS_OUTPUT.PUT_LINE('ERROR_CODE: ' || SQLCODE);
    RAISE;
END GEN_MERGE;
/