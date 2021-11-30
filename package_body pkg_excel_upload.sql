CREATE OR REPLACE PACKAGE BODY PKG_EXCEL_UPLOAD
IS

--validation values
 VDIVISION VARCHAR2(30);
 VCODUSR VARCHAR2(255);
 VNUMORD NUMBER;
 VCUSTOMER VARCHAR2(255);
 VPRODUCT VARCHAR2(255);
 V_DATE VARCHAR2(255);
 VALID_YEAR_MONTH VARCHAR2(255);
 VQUANTITY VARCHAR2(255);
 VAMOUNT VARCHAR2(255);
 VSICK_DATE VARCHAR2(255);
 VPOLICY_CODE VARCHAR2(255);
 VRATE_DATE VARCHAR2(255);
 VRATE NUMBER(14,4);

 -- target
 VCATEGORY VARCHAR2(255);
 VCOMP_TYPE VARCHAR2(255);
 VTARGET NUMBER(12,2);
 VTRG_DATE VARCHAR2(255);

--check rows
 VCOUNT NUMBER;

-- exceptions - errors
exsp    EXCEPTION;
my_code NUMBER;
my_errm VARCHAR2(32000);



FUNCTION IS_NUMBER( p_str IN VARCHAR2 )
  RETURN VARCHAR2 DETERMINISTIC PARALLEL_ENABLE
IS
  l_num NUMBER;
BEGIN
  l_num := to_number( p_str );
  RETURN 'Y';
EXCEPTION
  WHEN value_error THEN
    RETURN 'N';
END IS_NUMBER;


FUNCTION is_valid_date_format ( d_format in VARCHAR2 )
  RETURN VARCHAR2 DETERMINISTIC PARALLEL_ENABLE
IS
  v_date date;
  BEGIN
      SELECT TO_DATE(d_format,'DDMMYYYY') into v_date from dual;
      RETURN 'TRUE';

  EXCEPTION
      WHEN OTHERS THEN
          RETURN 'FALSE';

END is_valid_date_format;


PROCEDURE SP_UPLOAD_B2B_C (VERRORCODE OUT VARCHAR2 ) --VERRORCODE
AS


TYPE getData IS TABLE OF xin_excel_b2b%ROWTYPE;
getValue getData;



BEGIN
   --CALL VALIDATE PROCEDURE
   SP_VALIDATE_B2B_C (VERRORCODE) ;

    IF VERRORCODE = 'KO' -- wrong data uploaded
      THEN
           RAISE exsp;
    END IF;

    --data is correct & insert all records in historical table ..
    INSERT INTO XIN_EXCEL_B2B_HIST
    SELECT XIN.* , SYSDATE
    FROM XIN_EXCEL_B2B XIN;

    COMMIT;

BEGIN

SELECT *
  BULK COLLECT INTO getValue
  FROM xin_excel_b2b;

   FOR i IN getValue.FIRST..getValue.LAST
    LOOP

               VDIVISION := getValue(i).DIVISION;
               VCODUSR   :=  getValue(i).CODUSR;
               VCUSTOMER := getValue(i).CUSTOMER;
               VPRODUCT  :=  getValue(i).PRODUCT;
               VQUANTITY := TO_NUMBER(LTRIM(getValue(i).QUANTITY,'0'));
               VAMOUNT   := TO_NUMBER(LTRIM(getValue(i).AMOUNT,'0'));
               V_DATE    := TO_DATE(getValue(i).VDATE,'DD/MM/YYYY');

               SELECT NVL(MAX(NUMORD) + 1 , 1 )
               INTO VNUMORD
               FROM T660CONT_ATT;

            MERGE INTO T660CONT_ATT ATT
            USING (SELECT getValue(i).DIVISION AS DIVISION,
                          getValue(i).CODUSR AS CODUSR,
                          getValue(i).CUSTOMER AS CUSTOMER,
                          getValue(i).PRODUCT AS PRODUCT,
                          TO_NUMBER(LTRIM(getValue(i).QUANTITY,'0')) AS QUANTITY,
                          TO_NUMBER(LTRIM(getValue(i).AMOUNT,'0')) AS AMOUNT,
                          TO_DATE(getValue(i).VDATE,'DD/MM/YYYY') AS DINV
                   FROM DUAL)XIN
                   ON (XIN.DIVISION = ATT.CODDIV
                   AND XIN.CODUSR = ATT.CODUSR
                   AND XIN.CUSTOMER = ATT.CODCUSTINV
                   AND XIN.PRODUCT = ATT.CODART
                   AND XIN.DINV = ATT.DTEINV)
                   WHEN MATCHED THEN
                     UPDATE SET ATT.QTYINV = VQUANTITY , ATT.NETAMOUNT = VAMOUNT
                   WHEN NOT MATCHED THEN
                     INSERT (NUMORD,CODTYPORD,NUMROW,CODDIV,CODUSR,CODCUSTINV,CODCUSTDELIV,CODART,QTYINV,NETAMOUNT,DTEINV)
                     VALUES (VNUMORD,'70','1',VDIVISION , VCODUSR , VCUSTOMER, VCUSTOMER,VPRODUCT , VQUANTITY,VAMOUNT, V_DATE);


                    /*
                     --update the progressive table T011PRG next value for numord
                     UPDATE T011PRG SET NUMPRG = NUMPRG + 1
                     WHERE CODPRG = 'NUMORD';
                    */
                   COMMIT;


  END LOOP;
  END;

  -- DELETE XIN TABLE
  DELETE FROM XIN_EXCEL_B2B;
  COMMIT;


EXCEPTION

  WHEN exsp THEN
    my_code := SQLCODE;
    my_errm := SQLERRM;

        INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
        VALUES (my_code ,my_errm , null );

        DELETE FROM XIN_EXCEL_B2B;

        COMMIT;

  WHEN OTHERS THEN
    my_code := SQLCODE;
    my_errm := SQLERRM;

    INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
    VALUES (my_code ,my_errm , null );

    DELETE FROM XIN_EXCEL_B2B;

    COMMIT;
  RAISE;

END SP_UPLOAD_B2B_C;


PROCEDURE SP_VALIDATE_B2B_C (VERRORCODE OUT VARCHAR2) AS

BEGIN

    DELETE FROM XIN_EXCEL_B2B_LOG;
    COMMIT;

   VERRORCODE := 'KO';

DECLARE CURSOR XIN IS
        SELECT * FROM XIN_EXCEL_B2B;

BEGIN
   FOR B2B IN XIN
    LOOP

       VDIVISION := B2B.DIVISION;
       VCODUSR := B2B.CODUSR;
       VCUSTOMER := B2B.CUSTOMER;
       VPRODUCT := B2B.PRODUCT;
       V_DATE := B2B.VDATE;
       VQUANTITY := B2B.QUANTITY;
       VAMOUNT := B2B.AMOUNT;


    --Null row key check
    IF  (VDIVISION IS NULL OR VDIVISION = '') OR
        (VCODUSR IS NULL OR VCODUSR = '')OR
        (VCUSTOMER  IS NULL OR VCUSTOMER = '')OR
        (VPRODUCT IS NULL OR VPRODUCT = '')OR
        (V_DATE IS NULL OR V_DATE = '')
      THEN
      INSERT INTO XIN_EXCEL_B2B_LOG
      VALUES (VDIVISION, VCODUSR, VCUSTOMER , VPRODUCT ,VQUANTITY , VAMOUNT,V_DATE, 'Null key found', SYSDATE );
     END IF;

    --Duplicate row key check -
    INSERT INTO XIN_EXCEL_B2B_LOG
      SELECT excel.DIVISION,
             excel.CODUSR,
             excel.CUSTOMER,
             excel.PRODUCT,
             excel.QUANTITY,
             excel.AMOUNT ,
             excel.VDATE,
             'Duplicate ROWS',
             SYSDATE
      FROM (  SELECT B.DIVISION, B.CODUSR , B.CUSTOMER, B.PRODUCT ,B.QUANTITY,B.AMOUNT, B.VDATE,
          ROW_NUMBER() OVER (PARTITION BY B.DIVISION,B.CODUSR,B.CUSTOMER,B.PRODUCT,B.VDATE ORDER BY ROWID ) AS Duplicated
          FROM XIN_EXCEL_B2B B) excel
      WHERE excel.Duplicated > 1 ;


       --Correct value check - DIVISONCODE
       SELECT COUNT(*)
       INTO VCOUNT
       FROM QTABS_C
       WHERE CODTAB = 'CDIV'
       AND COD = VDIVISION;

    IF (VCOUNT = 0 )
      THEN
      INSERT INTO XIN_EXCEL_B2B_LOG
      VALUES (VDIVISION, VCODUSR, VCUSTOMER , VPRODUCT ,VQUANTITY , VAMOUNT,V_DATE, 'Wrong value for DIVISION CODE  '|| VDIVISION, SYSDATE );
     END IF;


    --Correct value check - CODUSR
    SELECT COUNT(*)
    INTO VCOUNT
    FROM T030USER
    WHERE CODUSR = VCODUSR;

    IF ( VCOUNT = 0 )
      THEN
      INSERT INTO XIN_EXCEL_B2B_LOG
      VALUES (VDIVISION, VCODUSR, VCUSTOMER , VPRODUCT ,VQUANTITY , VAMOUNT,V_DATE, 'Wrong value for CODUSR '|| VCODUSR, SYSDATE );
     END IF;


      --Correct value check - CODUSR & CODDIV
      SELECT COUNT(*)
      INTO VCOUNT
      FROM T031USERDIV
      WHERE CODUSR = VCODUSR
      AND CODDIV = VDIVISION;

      IF (VCOUNT = 0 )
       THEN
        INSERT INTO XIN_EXCEL_B2B_LOG
        VALUES (VDIVISION, VCODUSR, VCUSTOMER , VPRODUCT ,VQUANTITY , VAMOUNT,V_DATE, 'Wrong value for CODUSR & DIVISION ' || VCODUSR || ' ' || VDIVISION, SYSDATE );
      END IF;

    --Correct value check - CUSTOMER
      SELECT COUNT(*)
      INTO VCOUNT
      FROM T040PARTY
      WHERE CODPARTY = VCUSTOMER;

     IF ( VCOUNT = 0 )
       THEN
        INSERT INTO XIN_EXCEL_B2B_LOG
        VALUES (VDIVISION, VCODUSR, VCUSTOMER , VPRODUCT ,VQUANTITY , VAMOUNT,V_DATE, 'Wrong value for CUSTOMER  ' || VCUSTOMER, SYSDATE);
      END IF;

     --Correct value check - PRODUCT
     SELECT COUNT(*)
     INTO VCOUNT
     FROM T060ARTICLE
     WHERE CODCATMER = VPRODUCT;

     IF ( VCOUNT = 0 )
       THEN
        INSERT INTO XIN_EXCEL_B2B_LOG
        VALUES (VDIVISION, VCODUSR, VCUSTOMER , VPRODUCT ,VQUANTITY , VAMOUNT,V_DATE, 'Wrong value for PRODUCT  ' || VPRODUCT, SYSDATE);
      END IF;

     --Correct quantity number - QUANTITY
     IF IS_NUMBER(VQUANTITY) = 'N'
       THEN
         INSERT INTO XIN_EXCEL_B2B_LOG
         VALUES (VDIVISION, VCODUSR, VCUSTOMER , VPRODUCT ,VQUANTITY , VAMOUNT,V_DATE, 'Wrong value for the quantity  ' || VQUANTITY, SYSDATE);
     END IF;

     --Correct amount number - AMOUNT
     IF IS_NUMBER(VAMOUNT) = 'N'
      THEN
        INSERT INTO XIN_EXCEL_B2B_LOG
        VALUES (VDIVISION, VCODUSR, VCUSTOMER , VPRODUCT ,VQUANTITY , VAMOUNT,V_DATE, 'Wrong value for the amount  ' || VAMOUNT, SYSDATE);
      END IF;

     --Correct format date - VDATE
     IF is_valid_date_format(V_DATE) = 'FALSE'
      THEN
        INSERT INTO XIN_EXCEL_B2B_LOG
        VALUES (VDIVISION, VCODUSR, VCUSTOMER , VPRODUCT ,VQUANTITY , VAMOUNT,V_DATE, 'Wrong format date ' || V_DATE, SYSDATE);
      END IF;

     --Correct valid date - VDATE
     SELECT TO_CHAR(SYSDATE,'MM/YYYY')
     INTO VALID_YEAR_MONTH
     FROM DUAL;

     IF( TO_CHAR(TO_DATE(V_DATE,'DD/MM/YYYY'),'MM/YYYY')  <>  VALID_YEAR_MONTH )
      THEN
        INSERT INTO XIN_EXCEL_B2B_LOG
        VALUES (VDIVISION, VCODUSR, VCUSTOMER , VPRODUCT ,VQUANTITY , VAMOUNT,V_DATE, 'Wrong value for date ' || V_DATE, SYSDATE);
     END IF;

       COMMIT; -- COMMIT

END LOOP;

    SELECT COUNT(*)
      INTO VCOUNT
      FROM XIN_EXCEL_B2B_LOG;

       IF VCOUNT = 0  -- correct data
         THEN
            VERRORCODE :='OK';  -- complete with success

          ELSE
            VERRORCODE :='KO';  -- ERRORS
            -- CLEAR XIN TABLE FOR THE NEXT UPLOAD
            DELETE FROM XIN_EXCEL_B2B;
          COMMIT;

       END IF;

END;

  EXCEPTION
       WHEN OTHERS THEN
          my_code := SQLCODE;
          my_errm := SQLERRM;

          INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
          VALUES (my_code ,my_errm , '' );

          DELETE FROM XIN_EXCEL_B2B;

          COMMIT;

        RAISE;

END SP_VALIDATE_B2B_C;


PROCEDURE SP_UPLOAD_SICK_LEAVE_C (VERRORCODE OUT VARCHAR2 ) --VERRORCODE
AS


TYPE getData IS TABLE OF xin_excel_sick_leave%ROWTYPE;
getValue getData;


BEGIN

   --CALL VALIDATE PROCEDURE
   SP_VALIDATE_SICK_LEAVE_C (VERRORCODE) ;

    IF VERRORCODE = 'KO' -- wrong data uploaded
      THEN
           RAISE exsp;
     END IF;


    --data is correct & insert all records in historical table ..
    INSERT INTO XIN_EXCEL_SICK_LEAVE_HIST
    SELECT xin.* , SYSDATE
    FROM xin_excel_sick_leave xin;

    COMMIT;


BEGIN

  SELECT *
  BULK COLLECT INTO getValue
  FROM xin_excel_sick_leave;

FOR i IN getValue.FIRST..getValue.LAST
  LOOP
                 VDIVISION := getValue(i).DIVISION;
                 VCODUSR  :=  getValue(i).CODUSR;
                 VSICK_DATE := TO_DATE(getValue(i).SICK_LEAVE_DATE,'DD/MM/YYYY');
                 VPOLICY_CODE := getValue(i).SICK_CODE;



                  MERGE INTO TZ_SICK_LEAVE SIK
                  USING ( SELECT getValue(i).DIVISION AS DIVISION,
                                 getValue(i).CODUSR AS CODUSR,
                                 TO_DATE(getValue(i).SICK_LEAVE_DATE,'DD/MM/YYYY') AS SICKDATE,
                                 getValue(i).SICK_CODE AS POLICY_CODE
                          FROM DUAL) XIN
                          ON (XIN.DIVISION = SIK.CODDIV
                          AND XIN.CODUSR = SIK.CODUSR
                          AND XIN.SICKDATE = SIK.DTECRE)

                        WHEN MATCHED THEN
                          UPDATE SET SIK.POLICYCODE = VPOLICY_CODE
                        WHEN NOT MATCHED THEN
                          INSERT (CODDIV , CODUSR , DTECRE , POLICYCODE)
                          VALUES (VDIVISION ,VCODUSR , VSICK_DATE , VPOLICY_CODE );
                        COMMIT;

  END LOOP;
END;

    -- DELETE XIN TABLE
  DELETE FROM xin_excel_sick_leave;
  COMMIT;

EXCEPTION

  WHEN exsp THEN
        my_code := SQLCODE;
        my_errm := SQLERRM;

        INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
        VALUES (my_code ,my_errm , NULL );

        DELETE FROM XIN_EXCEL_SICK_LEAVE;
        COMMIT;

  WHEN OTHERS THEN
        my_code := SQLCODE;
        my_errm := SQLERRM;

        INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
        VALUES (my_code ,my_errm , NULL );

        DELETE FROM XIN_EXCEL_SICK_LEAVE;

        COMMIT;
    RAISE;

END SP_UPLOAD_SICK_LEAVE_C;

PROCEDURE SP_VALIDATE_SICK_LEAVE_C (VERRORCODE OUT VARCHAR2) AS

BEGIN
   VERRORCODE := 'KO';

   DELETE FROM XIN_EXCEL_SICK_LEAVE_LOG;
   COMMIT;


DECLARE CURSOR XIN IS
        SELECT * FROM XIN_EXCEL_SICK_LEAVE;

BEGIN
   FOR sick IN XIN
    LOOP
           VDIVISION := sick.DIVISION;
           VCODUSR := sick.CODUSR;
           VSICK_DATE := sick.SICK_LEAVE_DATE;
           VPOLICY_CODE := sick.SICK_CODE;



       --Null row key check
       IF(VDIVISION IS NULL OR VDIVISION = '') OR
         (VCODUSR IS NULL OR VCODUSR = '')OR
         (VSICK_DATE  IS NULL OR VSICK_DATE = '')
      THEN
        INSERT INTO XIN_EXCEL_SICK_LEAVE_LOG
        VALUES (VDIVISION,VCODUSR,VSICK_DATE,VPOLICY_CODE,'Null key found!',SYSDATE);
      END IF;


    --Duplicate row key check -
    INSERT INTO XIN_EXCEL_SICK_LEAVE_LOG
      SELECT excel.DIVISION,
             excel.CODUSR,
             excel.SICK_LEAVE_DATE,
             excel.SICK_CODE,
             'Duplicate ROWS',
             SYSDATE
      FROM (  SELECT S.DIVISION, S.CODUSR , S.SICK_LEAVE_DATE, S.SICK_CODE,
          ROW_NUMBER() OVER (PARTITION BY S.DIVISION, S.CODUSR , S.SICK_LEAVE_DATE ORDER BY ROWID ) AS Duplicated
          FROM XIN_EXCEL_SICK_LEAVE S) excel
      WHERE excel.Duplicated > 1 ;

    --Correct value check - DIVISONCODE
    SELECT COUNT(*)
       INTO VCOUNT
       FROM QTABS_C
       WHERE CODTAB = 'CDIV'
       AND COD = VDIVISION;

      IF (VCOUNT = 0 )
      THEN
         INSERT INTO XIN_EXCEL_SICK_LEAVE_LOG
         VALUES (VDIVISION,VCODUSR,VSICK_DATE,VPOLICY_CODE,'Wrong value for DIVISION CODE  '|| VDIVISION,SYSDATE);
       END IF;

    --Correct value check - CODUSR
    SELECT COUNT(*)
    INTO VCOUNT
    FROM T030USER
    WHERE CODUSR = VCODUSR;

    IF (VCOUNT = 0 )
      THEN
      INSERT INTO XIN_EXCEL_SICK_LEAVE_LOG
      VALUES (VDIVISION,VCODUSR,VSICK_DATE,VPOLICY_CODE,'Wrong value for CODUSR  '|| VCODUSR,SYSDATE);
     END IF;

    --Correct value check - CODUSR & CODDIV
      SELECT COUNT(*)
      INTO VCOUNT
      FROM T031USERDIV
      WHERE CODUSR = VCODUSR
      AND CODDIV = VDIVISION;

      IF (VCOUNT = 0 )
       THEN
        INSERT INTO XIN_EXCEL_SICK_LEAVE_LOG
        VALUES (VDIVISION,VCODUSR,VSICK_DATE,VPOLICY_CODE,'Wrong value for CODUSR & DIVISION ' || VCODUSR || ' ' || VDIVISION,SYSDATE);
      END IF;

    --Correct format date - SICK_LEAVE_DATE
    IF is_valid_date_format(VSICK_DATE) = 'FALSE'
      THEN
        INSERT INTO XIN_EXCEL_SICK_LEAVE_LOG
        VALUES (VDIVISION, VCODUSR, VSICK_DATE,VPOLICY_CODE, 'Wrong format date ' || VSICK_DATE, SYSDATE);
      END IF;


     --Correct valid date - SICK_LEAVE_DATE
     SELECT TO_CHAR(SYSDATE,'MM/YYYY')
     INTO VALID_YEAR_MONTH
     FROM DUAL;

     IF( TO_CHAR(TO_DATE(VSICK_DATE,'DD/MM/YYYY'),'MM/YYYY')  <>  VALID_YEAR_MONTH )
        THEN
          INSERT INTO XIN_EXCEL_SICK_LEAVE_LOG
          VALUES (VDIVISION, VCODUSR, VSICK_DATE,VPOLICY_CODE, 'Wrong value date ' || VSICK_DATE, SYSDATE);
     END IF;


      --Correct valid check - SICK_CODE POLICY
      SELECT COUNT(*)
       INTO VCOUNT
       FROM QTABS_C
       WHERE CODTAB = 'SICK_LEAVE_POLICY'
       AND COD = VPOLICY_CODE;

      IF (VCOUNT = 0 )
      THEN
         INSERT INTO XIN_EXCEL_SICK_LEAVE_LOG
         VALUES (VDIVISION,VCODUSR,VSICK_DATE,VPOLICY_CODE,'Wrong value for Policy code  '|| VPOLICY_CODE,SYSDATE);
      END IF;


      COMMIT;

      SELECT COUNT(*)
      INTO VCOUNT
      FROM XIN_EXCEL_SICK_LEAVE_LOG;

       IF VCOUNT = 0  -- correct data
       THEN
          VERRORCODE :='OK';  -- complete with success
        ELSE
          VERRORCODE :='KO';  -- ERRORS
          --CLREAR THE XIN TABLE
          DELETE FROM XIN_EXCEL_SICK_LEAVE;
          COMMIT;
       END IF;

   END LOOP;
   END;

   EXCEPTION
  WHEN OTHERS THEN
      my_code := SQLCODE;
      my_errm := SQLERRM;

      INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
      VALUES (my_code ,my_errm , '' );

      DELETE FROM XIN_EXCEL_SICK_LEAVE;

      COMMIT;

    RAISE;

END SP_VALIDATE_SICK_LEAVE_C;


PROCEDURE SP_UPLOAD_SUP_RATE_C (VERRORCODE OUT VARCHAR2 ) --VERRORCODE
AS


TYPE getData IS TABLE OF xin_excel_sup_rate%ROWTYPE;
getValue getData;


BEGIN

   --CALL VALIDATE PROCEDURE
   SP_VALIDATE_SUP_RATE_C (VERRORCODE) ;

    IF VERRORCODE = 'KO' -- wrong data uploaded
      THEN
           RAISE exsp;
    END IF;


    --data is correct & insert all records in historical table ..
    INSERT INTO XIN_EXCEL_SUP_RATE_HIST
    SELECT xin.* , SYSDATE
    FROM XIN_EXCEL_SUP_RATE xin;

    COMMIT;


BEGIN

  SELECT *
  BULK COLLECT INTO getValue
  FROM XIN_EXCEL_SUP_RATE;

FOR i IN getValue.FIRST..getValue.LAST
  LOOP

                 VDIVISION := getValue(i).DIVISION;
                 VCODUSR  :=  getValue(i).CODUSR;
                 VRATE_DATE := TO_DATE(getValue(i).RDATE,'DD/MM/YYYY');
                 VRATE := getValue(i).RATE;

                MERGE INTO TZ_SUP_RATE RATE
                USING ( SELECT getValue(i).DIVISION AS DIVISION,
                               getValue(i).CODUSR AS CODUSR,
                               TO_DATE(getValue(i).RDATE,'DD/MM/YYYY') AS RATEDATE,
                               getValue(i).RATE AS RATE
                        FROM DUAL) XIN
                        ON (XIN.DIVISION = RATE.DIVISION
                        AND XIN.CODUSR = RATE.CODUSR
                        AND XIN.RATEDATE = RATE.RDATE)

                      WHEN MATCHED THEN
                        UPDATE SET RATE.RATE = VRATE
                      WHEN NOT MATCHED THEN
                        INSERT (DIVISION , CODUSR , RDATE , RATE)
                        VALUES (VDIVISION ,VCODUSR , VRATE_DATE, VRATE );
                      COMMIT;

  END LOOP;

END;

  -- DELETE XIN TABLE
  DELETE FROM XIN_EXCEL_SUP_RATE;
  COMMIT;

EXCEPTION

  WHEN exsp THEN
        my_code := SQLCODE;
        my_errm := SQLERRM;

          INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
          VALUES (my_code ,my_errm , NULL );

          DELETE FROM XIN_EXCEL_SUP_RATE;

        COMMIT;

  WHEN OTHERS THEN
          my_code := SQLCODE;
          my_errm := SQLERRM;

          INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
          VALUES (my_code ,my_errm , NULL );

          DELETE FROM XIN_EXCEL_SUP_RATE;

        COMMIT;
    RAISE;

END SP_UPLOAD_SUP_RATE_C;

PROCEDURE SP_VALIDATE_SUP_RATE_C (VERRORCODE OUT VARCHAR2) AS

 BEGIN
   VERRORCODE := 'KO';

   DELETE FROM XIN_EXCEL_SUP_RATE_LOG;
   COMMIT;


DECLARE CURSOR XIN IS
        SELECT * FROM XIN_EXCEL_SUP_RATE;

BEGIN
   FOR rate IN XIN
    LOOP
           VDIVISION := rate.DIVISION;
           VCODUSR := rate.CODUSR;
           VRATE_DATE := rate.RDATE;
           VRATE := rate.RATE;


       --Null row key check
       IF(VDIVISION IS NULL OR VDIVISION = '') OR
         (VCODUSR IS NULL OR VCODUSR = '')OR
         (VRATE_DATE  IS NULL OR VRATE_DATE = '')
      THEN
        INSERT INTO XIN_EXCEL_SUP_RATE_LOG
        VALUES (VDIVISION,VCODUSR,VRATE_DATE,VRATE,'Null key found!',SYSDATE);
      END IF;


    --Duplicate row key check -
    INSERT INTO XIN_EXCEL_SUP_RATE_LOG
      SELECT excel.DIVISION,
             excel.CODUSR,
             excel.RDATE,
             excel.RATE,
             'Duplicate ROWS',
             SYSDATE
      FROM (  SELECT R.DIVISION, R.CODUSR , R.RDATE, R.RATE,
          ROW_NUMBER() OVER (PARTITION BY R.DIVISION, R.CODUSR , R.RDATE ORDER BY ROWID ) AS Duplicated
          FROM XIN_EXCEL_SUP_RATE R) excel
      WHERE excel.Duplicated > 1 ;

    --Correct value check - DIVISONCODE
    SELECT COUNT(*)
       INTO VCOUNT
       FROM QTABS_C
       WHERE CODTAB = 'CDIV'
       AND COD = VDIVISION;

      IF ( VCOUNT = 0 )
      THEN
         INSERT INTO XIN_EXCEL_SUP_RATE_LOG
         VALUES (VDIVISION,VCODUSR,VRATE_DATE,VRATE,'Wrong value for DIVISION CODE  '|| VDIVISION,SYSDATE);
       END IF;

    --Correct value check - CODUSR
    SELECT COUNT(*)
    INTO VCOUNT
    FROM T030USER
    WHERE CODUSR = VCODUSR;

    IF (VCOUNT = 0 )
      THEN
      INSERT INTO XIN_EXCEL_SUP_RATE_LOG
      VALUES (VDIVISION,VCODUSR,VRATE_DATE,VRATE,'Wrong value for CODUSR  '|| VCODUSR,SYSDATE);
     END IF;

    --Correct value check - CODUSR & CODDIV
      SELECT COUNT(*)
      INTO VCOUNT
      FROM T031USERDIV
      WHERE CODUSR = VCODUSR
      AND CODDIV = VDIVISION;

      IF ( VCOUNT = 0 )
       THEN
        INSERT INTO XIN_EXCEL_SUP_RATE_LOG
        VALUES (VDIVISION,VCODUSR,VRATE_DATE,VRATE,'Wrong value for CODUSR & DIVISION ' || VCODUSR || ' ' || VDIVISION,SYSDATE);
      END IF;


    --Correct format date - RATE DATE
     IF is_valid_date_format(VRATE_DATE) = 'FALSE'
      THEN
        INSERT INTO XIN_EXCEL_SUP_RATE_LOG
        VALUES (VDIVISION,VCODUSR,VRATE_DATE,VRATE, 'Wrong format date ' || VRATE_DATE, SYSDATE);
      END IF;


     --Correct valid date - RATE DATE
     SELECT TO_CHAR(SYSDATE,'MM/YYYY')
     INTO VALID_YEAR_MONTH
     FROM DUAL;

     IF( TO_CHAR(TO_DATE(VRATE_DATE,'DD/MM/YYYY'),'MM/YYYY')  <>  VALID_YEAR_MONTH )
        THEN
          INSERT INTO XIN_EXCEL_SUP_RATE_LOG
          VALUES (VDIVISION,VCODUSR,VRATE_DATE,VRATE, 'Wrong value date ' || VRATE_DATE, SYSDATE);
     END IF;


     --Correct rate number - RATE
     IF IS_NUMBER(VRATE) = 'N'
       THEN
         INSERT INTO XIN_EXCEL_SUP_RATE_LOG
         VALUES (VDIVISION, VCODUSR, VRATE_DATE,VRATE, 'Wrong value for the Rate  ' || VRATE, SYSDATE);
     END IF;

      COMMIT;

      SELECT COUNT(*)
      INTO VCOUNT
      FROM XIN_EXCEL_SUP_RATE_LOG;

      IF VCOUNT = 0  -- correct data
        THEN
          VERRORCODE :='OK';  -- complete with success
        ELSE
          VERRORCODE :='KO';  -- ERRORS
          --clear xin table for the next upload
          DELETE FROM XIN_EXCEL_SUP_RATE;
          COMMIT;
       END IF;

   END LOOP;
   END;

   EXCEPTION
       WHEN OTHERS THEN
            my_code := SQLCODE;
            my_errm := SQLERRM;

            INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
            VALUES (my_code ,my_errm , '' );

            DELETE FROM XIN_EXCEL_SUP_RATE;

            COMMIT;
        RAISE;

END SP_VALIDATE_SUP_RATE_C;

PROCEDURE SP_UPLOAD_B2B_TRG_C (VERRORCODE OUT VARCHAR2 ) --VERRORCODE
AS


TYPE getData IS TABLE OF xin_excel_b2b_trg%ROWTYPE;
getValue getData;


BEGIN
   --CALL VALIDATE PROCEDURE SP_VALIDATE_B2B_TRG_C
   SP_VALIDATE_B2B_TRG_C (VERRORCODE) ;

    IF VERRORCODE = 'KO' -- wrong data uploaded
      THEN
           RAISE exsp;
    END IF;

    --data is correct & insert all records in historical table ..
    INSERT INTO XIN_EXCEL_B2B_TRG_HIST
    SELECT XIN.* , SYSDATE
    FROM XIN_EXCEL_B2B_TRG XIN;

    COMMIT;

BEGIN

SELECT *
  BULK COLLECT INTO getValue
  FROM xin_excel_b2b_trg;

   FOR i IN getValue.FIRST..getValue.LAST
    LOOP

               VDIVISION := getValue(i).CODDIV;
               VCODUSR   := getValue(i).CODUSR;
               VTRG_DATE := TO_DATE(getValue(i).TRG_DATE,'DD/MM/YYYY');
               VCATEGORY := getValue(i).CATEGORY;
               VCOMP_TYPE := getValue(i).CATEGORY;
               VTARGET   := TO_NUMBER(LTRIM(getValue(i).TARGET,'0'));




            MERGE INTO TZ_TARGET TRG
            USING (SELECT getValue(i).CODDIV AS CODDIV,
                          getValue(i).CODUSR AS CODUSR,
                          TO_DATE(getValue(i).TRG_DATE,'DD/MM/YYYY') AS TRG_DATE,
                          getValue(i).CATEGORY AS CATEGORY,
                          getValue(i).COMP_TYPE AS COMP_TYPE,
                          TO_NUMBER(LTRIM(getValue(i).TARGET,'0')) AS TARGET

                   FROM DUAL)XIN
                   ON (XIN.CODDIV = TRG.CODDIV
                   AND XIN.CODUSR = TRG.CODUSR
                   AND XIN.TRG_DATE = TRG.TRG_DATE
                   AND XIN.CATEGORY = TRG.CATEGORY
                   AND XIN.COMP_TYPE = TRG.COMP_TYPE)
                   WHEN MATCHED THEN
                     UPDATE SET TRG.TARGET = VTARGET
                   WHEN NOT MATCHED THEN
                     INSERT (CODDIV , CODUSR , TRG_DATE , CATEGORY , COMP_TYPE , TARGET)
                     VALUES (VDIVISION , VCODUSR , VTRG_DATE, VCATEGORY , VCOMP_TYPE,VTARGET);

                   COMMIT;


  END LOOP;
  END;

  -- DELETE XIN TABLE
  DELETE FROM XIN_EXCEL_B2B_TRG;
  COMMIT;


EXCEPTION

  WHEN exsp THEN
          my_code := SQLCODE;
          my_errm := SQLERRM;

          INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
          VALUES (my_code ,my_errm , null );

          DELETE FROM XIN_EXCEL_B2B_TRG;

        COMMIT;

  WHEN OTHERS THEN
        my_code := SQLCODE;
        my_errm := SQLERRM;

        INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
        VALUES (my_code ,my_errm , null );

        DELETE FROM XIN_EXCEL_B2B_TRG;

      COMMIT;

    RAISE;

END SP_UPLOAD_B2B_TRG_C;

PROCEDURE SP_VALIDATE_B2B_TRG_C (VERRORCODE OUT VARCHAR2) AS

BEGIN


    DELETE FROM XIN_EXCEL_B2B_TRG_LOG;
    COMMIT;

   VERRORCODE := 'KO';

DECLARE CURSOR XIN IS
        SELECT * FROM XIN_EXCEL_B2B_TRG;

BEGIN
   FOR B2B IN XIN
    LOOP

       VDIVISION := B2B.CODDIV;
       VCODUSR := B2B.CODUSR;
       VTRG_DATE := B2B.TRG_DATE;
       VCATEGORY := B2B.CATEGORY;
       VCOMP_TYPE := B2B.COMP_TYPE;
       VTARGET := B2B.TARGET;


    --Null row key check
    IF  (VDIVISION IS NULL OR VDIVISION = '') OR
        (VCODUSR IS NULL OR VCODUSR = '')OR
        (VTRG_DATE  IS NULL OR VTRG_DATE = '')OR
        (VCATEGORY IS NULL OR VCATEGORY = '')OR
        (VCOMP_TYPE IS NULL OR VCOMP_TYPE = '')OR
        (VTARGET IS NULL OR VTARGET = '')
      THEN
      INSERT INTO XIN_EXCEL_B2B_TRG_LOG
      VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE, VTARGET , 'Null key found', SYSDATE );
     END IF;

    --Duplicate row key check -
    INSERT INTO XIN_EXCEL_B2B_TRG_LOG
      SELECT excel.CODDIV,
             excel.CODUSR,
             excel.TRG_DATE,
             excel.CATEGORY,
             excel.COMP_TYPE,
             excel.TARGET ,
             'Duplicate ROWS',
             SYSDATE
      FROM (  SELECT B.CODDIV, B.CODUSR , B.TRG_DATE, B.CATEGORY ,B.COMP_TYPE,B.TARGET,
          ROW_NUMBER() OVER (PARTITION BY B.CODDIV,B.CODUSR,B.TRG_DATE,B.CATEGORY,B.COMP_TYPE,B.TARGET ORDER BY ROWID ) AS Duplicated
          FROM XIN_EXCEL_B2B_TRG B) excel
      WHERE excel.Duplicated > 1 ;


       --Correct value check - CODDIV
       SELECT COUNT(*)
       INTO VCOUNT
       FROM QTABS_C
       WHERE CODTAB = 'CDIV'
       AND COD = VDIVISION;

    IF ( VCOUNT = 0 )
      THEN
      INSERT INTO XIN_EXCEL_B2B_TRG_LOG
      VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for DIVISION CODE  '|| VDIVISION, SYSDATE );
     END IF;


    --Correct value check - CODUSR
    SELECT COUNT(*)
    INTO VCOUNT
    FROM T030USER
    WHERE CODUSR = VCODUSR;

    IF ( VCOUNT = 0 )
      THEN
      INSERT INTO XIN_EXCEL_B2B_TRG_LOG
      VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for CODUSR '|| VCODUSR, SYSDATE );
     END IF;


      --Correct value check - CODUSR & CODDIV
      SELECT COUNT(*)
      INTO VCOUNT
      FROM T031USERDIV
      WHERE CODUSR = VCODUSR
      AND CODDIV = VDIVISION;

      IF ( VCOUNT = 0 )
       THEN
        INSERT INTO XIN_EXCEL_B2B_TRG_LOG
        VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for CODUSR & DIVISION ' || VCODUSR || ' ' || VDIVISION, SYSDATE );
      END IF;


     --Correct value check - CATEGORY
     SELECT COUNT(*)
     INTO VCOUNT
     FROM T060ARTICLE
     WHERE CODCATMER = VCATEGORY
     AND CODDIV = VDIVISION;

     IF ( VCOUNT = 0 )
       THEN
        INSERT INTO XIN_EXCEL_B2B_TRG_LOG
        VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for CATEGORY & CODDIV  ' || VCATEGORY, SYSDATE);
      END IF;


     --Correct format date - TRG_DATE
     IF is_valid_date_format(VTRG_DATE) = 'FALSE'
      THEN
        INSERT INTO XIN_EXCEL_B2B_TRG_LOG
        VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong format date ' || VTRG_DATE, SYSDATE);
      END IF;

     --Correct valid date - TRG_DATE
     SELECT TO_CHAR(SYSDATE,'MM/YYYY')
     INTO VALID_YEAR_MONTH
     FROM DUAL;

     IF( TO_CHAR(TO_DATE(VTRG_DATE,'DD/MM/YYYY'),'MM/YYYY')  <>  VALID_YEAR_MONTH )
      THEN
        INSERT INTO XIN_EXCEL_B2B_TRG_LOG
        VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for date ' || VTRG_DATE, SYSDATE);
     END IF;

      --Correct target number - TARGET
     IF IS_NUMBER(VTARGET) = 'N'
       THEN
         INSERT INTO XIN_EXCEL_B2B_TRG_LOG
         VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for the target  ' || VTARGET, SYSDATE);
     END IF;

      --Correct compensation type -
      IF (VCOMP_TYPE <> 'B2B')
         THEN
           INSERT INTO XIN_EXCEL_B2B_TRG_LOG
           VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for compensation type  ' || VCOMP_TYPE, SYSDATE);
     END IF;

       COMMIT; -- COMMIT


END LOOP;

    SELECT COUNT(*)
      INTO VCOUNT
      FROM XIN_EXCEL_B2B_TRG_LOG;

       IF VCOUNT = 0  -- correct data
         THEN
           VERRORCODE :='OK';  -- complete with success
         ELSE
           VERRORCODE :='KO';  -- ERRORS
          --CLEAR XIN TABLE FOR THE NEXT UPLOAD
          DELETE FROM XIN_EXCEL_B2B_TRG;
          COMMIT;

       END IF;

END;

  EXCEPTION
       WHEN OTHERS THEN
          my_code := SQLCODE;
          my_errm := SQLERRM;

            INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
            VALUES (my_code ,my_errm , '' );

            DELETE FROM XIN_EXCEL_B2B_TRG;

          COMMIT;

         RAISE;

END SP_VALIDATE_B2B_TRG_C;

PROCEDURE SP_UPLOAD_B2C_TRG_C (VERRORCODE OUT VARCHAR2 ) --VERRORCODE
AS

TYPE getData IS TABLE OF xin_excel_b2c_trg%ROWTYPE;
getValue getData;


BEGIN
   --CALL VALIDATE PROCEDURE SP_VALIDATE_B2C_TRG_C
   SP_VALIDATE_B2C_TRG_C (VERRORCODE) ;

    IF VERRORCODE = 'KO' -- wrong data uploaded
      THEN
           RAISE exsp;
    END IF;

    --data is correct & insert all records in historical table ..
    INSERT INTO XIN_EXCEL_B2C_TRG_HIST
    SELECT XIN.* , SYSDATE
    FROM XIN_EXCEL_B2C_TRG XIN;

    COMMIT;

BEGIN

SELECT *
  BULK COLLECT INTO getValue
  FROM xin_excel_b2c_trg;

   FOR i IN getValue.FIRST..getValue.LAST
    LOOP

               VDIVISION := getValue(i).CODDIV;
               VCODUSR   := getValue(i).CODUSR;
               VTRG_DATE := TO_DATE(getValue(i).TRG_DATE,'DD/MM/YYYY');
               VCATEGORY := getValue(i).CATEGORY;
               VCOMP_TYPE := getValue(i).CATEGORY;
               VTARGET   := TO_NUMBER(LTRIM(getValue(i).TARGET,'0'));




            MERGE INTO TZ_TARGET TRG
            USING (SELECT getValue(i).CODDIV AS CODDIV,
                          getValue(i).CODUSR AS CODUSR,
                          TO_DATE(getValue(i).TRG_DATE,'DD/MM/YYYY') AS TRG_DATE,
                          getValue(i).CATEGORY AS CATEGORY,
                          getValue(i).COMP_TYPE AS COMP_TYPE,
                          TO_NUMBER(LTRIM(getValue(i).TARGET,'0')) AS TARGET

                   FROM DUAL)XIN
                   ON (XIN.CODDIV = TRG.CODDIV
                   AND XIN.CODUSR = TRG.CODUSR
                   AND XIN.TRG_DATE = TRG.TRG_DATE
                   AND XIN.CATEGORY = TRG.CATEGORY
                   AND XIN.COMP_TYPE = TRG.COMP_TYPE)
                   WHEN MATCHED THEN
                     UPDATE SET TRG.TARGET = VTARGET
                   WHEN NOT MATCHED THEN
                     INSERT (CODDIV , CODUSR , TRG_DATE , CATEGORY , COMP_TYPE , TARGET)
                     VALUES (VDIVISION , VCODUSR , VTRG_DATE, VCATEGORY , VCOMP_TYPE,VTARGET);

                   COMMIT;


  END LOOP;
  END;

  -- DELETE XIN TABLE
  DELETE FROM XIN_EXCEL_B2C_TRG;
  COMMIT;


EXCEPTION

  WHEN exsp THEN
    my_code := SQLCODE;
    my_errm := SQLERRM;

        INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
        VALUES (my_code ,my_errm , null );

        DELETE FROM XIN_EXCEL_B2C_TRG;

        COMMIT;

  WHEN OTHERS THEN
    my_code := SQLCODE;
    my_errm := SQLERRM;

      INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
      VALUES (my_code ,my_errm , null );

      DELETE FROM XIN_EXCEL_B2C_TRG;

      COMMIT;

    RAISE;

END SP_UPLOAD_B2C_TRG_C;

PROCEDURE SP_VALIDATE_B2C_TRG_C (VERRORCODE OUT VARCHAR2) AS

BEGIN


    DELETE FROM XIN_EXCEL_B2C_TRG_LOG;
    COMMIT;

   VERRORCODE := 'KO';

DECLARE CURSOR XIN IS
        SELECT * FROM XIN_EXCEL_B2C_TRG;

BEGIN
   FOR B2C IN XIN
    LOOP

       VDIVISION := B2C.CODDIV;
       VCODUSR := B2C.CODUSR;
       VTRG_DATE := B2C.TRG_DATE;
       VCATEGORY := B2C.CATEGORY;
       VCOMP_TYPE := B2C.COMP_TYPE;
       VTARGET := B2C.TARGET;


    --Null row key check
    IF  (VDIVISION IS NULL OR VDIVISION = '') OR
        (VCODUSR IS NULL OR VCODUSR = '')OR
        (VTRG_DATE  IS NULL OR VTRG_DATE = '')OR
        (VCATEGORY IS NULL OR VCATEGORY = '')OR
        (VCOMP_TYPE IS NULL OR VCOMP_TYPE = '')OR
        (VTARGET IS NULL OR VTARGET = '')
      THEN
      INSERT INTO XIN_EXCEL_B2C_TRG_LOG
      VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE, VTARGET , 'Null key found', SYSDATE );
     END IF;

    --Duplicate row key check -
    INSERT INTO XIN_EXCEL_B2C_TRG_LOG
      SELECT excel.CODDIV,
             excel.CODUSR,
             excel.TRG_DATE,
             excel.CATEGORY,
             excel.COMP_TYPE,
             excel.TARGET ,
             'Duplicate ROWS',
             SYSDATE
      FROM (  SELECT B.CODDIV, B.CODUSR , B.TRG_DATE, B.CATEGORY ,B.COMP_TYPE,B.TARGET,
          ROW_NUMBER() OVER (PARTITION BY B.CODDIV,B.CODUSR,B.TRG_DATE,B.CATEGORY,B.COMP_TYPE,B.TARGET ORDER BY ROWID ) AS Duplicated
          FROM XIN_EXCEL_B2C_TRG B) excel
      WHERE excel.Duplicated > 1 ;


       --Correct value check - CODDIV
       SELECT COUNT(*)
       INTO VCOUNT
       FROM QTABS_C
       WHERE CODTAB = 'CDIV'
       AND COD = VDIVISION;

    IF (VCOUNT = 0 )
      THEN
      INSERT INTO XIN_EXCEL_B2C_TRG_LOG
      VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for DIVISION CODE  '|| VDIVISION, SYSDATE );
     END IF;


    --Correct value check - CODUSR
    SELECT COUNT(*)
    INTO VCOUNT
    FROM T030USER
    WHERE CODUSR = VCODUSR;

    IF ( VCOUNT = 0 )
      THEN
      INSERT INTO XIN_EXCEL_B2C_TRG_LOG
      VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for CODUSR '|| VCODUSR, SYSDATE );
     END IF;


      --Correct value check - CODUSR & CODDIV
      SELECT COUNT(*)
      INTO VCOUNT
      FROM T031USERDIV
      WHERE CODUSR = VCODUSR
      AND CODDIV = VDIVISION;

      IF ( VCOUNT = 0 )
       THEN
        INSERT INTO XIN_EXCEL_B2C_TRG_LOG
        VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for CODUSR & DIVISION ' || VCODUSR || ' ' || VDIVISION, SYSDATE );
      END IF;


     --Correct value check - CATEGORY
     SELECT COUNT(*)
     INTO VCOUNT
     FROM T060ARTICLE
     WHERE CODCATMER = VCATEGORY
     AND CODDIV = VDIVISION;

     IF ( VCOUNT = 0 )
       THEN
        INSERT INTO XIN_EXCEL_B2C_TRG_LOG
        VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for CATEGORY  ' || VCATEGORY, SYSDATE);
      END IF;


     --Correct format date - TRG_DATE
     IF is_valid_date_format(VTRG_DATE) = 'FALSE'
      THEN
        INSERT INTO XIN_EXCEL_B2C_TRG_LOG
        VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong format date ' || VTRG_DATE, SYSDATE);
      END IF;

     --Correct valid date - TRG_DATE
     SELECT TO_CHAR(SYSDATE,'MM/YYYY')
     INTO VALID_YEAR_MONTH
     FROM DUAL;

     IF( TO_CHAR(TO_DATE(VTRG_DATE,'DD/MM/YYYY'),'MM/YYYY')  <>  VALID_YEAR_MONTH )
      THEN
        INSERT INTO XIN_EXCEL_B2C_TRG_LOG
        VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for date ' || VTRG_DATE, SYSDATE);
     END IF;

      --Correct target number - TARGET
     IF IS_NUMBER(VTARGET) = 'N'
       THEN
         INSERT INTO XIN_EXCEL_B2C_TRG_LOG
         VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for the target  ' || VTARGET, SYSDATE);
     END IF;

      --Correct compensation type -
      IF (VCOMP_TYPE <> 'B2C')
         THEN
           INSERT INTO XIN_EXCEL_B2B_TRG_LOG
           VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for compensation type  ' || VCOMP_TYPE, SYSDATE);
     END IF;

       COMMIT; -- COMMIT


END LOOP;

    SELECT COUNT(*)
      INTO VCOUNT
      FROM XIN_EXCEL_B2C_TRG_LOG;

       IF VCOUNT = 0  -- correct data
         THEN
           VERRORCODE :='OK';  -- complete with success
         ELSE
           VERRORCODE :='KO';  -- ERRORS
          --clear the xin table for the next upload
          DELETE FROM XIN_EXCEL_B2C_TRG;
          COMMIT;

       END IF;

END;

  EXCEPTION
       WHEN OTHERS THEN
            my_code := SQLCODE;
            my_errm := SQLERRM;

            INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
            VALUES (my_code ,my_errm , '' );

            DELETE FROM XIN_EXCEL_B2C_TRG;
            COMMIT;

          RAISE;

END SP_VALIDATE_B2C_TRG_C;

PROCEDURE SP_UPLOAD_MERCH_TRG_C (VERRORCODE OUT VARCHAR2 ) --VERRORCODE
AS


TYPE getData IS TABLE OF xin_excel_merch_trg%ROWTYPE;
getValue getData;


BEGIN
   --CALL VALIDATE PROCEDURE SP_VALIDATE_MERCH_TRG_C
   SP_VALIDATE_MERCH_TRG_C (VERRORCODE) ;

    IF VERRORCODE = 'KO' -- wrong data uploaded
      THEN
           RAISE exsp;
    END IF;

    --data is correct & insert all records in historical table ..
    INSERT INTO XIN_EXCEL_MERCH_TRG_HIST
    SELECT XIN.* , SYSDATE
    FROM XIN_EXCEL_MERCH_TRG XIN;

    COMMIT;

BEGIN

SELECT *
  BULK COLLECT INTO getValue
  FROM xin_excel_merch_trg;

   FOR i IN getValue.FIRST..getValue.LAST
    LOOP

               VDIVISION := getValue(i).CODDIV;
               VCODUSR   := getValue(i).CODUSR;
               VTRG_DATE := TO_DATE(getValue(i).TRG_DATE,'DD/MM/YYYY');
               VCATEGORY := getValue(i).CATEGORY;
               VCOMP_TYPE := getValue(i).CATEGORY;
               VTARGET   := TO_NUMBER(LTRIM(getValue(i).TARGET,'0'));




            MERGE INTO TZ_TARGET TRG
            USING (SELECT getValue(i).CODDIV AS CODDIV,
                          getValue(i).CODUSR AS CODUSR,
                          TO_DATE(getValue(i).TRG_DATE,'DD/MM/YYYY') AS TRG_DATE,
                          getValue(i).CATEGORY AS CATEGORY,
                          getValue(i).COMP_TYPE AS COMP_TYPE,
                          TO_NUMBER(LTRIM(getValue(i).TARGET,'0')) AS TARGET

                   FROM DUAL)XIN
                   ON (XIN.CODDIV = TRG.CODDIV
                   AND XIN.CODUSR = TRG.CODUSR
                   AND XIN.TRG_DATE = TRG.TRG_DATE
                   AND XIN.CATEGORY = TRG.CATEGORY
                   AND XIN.COMP_TYPE = TRG.COMP_TYPE)
                   WHEN MATCHED THEN
                     UPDATE SET TRG.TARGET = VTARGET
                   WHEN NOT MATCHED THEN
                     INSERT (CODDIV , CODUSR , TRG_DATE , CATEGORY , COMP_TYPE , TARGET)
                     VALUES (VDIVISION , VCODUSR , VTRG_DATE, VCATEGORY , VCOMP_TYPE,VTARGET);

                   COMMIT;


  END LOOP;
  END;

  -- DELETE XIN TABLE
  DELETE FROM XIN_EXCEL_MERCH_TRG;
  COMMIT;


EXCEPTION

  WHEN exsp THEN
    my_code := SQLCODE;
    my_errm := SQLERRM;

          INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
          VALUES (my_code ,my_errm , null );

          DELETE FROM XIN_EXCEL_MERCH_TRG;

        COMMIT;

  WHEN OTHERS THEN
      my_code := SQLCODE;
      my_errm := SQLERRM;

      INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
      VALUES (my_code ,my_errm , null );

      DELETE FROM XIN_EXCEL_MERCH_TRG;
    COMMIT;

    RAISE;

END SP_UPLOAD_MERCH_TRG_C;

PROCEDURE SP_VALIDATE_MERCH_TRG_C (VERRORCODE OUT VARCHAR2) AS


 BEGIN


    DELETE FROM XIN_EXCEL_MERCH_TRG_LOG;
    COMMIT;

   VERRORCODE := 'KO';

DECLARE CURSOR XIN IS
        SELECT * FROM XIN_EXCEL_MERCH_TRG;

BEGIN
   FOR MERCH IN XIN
    LOOP

           VDIVISION  := MERCH.CODDIV;
           VCODUSR    := MERCH.CODUSR;
           VTRG_DATE  := MERCH.TRG_DATE;
           VCATEGORY  := MERCH.CATEGORY;
           VCOMP_TYPE := MERCH.COMP_TYPE;
           VTARGET    := MERCH.TARGET;


    --Null row key check
    IF  (VDIVISION IS NULL OR VDIVISION = '') OR
        (VCODUSR IS NULL OR VCODUSR = '')OR
        (VTRG_DATE  IS NULL OR VTRG_DATE = '')OR
        (VCATEGORY IS NULL OR VCATEGORY = '')OR
        (VCOMP_TYPE IS NULL OR VCOMP_TYPE = '')OR
        (VTARGET IS NULL OR VTARGET = '')
      THEN
        INSERT INTO XIN_EXCEL_MERCH_TRG_LOG
        VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE, VTARGET , 'Null key found', SYSDATE );
     END IF;

    --Duplicate row key check -
    INSERT INTO XIN_EXCEL_MERCH_TRG_LOG
      SELECT excel.CODDIV,
             excel.CODUSR,
             excel.TRG_DATE,
             excel.CATEGORY,
             excel.COMP_TYPE,
             excel.TARGET ,
             'Duplicate ROWS',
             SYSDATE
      FROM (  SELECT B.CODDIV, B.CODUSR , B.TRG_DATE, B.CATEGORY ,B.COMP_TYPE,B.TARGET,
          ROW_NUMBER() OVER (PARTITION BY B.CODDIV,B.CODUSR,B.TRG_DATE,B.CATEGORY,B.COMP_TYPE,B.TARGET ORDER BY ROWID ) AS Duplicated
          FROM XIN_EXCEL_MERCH_TRG B) excel
      WHERE excel.Duplicated > 1 ;


       --Correct value check - CODDIV
       SELECT COUNT(*)
       INTO VCOUNT
       FROM QTABS_C
       WHERE CODTAB = 'CDIV'
       AND COD = VDIVISION;

    IF (VCOUNT = 0 )
      THEN
      INSERT INTO XIN_EXCEL_MERCH_TRG_LOG
      VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for DIVISION CODE  '|| VDIVISION, SYSDATE );
     END IF;


    --Correct value check - CODUSR
    SELECT COUNT(*)
    INTO VCOUNT
    FROM T030USER
    WHERE CODUSR = VCODUSR;

    IF ( VCOUNT = 0 )
      THEN
      INSERT INTO XIN_EXCEL_MERCH_TRG_LOG
      VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for CODUSR '|| VCODUSR, SYSDATE );
     END IF;


      --Correct value check - CODUSR & CODDIV
      SELECT COUNT(*)
      INTO VCOUNT
      FROM T031USERDIV
      WHERE CODUSR = VCODUSR
      AND CODDIV = VDIVISION;

      IF ( VCOUNT = 0 )
       THEN
        INSERT INTO XIN_EXCEL_MERCH_TRG_LOG
        VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for CODUSR & DIVISION ' || VCODUSR || ' ' || VDIVISION, SYSDATE );
      END IF;


     --Correct value check - CATEGORY
     SELECT COUNT(*)
     INTO VCOUNT
     FROM T060ARTICLE
     WHERE CODCATMER = VCATEGORY
     AND CODDIV = VDIVISION;

     IF ( VCOUNT = 0 )
       THEN
        INSERT INTO XIN_EXCEL_MERCH_TRG_LOG
        VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for CATEGORY  ' || VCATEGORY, SYSDATE);
      END IF;


     --Correct format date - TRG_DATE
     IF is_valid_date_format(VTRG_DATE) = 'FALSE'
      THEN
        INSERT INTO XIN_EXCEL_MERCH_TRG_LOG
        VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong format date ' || VTRG_DATE, SYSDATE);
      END IF;

     --Correct valid date - TRG_DATE
     SELECT TO_CHAR(SYSDATE,'MM/YYYY')
     INTO VALID_YEAR_MONTH
     FROM DUAL;

     IF( TO_CHAR(TO_DATE(VTRG_DATE,'DD/MM/YYYY'),'MM/YYYY')  <>  VALID_YEAR_MONTH )
      THEN
        INSERT INTO XIN_EXCEL_MERCH_TRG_LOG
        VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for date ' || VTRG_DATE, SYSDATE);
     END IF;

      --Correct target number - TARGET
     IF IS_NUMBER(VTARGET) = 'N'
       THEN
         INSERT INTO XIN_EXCEL_MERCH_TRG_LOG
         VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for the target  ' || VTARGET, SYSDATE);
     END IF;

      --Correct compensation type -
      IF (VCOMP_TYPE <> 'MERCH')
         THEN
           INSERT INTO XIN_EXCEL_MERCH_TRG_LOG
           VALUES (VDIVISION, VCODUSR, VTRG_DATE , VCATEGORY ,VCOMP_TYPE , VTARGET, 'Wrong value for compensation type  ' || VCOMP_TYPE, SYSDATE);
     END IF;

       COMMIT; -- COMMIT


END LOOP;

    SELECT COUNT(*)
      INTO VCOUNT
      FROM XIN_EXCEL_MERCH_TRG_LOG;

       IF VCOUNT = 0  -- correct data
       THEN
          VERRORCODE :='OK';  -- complete with success
        ELSE
          VERRORCODE :='KO';  -- ERRORS
          --clear xin table for the next upload
          DELETE FROM XIN_EXCEL_MERCH_TRG;
          COMMIT;
       END IF;

END;

  EXCEPTION
       WHEN OTHERS THEN
          my_code := SQLCODE;
          my_errm := SQLERRM;

            INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
            VALUES (my_code ,my_errm , '' );

            DELETE FROM XIN_EXCEL_MERCH_TRG;

          COMMIT;

        RAISE;

END SP_VALIDATE_MERCH_TRG_C;

PROCEDURE SP_UPLOAD_SA_TRG_C (VERRORCODE OUT VARCHAR2 ) --VERRORCODE
AS


TYPE getData IS TABLE OF xin_excel_sa_trg%ROWTYPE;
getValue getData;


BEGIN
   --CALL VALIDATE PROCEDURE SP_VALIDATE_SA_TRG_C
   SP_VALIDATE_SA_TRG_C (VERRORCODE) ;

    IF VERRORCODE = 'KO' -- wrong data uploaded
      THEN
           RAISE exsp;
    END IF;

    --data is correct & insert all records in historical table ..
    INSERT INTO XIN_EXCEL_SA_TRG_HIST
    SELECT XIN.* , SYSDATE
    FROM XIN_EXCEL_SA_TRG XIN;

    COMMIT;

BEGIN

SELECT *
  BULK COLLECT INTO getValue
  FROM xin_excel_sa_trg;

   FOR i IN getValue.FIRST..getValue.LAST
    LOOP

               VDIVISION := getValue(i).CODDIV;
               VCODUSR   := getValue(i).CODUSR;
               VTRG_DATE := TO_DATE(getValue(i).TRG_DATE,'DD/MM/YYYY');
               VCOMP_TYPE := getValue(i).COMP_TYPE;
               VTARGET   := TO_NUMBER(LTRIM(getValue(i).TARGET,'0'));


            MERGE INTO TZ_TARGET TRG
            USING (SELECT getValue(i).CODDIV AS CODDIV,
                          getValue(i).CODUSR AS CODUSR,
                          TO_DATE(getValue(i).TRG_DATE,'DD/MM/YYYY') AS TRG_DATE,
                          getValue(i).COMP_TYPE AS COMP_TYPE,
                          TO_NUMBER(LTRIM(getValue(i).TARGET,'0')) AS TARGET

                   FROM DUAL)XIN
                   ON (XIN.CODDIV = TRG.CODDIV
                   AND XIN.CODUSR = TRG.CODUSR
                   AND XIN.TRG_DATE = TRG.TRG_DATE
                   AND XIN.COMP_TYPE = TRG.COMP_TYPE)
                   WHEN MATCHED THEN
                     UPDATE SET TRG.TARGET = VTARGET
                   WHEN NOT MATCHED THEN
                     INSERT (CODDIV , CODUSR , TRG_DATE , CATEGORY , COMP_TYPE , TARGET)
                     VALUES (VDIVISION , VCODUSR , VTRG_DATE, null , VCOMP_TYPE,VTARGET);

                   COMMIT;


  END LOOP;
  END;

  -- DELETE XIN TABLE
  DELETE FROM XIN_EXCEL_SA_TRG;
  COMMIT;


EXCEPTION

  WHEN exsp THEN
          my_code := SQLCODE;
          my_errm := SQLERRM;

          INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
          VALUES (my_code ,my_errm , null );

          DELETE FROM XIN_EXCEL_SA_TRG;

        COMMIT;

  WHEN OTHERS THEN
        my_code := SQLCODE;
        my_errm := SQLERRM;

        INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
        VALUES (my_code ,my_errm , null );

        DELETE FROM XIN_EXCEL_SA_TRG;

      COMMIT;

    RAISE;

END SP_UPLOAD_SA_TRG_C;

PROCEDURE SP_VALIDATE_SA_TRG_C (VERRORCODE OUT VARCHAR2) AS

BEGIN


    DELETE FROM XIN_EXCEL_SA_TRG_LOG;
    COMMIT;

   VERRORCODE := 'KO';

DECLARE CURSOR XIN IS
        SELECT * FROM XIN_EXCEL_SA_TRG;

BEGIN
   FOR SA IN XIN
    LOOP

       VDIVISION := SA.CODDIV;
       VCODUSR := SA.CODUSR;
       VTRG_DATE := SA.TRG_DATE;
       VCOMP_TYPE := SA.COMP_TYPE;
       VTARGET := SA.TARGET;


    --Null row key check
    IF  (VDIVISION IS NULL OR VDIVISION = '') OR
        (VCODUSR IS NULL OR VCODUSR = '')OR
        (VTRG_DATE  IS NULL OR VTRG_DATE = '')OR
        (VCOMP_TYPE IS NULL OR VCOMP_TYPE = '')OR
        (VTARGET IS NULL OR VTARGET = '')
      THEN
        INSERT INTO XIN_EXCEL_SA_TRG_LOG
        VALUES (VDIVISION, VCODUSR, VTRG_DATE,VCOMP_TYPE, VTARGET , 'Null key found', SYSDATE );
     END IF;

    --Duplicate row key check -
    INSERT INTO XIN_EXCEL_SA_TRG_LOG
      SELECT excel.CODDIV,
             excel.CODUSR,
             excel.TRG_DATE,
             excel.COMP_TYPE,
             excel.TARGET ,
             'Duplicate ROWS',
             SYSDATE
      FROM (  SELECT B.CODDIV, B.CODUSR , B.TRG_DATE,B.COMP_TYPE,B.TARGET,
          ROW_NUMBER() OVER (PARTITION BY B.CODDIV,B.CODUSR,B.TRG_DATE,B.COMP_TYPE,B.TARGET ORDER BY ROWID ) AS Duplicated
          FROM XIN_EXCEL_SA_TRG B) excel
      WHERE excel.Duplicated > 1 ;


       --Correct value check - CODDIV
       SELECT COUNT(*)
       INTO VCOUNT
       FROM QTABS_C
       WHERE CODTAB = 'CDIV'
       AND COD = VDIVISION;

    IF (VCOUNT = 0 )
      THEN
      INSERT INTO XIN_EXCEL_SA_TRG_LOG
      VALUES (VDIVISION, VCODUSR, VTRG_DATE ,VCOMP_TYPE , VTARGET, 'Wrong value for DIVISION CODE  '|| VDIVISION, SYSDATE );
     END IF;


    --Correct value check - CODUSR
    SELECT COUNT(*)
    INTO VCOUNT
    FROM T030USER
    WHERE CODUSR = VCODUSR;

    IF ( VCOUNT = 0 )
      THEN
      INSERT INTO XIN_EXCEL_SA_TRG_LOG
      VALUES (VDIVISION, VCODUSR, VTRG_DATE ,VCOMP_TYPE , VTARGET, 'Wrong value for CODUSR '|| VCODUSR, SYSDATE );
     END IF;


      --Correct value check - CODUSR & CODDIV
      SELECT COUNT(*)
      INTO VCOUNT
      FROM T031USERDIV
      WHERE CODUSR = VCODUSR
      AND CODDIV = VDIVISION;

      IF ( VCOUNT = 0 )
       THEN
        INSERT INTO XIN_EXCEL_SA_TRG_LOG
        VALUES (VDIVISION, VCODUSR, VTRG_DATE ,VCOMP_TYPE , VTARGET, 'Wrong value for CODUSR & DIVISION ' || VCODUSR || ' ' || VDIVISION, SYSDATE );
      END IF;


     --Correct format date - TRG_DATE
     IF is_valid_date_format(VTRG_DATE) = 'FALSE'
      THEN
        INSERT INTO XIN_EXCEL_SA_TRG_LOG
        VALUES (VDIVISION, VCODUSR, VTRG_DATE ,VCOMP_TYPE , VTARGET, 'Wrong format date ' || VTRG_DATE, SYSDATE);
      END IF;

     --Correct valid date - TRG_DATE
     SELECT TO_CHAR(SYSDATE,'MM/YYYY')
     INTO VALID_YEAR_MONTH
     FROM DUAL;

     IF( TO_CHAR(TO_DATE(VTRG_DATE,'DD/MM/YYYY'),'MM/YYYY')  <>  VALID_YEAR_MONTH )
      THEN
        INSERT INTO XIN_EXCEL_SA_TRG_LOG
        VALUES (VDIVISION, VCODUSR, VTRG_DATE,VCOMP_TYPE, VTARGET, 'Wrong value for date ' || VTRG_DATE, SYSDATE);
     END IF;

      --Correct target number - TARGET
     IF IS_NUMBER(VTARGET) = 'N'
       THEN
         INSERT INTO XIN_EXCEL_SA_TRG_LOG
         VALUES (VDIVISION, VCODUSR, VTRG_DATE,VCOMP_TYPE, VTARGET, 'Wrong value for the target  ' || VTARGET, SYSDATE);
     END IF;

      --Correct compensation type -
      IF (VCOMP_TYPE <> 'SA')
         THEN
           INSERT INTO XIN_EXCEL_SA_TRG_LOG
           VALUES (VDIVISION, VCODUSR, VTRG_DATE,VCOMP_TYPE, VTARGET, 'Wrong value for compensation type  ' || VCOMP_TYPE, SYSDATE);
     END IF;

       COMMIT; -- COMMIT


END LOOP;

    SELECT COUNT(*)
      INTO VCOUNT
      FROM XIN_EXCEL_SA_TRG_LOG;

       IF VCOUNT = 0  -- correct data
       THEN
          VERRORCODE :='OK';  -- complete with success
        ELSE
          VERRORCODE :='KO';  -- ERRORS
          --clear the xin table for the next upload
          DELETE FROM XIN_EXCEL_SA_TRG;
          COMMIT;
       END IF;

END;

  EXCEPTION
       WHEN OTHERS THEN
            my_code := SQLCODE;
            my_errm := SQLERRM;

            INSERT INTO TEST_LOG_DAL (LOG1,LOG2,LOG3)
            VALUES (my_code ,my_errm , '' );

            DELETE FROM XIN_EXCEL_SA_TRG;

            COMMIT;

          RAISE;

END SP_VALIDATE_SA_TRG_C;

END PKG_EXCEL_UPLOAD;
