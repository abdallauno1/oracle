/*
ESEMPIO MOLTO UTILE ....
http://nimishgarg.blogspot.com.es/2013/05/ora-01422-exact-fetch-returns-more-than.html
*/
----------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE TZ_IMPORT
AS
CURSOR XIN IS
          SELECT XIN_TZ_TA7091PAYMENTS.NUMORD AS NUMORD,
                 XIN_TZ_TA7091PAYMENTS.CLEARINGDATE AS DATAPAG
                FROM XIN_TZ_TA7091PAYMENTS  ;
CURSOR TZ IS
          SELECT  T660CONT_ATT.CODTYPORD AS CODTYPORD ,
                  T660CONT_ATT.CODDIV AS CODDIV
                FROM T660CONT_ATT
                JOIN XIN_TZ_TA7091PAYMENTS
                ON T660CONT_ATT.NUMORD = XIN_TZ_TA7091PAYMENTS.NUMORD
                AND T660CONT_ATT.CODDIV = '4770';

NUMORD NUMBER(12,0);
DATAPAG DATE ;
CODTYPORD VARCHAR2(255);
CODDIV VARCHAR2(255);

BEGIN
    OPEN XIN ;
    OPEN TZ;

  LOOP

    EXIT WHEN (XIN%NOTFOUND);
    EXIT WHEN (TZ%NOTFOUND);

    FETCH XIN INTO NUMORD ,DATAPAG ;
    FETCH TZ INTO CODTYPORD ,CODDIV ;

    INSERT INTO TZ_TA7091PAYMENTS( NUMORD ,DATAPAG ,STATOPAG ,  TIPODOC , CODDIV , PAYMENT_KEY   )
                            VALUES(NUMORD ,DATAPAG ,'PAG' ,  CODTYPORD ,CODDIV ,NUMORD ||  CODTYPORD) ;
      COMMIT;
  END LOOP;

        CLOSE XIN;
        CLOSE TZ;

  EXCEPTION WHEN OTHERS then
      ROLLBACK;

 END;
 /
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 CREATE OR REPLACE PROCEDURE TZ_IMPORT
AS
CURSOR XIN IS
          SELECT XIN_TZ_TA7091PAYMENTS.NUMORD AS NUMORD ,
                 XIN_TZ_TA7091PAYMENTS.FLG_RIBA AS FLG_RIBA ,

                 XIN_TZ_TA7091PAYMENTS.CLEARINGDATE AS DATAPAG ,
                 XIN_TZ_TA7091PAYMENTS.ENTRYDATE AS ENTRYDATE ,

                 TO_CHAR(TO_DATE(XIN_TZ_TA7091PAYMENTS.CLEARINGDATE,'DD/MM/YYYYHH24:mi:ss'), 'MM') AS CLE_MONTH ,
                 TO_CHAR(TO_DATE(XIN_TZ_TA7091PAYMENTS.ENTRYDATE,'DD/MM/YYYYHH24:mi:ss'), 'MM') AS ENT_MONTH
                FROM XIN_TZ_TA7091PAYMENTS  ;
CURSOR TZ IS
          SELECT  T660CONT_ATT.CODTYPORD AS CODTYPORD ,
                  T660CONT_ATT.CODDIV AS CODDIV
                FROM T660CONT_ATT
                JOIN XIN_TZ_TA7091PAYMENTS
                ON T660CONT_ATT.NUMORD = XIN_TZ_TA7091PAYMENTS.NUMORD
                AND T660CONT_ATT.CODDIV = '4770';

NUMORD NUMBER(12,0);
FLG_RIBA NUMBER(1);
DATAPAG DATE ;
ENTRYDATE DATE;
CLE_MONTH NUMBER(2);
ENT_MONTH NUMBER (2);
MESE NUMBER(2) := 10;



CODTYPORD VARCHAR2(255);
CODDIV VARCHAR2(255);

BEGIN
    OPEN XIN ;
    OPEN TZ;

  LOOP

    EXIT WHEN (XIN%NOTFOUND);
    EXIT WHEN (TZ%NOTFOUND);

    FETCH XIN INTO NUMORD , FLG_RIBA , DATAPAG , ENTRYDATE ,  CLE_MONTH , ENT_MONTH  ;
    FETCH TZ INTO CODTYPORD ,CODDIV ;

      IF
         FLG_RIBA <> -1  OR FLG_RIBA IS NULL THEN DATAPAG := DATAPAG;

      END IF;

    INSERT INTO TZ_TA7091PAYMENTS( NUMORD ,DATAPAG ,STATOPAG ,  TIPODOC , CODDIV , PAYMENT_KEY   )
                            VALUES(NUMORD ,DATAPAG ,'PAG' ,  CODTYPORD ,CODDIV ,NUMORD ||  CODTYPORD) ;
      COMMIT;
  END LOOP;

        CLOSE XIN;
        CLOSE TZ;

  EXCEPTION WHEN OTHERS then
      ROLLBACK;

 END;
 /
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


 CREATE OR REPLACE PROCEDURE TZ_IMPORT
AS
CURSOR XIN IS
          SELECT XIN_TZ_TA7091PAYMENTS.NUMORD AS NUMORD ,
                 XIN_TZ_TA7091PAYMENTS.FLG_RIBA AS FLG_RIBA ,

                 XIN_TZ_TA7091PAYMENTS.CLEARINGDATE AS CLEARINGDATE ,
                 XIN_TZ_TA7091PAYMENTS.ENTRYDATE AS ENTRYDATE ,

                 TO_CHAR(TO_DATE(XIN_TZ_TA7091PAYMENTS.CLEARINGDATE,'DD/MM/YYYYHH24:mi:ss'), 'MM') AS CLE_MONTH ,
                 TO_CHAR(TO_DATE(XIN_TZ_TA7091PAYMENTS.ENTRYDATE,'DD/MM/YYYYHH24:mi:ss'), 'MM') AS ENT_MONTH
                FROM XIN_TZ_TA7091PAYMENTS  ;
CURSOR TZ IS
          SELECT  T660CONT_ATT.CODTYPORD AS CODTYPORD ,
                  T660CONT_ATT.CODDIV AS CODDIV
                FROM T660CONT_ATT
                JOIN XIN_TZ_TA7091PAYMENTS
                ON T660CONT_ATT.NUMORD = XIN_TZ_TA7091PAYMENTS.NUMORD
                AND T660CONT_ATT.CODDIV = '4770';

NUMORD NUMBER(12,0);
FLG_RIBA NUMBER(1);
DATAPAG DATE ;
ENTRYDATE DATE;
CLE_MONTH NUMBER(2);
ENT_MONTH NUMBER (2);
MESE NUMBER(2) := 10;

CODTYPORD VARCHAR2(255);
CODDIV VARCHAR2(255);

BEGIN
    OPEN XIN ;
    OPEN TZ;

  LOOP

    EXIT WHEN (XIN%NOTFOUND);
    EXIT WHEN (TZ%NOTFOUND);

    FETCH XIN INTO NUMORD , FLG_RIBA , DATAPAG , ENTRYDATE ,  CLE_MONTH , ENT_MONTH  ;
    FETCH TZ INTO CODTYPORD ,CODDIV ;

      IF
         FLG_RIBA <> -1  OR FLG_RIBA IS NULL
              THEN DATAPAG := DATAPAG;
      ELSIF
         FLG_RIBA = -1  AND  ENT_MONTH >= CLE_MONTH
              THEN DATAPAG := ENTRYDATE;
      ELSIF
         FLG_RIBA = -1  AND ENT_MONTH < CLE_MONTH
              THEN DATAPAG := DATAPAG;
      END IF;


    INSERT INTO TZ_TA7091PAYMENTS( NUMORD ,DATAPAG ,STATOPAG ,  TIPODOC , CODDIV , PAYMENT_KEY   )
                            VALUES(NUMORD ,DATAPAG ,'PAG' ,  CODTYPORD ,CODDIV ,NUMORD ||  CODTYPORD) ;
      COMMIT;

  END LOOP;

        CLOSE XIN;
        CLOSE TZ;

  EXCEPTION WHEN OTHERS then
      ROLLBACK;

 END;
 /
-------------------------------------------------------------------------------------------------------------------------------------------------------------------

 CREATE OR REPLACE PROCEDURE TZ_IMPORT
AS
CURSOR XIN IS
          SELECT XIN_TZ.NUMORD AS NUMORD ,
                 XIN_TZ.FLG_RIBA AS FLG_RIBA ,

                 XIN_TZ.CLEARINGDATE AS CLEARINGDATE ,
                 XIN_TZ.ENTRYDATE AS ENTRYDATE ,

                 TO_CHAR(TO_DATE(XIN_TZ.CLEARINGDATE,'DD/MM/YYYYHH24:mi:ss'), 'MM') AS CLE_MONTH ,
                 TO_CHAR(TO_DATE(XIN_TZ.ENTRYDATE,'DD/MM/YYYYHH24:mi:ss'), 'MM') AS ENT_MONTH
                FROM XIN_TZ_TA7091PAYMENTS  XIN_TZ;

NUMORD NUMBER(12,0);
FLG_RIBA NUMBER(1);
DATAPAG DATE ;
ENTRYDATE DATE;
CLE_MONTH NUMBER(2);
ENT_MONTH NUMBER (2);


V_CODTYPORD VARCHAR2(255);
V_CODDIV VARCHAR2(255);
V_PAYMENT_KEY VARCHAR2(255);

BEGIN

  FOR C IN (SELECT DISTINCT  ATT.CODTYPORD  ,ATT.CODDIV , ATT.PAYMENT_KEY
              INTO V_CODTYPORD , V_CODDIV , V_PAYMENT_KEY
              FROM   T660CONT_ATT ATT
              WHERE CODDIV = '4770'
              AND NUMORD = ATT.NUMORD)
         LOOP
               V_CODTYPORD := C.CODTYPORD;
               V_CODDIV := C.CODDIV;
               V_PAYMENT_KEY := C.PAYMENT_KEY;
         END LOOP;

    OPEN XIN ;

  LOOP

    EXIT WHEN (XIN%NOTFOUND);

    FETCH XIN INTO NUMORD , FLG_RIBA , DATAPAG , ENTRYDATE ,  CLE_MONTH , ENT_MONTH  ;

      IF
         FLG_RIBA <> -1  OR FLG_RIBA IS NULL
              THEN DATAPAG := DATAPAG;
      ELSIF
         FLG_RIBA = -1  AND  ENT_MONTH >= CLE_MONTH
              THEN DATAPAG := ENTRYDATE;
      ELSIF
         FLG_RIBA = -1  AND ENT_MONTH < CLE_MONTH
              THEN DATAPAG := DATAPAG;
      END IF;


    INSERT INTO TZ_TA7091PAYMENTS( NUMORD ,DATAPAG ,STATOPAG ,  TIPODOC , CODDIV , PAYMENT_KEY   )
                            VALUES(NUMORD ,DATAPAG ,'PAG' ,  V_CODTYPORD ,V_CODDIV ,V_PAYMENT_KEY) ;
      COMMIT;

  END LOOP;

        CLOSE XIN;


  EXCEPTION WHEN OTHERS THEN
      ROLLBACK;
 -- WHEN TOO_MANY_ROWS THEN NULL;
 END;
 /
-----------------------------------------------------------------------------------------------------------------------------

 CREATE OR REPLACE PROCEDURE TZ_IMPORT
AS

V_NUMORD NUMBER(12,0);
V_FLG_RIBA NUMBER(1);
V_DATAPAG DATE ;
V_ENTRYDATE DATE;
V_CLE_MONTH NUMBER(2);
V_ENT_MONTH NUMBER (2);


V_CODTYPORD VARCHAR2(255);
V_CODDIV VARCHAR2(255);
V_PAYMENT_KEY VARCHAR2(255);

BEGIN

  FOR I IN (SELECT XIN_TZ.NUMORD AS NUMORD ,
                 XIN_TZ.FLG_RIBA AS FLG_RIBA ,
                 XIN_TZ.CLEARINGDATE AS CLEARINGDATE ,
                 XIN_TZ.ENTRYDATE AS ENTRYDATE ,
                 TO_CHAR(TO_DATE(XIN_TZ.CLEARINGDATE,'DD/MM/YYYYHH24:mi:ss'), 'MM') AS CLE_MONTH ,
                 TO_CHAR(TO_DATE(XIN_TZ.ENTRYDATE,'DD/MM/YYYYHH24:mi:ss'), 'MM') AS ENT_MONTH
                FROM XIN_TZ_TA7091PAYMENTS  XIN_TZ)
           LOOP
                V_NUMORD    := I.NUMORD;
                V_FLG_RIBA  := I.FLG_RIBA;
                V_DATAPAG   := I.CLEARINGDATE;
                V_ENTRYDATE := I.ENTRYDATE;
                V_CLE_MONTH := I.CLE_MONTH;
                V_ENT_MONTH := I.ENT_MONTH;
           END LOOP;

  FOR C IN (SELECT DISTINCT  ATT.CODTYPORD  ,ATT.CODDIV , ATT.PAYMENT_KEY
              INTO V_CODTYPORD , V_CODDIV , V_PAYMENT_KEY
              FROM   T660CONT_ATT ATT
              WHERE CODDIV = '4770'
              AND NUMORD = ATT.NUMORD)
         LOOP
               V_CODTYPORD := C.CODTYPORD;
               V_CODDIV := C.CODDIV;
               V_PAYMENT_KEY := C.PAYMENT_KEY;
         END LOOP;

      IF
         V_FLG_RIBA <> -1  OR V_FLG_RIBA IS NULL
              THEN V_DATAPAG := V_DATAPAG;
      ELSIF
         V_FLG_RIBA = -1  AND  V_ENT_MONTH >= V_CLE_MONTH
              THEN V_DATAPAG := V_ENTRYDATE;
      ELSIF
         V_FLG_RIBA = -1  AND V_ENT_MONTH < V_CLE_MONTH
              THEN V_DATAPAG := V_DATAPAG;
      END IF;


    INSERT INTO TZ_TA7091PAYMENTS( NUMORD ,DATAPAG ,STATOPAG ,  TIPODOC , CODDIV , PAYMENT_KEY   )
                            VALUES(V_NUMORD ,V_DATAPAG ,'PAG' ,  V_CODTYPORD ,V_CODDIV ,V_PAYMENT_KEY) ;
      COMMIT;

  EXCEPTION WHEN OTHERS THEN
      ROLLBACK;
 -- WHEN TOO_MANY_ROWS THEN NULL;
 END;
 /

--------------------------------------------------------------------------------------------------------------------------------------------

  CREATE OR REPLACE PROCEDURE TZ_IMPORT
AS
CURSOR XIN IS
          SELECT XIN_TZ.NUMORD AS NUMORD ,
                 XIN_TZ.FLG_RIBA AS FLG_RIBA ,

                 XIN_TZ.CLEARINGDATE AS CLEARINGDATE ,
                 XIN_TZ.ENTRYDATE AS ENTRYDATE ,

                 TO_CHAR(TO_DATE(XIN_TZ.CLEARINGDATE,'DD/MM/YYYYHH24:mi:ss'), 'MM') AS CLE_MONTH ,
                 TO_CHAR(TO_DATE(XIN_TZ.ENTRYDATE,'DD/MM/YYYYHH24:mi:ss'), 'MM') AS ENT_MONTH
                FROM XIN_TZ_TA7091PAYMENTS  XIN_TZ;

V_NUMORD NUMBER(12,0);
V_FLG_RIBA NUMBER(1);
V_DATAPAG DATE ;
V_ENTRYDATE DATE;
V_CLE_MONTH NUMBER(2);
V_ENT_MONTH NUMBER (2);

V_CODTYPORD VARCHAR2(255);
V_CODDIV VARCHAR2(255);
V_PAYMENT_KEY VARCHAR2(255);

BEGIN

  FOR C IN (SELECT DISTINCT  ATT.CODTYPORD  ,ATT.CODDIV , ATT.PAYMENT_KEY
              INTO V_CODTYPORD , V_CODDIV , V_PAYMENT_KEY
              FROM   T660CONT_ATT ATT
              WHERE CODDIV = '4770')

         LOOP
               V_CODTYPORD := C.CODTYPORD;
               V_CODDIV := C.CODDIV;
               V_PAYMENT_KEY := C.PAYMENT_KEY;
         END LOOP;

       OPEN XIN ;
  LOOP

    EXIT WHEN (XIN%NOTFOUND);

    FETCH XIN INTO V_NUMORD , V_FLG_RIBA , V_DATAPAG , V_ENTRYDATE ,  V_CLE_MONTH , V_ENT_MONTH  ;

      IF
         V_FLG_RIBA <> -1  OR V_FLG_RIBA IS NULL
              THEN V_DATAPAG := V_DATAPAG;
      ELSIF
         V_FLG_RIBA = -1  AND  V_ENT_MONTH >= V_CLE_MONTH
              THEN V_DATAPAG := V_ENTRYDATE;
      ELSIF
         V_FLG_RIBA = -1  AND V_ENT_MONTH < V_CLE_MONTH
              THEN V_DATAPAG := V_DATAPAG;
      END IF;

    INSERT INTO TZ_TA7091PAYMENTS( NUMORD ,DATAPAG ,STATOPAG ,  TIPODOC , CODDIV , PAYMENT_KEY   )
                            VALUES(V_NUMORD ,V_DATAPAG ,'PAG' ,  V_CODTYPORD ,V_CODDIV ,V_PAYMENT_KEY) ;
      COMMIT;

  END LOOP;

        CLOSE XIN;


  EXCEPTION WHEN OTHERS THEN
      ROLLBACK;
 -- WHEN TOO_MANY_ROWS THEN NULL;
 END;
 /

 --------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE PROCEDURE TZ_IMPORT
AS
CURSOR XIN IS
          SELECT XIN_TZ.NUMORD AS NUMORD ,
                 XIN_TZ.FLG_RIBA AS FLG_RIBA ,

                 XIN_TZ.CLEARINGDATE AS CLEARINGDATE ,
                 XIN_TZ.ENTRYDATE AS ENTRYDATE ,

                 TO_CHAR(TO_DATE(XIN_TZ.CLEARINGDATE,'DD/MM/YYYYHH24:mi:ss'), 'MM') AS CLE_MONTH ,
                 TO_CHAR(TO_DATE(XIN_TZ.ENTRYDATE,'DD/MM/YYYYHH24:mi:ss'), 'MM') AS ENT_MONTH
                FROM XIN_TZ_TA7091PAYMENTS  XIN_TZ;

V_NUMORD NUMBER(12,0);
V_FLG_RIBA NUMBER(1);
V_DATAPAG DATE ;
V_ENTRYDATE DATE;
V_CLE_MONTH NUMBER(2);
V_ENT_MONTH NUMBER (2);

V_CODTYPORD VARCHAR2(255);
V_CODDIV VARCHAR2(255);
V_PAYMENT_KEY VARCHAR2(255);

BEGIN

  FOR C IN (SELECT DISTINCT  ATT.CODTYPORD  ,ATT.CODDIV , ATT.PAYMENT_KEY
              INTO V_CODTYPORD , V_CODDIV , V_PAYMENT_KEY
              FROM   T660CONT_ATT ATT
              WHERE CODDIV = '4770')

         LOOP
               V_CODTYPORD := C.CODTYPORD;
               V_CODDIV := C.CODDIV;
               V_PAYMENT_KEY := C.PAYMENT_KEY;
         END LOOP;

      FOR I IN XIN
       LOOP

          IF
            I.FLG_RIBA <> -1  OR I.FLG_RIBA IS NULL
                  THEN V_DATAPAG := I.CLEARINGDATE;
          ELSIF
             I.FLG_RIBA = -1  AND  I.ENT_MONTH >= I.CLE_MONTH
                  THEN V_DATAPAG := I.ENTRYDATE;
          ELSIF
             I.FLG_RIBA = -1  AND I.ENT_MONTH < I.CLE_MONTH
                  THEN V_DATAPAG := I.CLEARINGDATE;
          END IF;

        INSERT INTO TZ_TA7091PAYMENTS( NUMORD ,DATAPAG ,STATOPAG ,  TIPODOC , CODDIV , PAYMENT_KEY)
                          VALUES(I.NUMORD ,V_DATAPAG,'PAG' ,  V_CODTYPORD ,V_CODDIV ,V_PAYMENT_KEY) ;
          COMMIT;

    END LOOP;

  EXCEPTION WHEN OTHERS THEN
      ROLLBACK;
 -- WHEN TOO_MANY_ROWS THEN NULL;
 END;
 /
