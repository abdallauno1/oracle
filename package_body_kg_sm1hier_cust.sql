CREATE OR REPLACE PACKAGE BODY PKG_SM1V4HIER_CUST
IS

VCOUNT NUMBER(9);
--TYPE arr_type is TABLE of VARCHAR2(255);
--my_array arr_type := arr_type();

VDESNODE0 VARCHAR2(255);
VDESNODELONG0 VARCHAR2(255);

VDESNODE1 VARCHAR2(255);
VDESNODELONG1 VARCHAR2(255);

VDESNODE2 VARCHAR2(255);
VDESNODELONG2 VARCHAR2(255);

VDESNODE3 VARCHAR2(255);
VDESNODELONG3 VARCHAR2(255);

VDESNODE4 VARCHAR2(255);
VDESNODELONG4 VARCHAR2(255);

VDESNODE5 VARCHAR2(255);
VDESNODELONG5 VARCHAR2(255);

VCHECKNODE VARCHAR2 (255);
VCHECKSUP1 VARCHAR2 (255);
VCHECKSUP2 VARCHAR2 (255);
VCHECKSUP3 VARCHAR2 (255);
VCHECKSUP4 VARCHAR2 (255);
VCHECKSUP5 VARCHAR2 (255);

VUSRGROUP VARCHAR2(255);
VSALESMAN VARCHAR2(255);
VCODLEVEL VARCHAR2(30);
VDIV VARCHAR2(30);
VUSRAREA VARCHAR2(255);

VCODNODE VARCHAR2(255);
VSUP1 VARCHAR2(255);
VSUP2 VARCHAR2(255);
VSUP3 VARCHAR2(255);
VSUP4 VARCHAR2(255);
VSUP5 VARCHAR2(255);

PROCEDURE SP_GENERATE(VCODDIV IN VARCHAR2)AS

BEGIN

      -- DELTE FROM  TZ_HIER_NODES  & TZ_HIER_REL
      DELETE FROM TZ_HIER_NODES_SRC_USER_C;
      DELETE FROM TZ_HIER_REL_SRC_USER_C;
      COMMIT;

      SP_USERS_B2B(VCODDIV);
      SP_USERS_B2C(VCODDIV);
      SP_USERS_MERCH_AUDIT(VCODDIV);
      SP_USERS_DELIVERY(VCODDIV);
      REMOVE_ROWS;

END SP_GENERATE;

PROCEDURE SP_USERS_DELIVERY(VCODDIV IN VARCHAR2)AS

CURSOR VCUR IS
              -- query delivery man
              SELECT DISTINCT
                      T030.CODUSR AS CODNODE
                     , T031.CODDIV AS CODDIV
                     , GRP.USRGROUP AS USRGROUP
                     , GRP.USRTYPE -1 AS CODLEVEL
                     , T031.CODUSRSUP1 AS SUP1
                     , T031.CODUSRSUP2 AS SUP2
                     , T031.CODUSRSUP3 AS SUP3
                     , T031.CODUSRSUP4 AS SUP4
                     , T031.CODUSRSUP5 AS SUP5
              FROM T031USERDIV T031
              INNER JOIN T033GROUPS GRP
              ON GRP.USRGROUP = T031.USRGROUP
              INNER JOIN T030USER T030
              ON T030.CODUSR = T031.CODUSR
              WHERE T030.CODUSR IN
              ( SELECT NOD.CODNODE
                FROM NODES_SRC_USER_C NOD
                INNER JOIN T030USER USR
                ON NOD.CODNODE = USR.CODUSR
                AND NOD.CODDIV = VCODDIV
                INNER JOIN T031USERDIV DIV
                ON DIV.CODUSR = NOD.CODNODE
                AND NOD.CODDIV = VCODDIV
              )
              AND GRP.USRTYPE -1 >= 0
              AND T031.CODDIV = VCODDIV
             /*
              -- FILTERS ACCORDING WITH DAL DELIVERY
              AND T031.CODUSRSUP1 IS NOT NULL
              AND T031.CODUSRSUP2 IS NOT NULL
              AND T031.CODUSRSUP3 IS NOT NULL
              */
              AND T031.Usrgroup IN  ( SELECT COD FROM QTABS_C WHERE CODSYS = VCODDIV AND CODTAB = 'DELIVERY_GRP')
              ;

      BEGIN


        FOR XIN_R IN VCUR
            LOOP


                              VCODNODE := XIN_R.CODNODE;
                              VUSRGROUP := XIN_R.USRGROUP;

                              VSUP1 := XIN_R.SUP2; -- takes from sup2 for the delivery
                              VSUP1 := VSUP1 || '_dummy';

                              VSUP2 := XIN_R.SUP2;
                              VSUP3 := XIN_R.SUP3;

                              VSUP4 := XIN_R.SUP3; -- takes from sup3 delivery_dummy
                              VSUP4 := VSUP4 || '_dummy_4';

                              VSUP5 := XIN_R.SUP3; -- takes from sup3 delivery_dummy
                              VSUP5 := VSUP5 || '_dummy_5';



            -- NODE 0
              SELECT DESNODE
              INTO VDESNODE0
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VCODNODE;

              SELECT DESNODELONG
              INTO VDESNODELONG0
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VCODNODE;

              -- NODE 1 -->  should be supervisor _dummy takes from sup2
              SELECT DESNODE
              INTO VDESNODE1
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP1;
              VDESNODE1:= VDESNODE1 || '_dummy';

              SELECT DESNODELONG
              INTO VDESNODELONG1
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP1;
              VDESNODELONG1 := VDESNODELONG1 || '_dummy';

              --NODE 2
              SELECT DESNODE
              INTO VDESNODE2
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP2;

              SELECT DESNODELONG
              INTO VDESNODELONG2
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP2;

              --NODE 3
              SELECT DESNODE
              INTO VDESNODE3
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP3;

              SELECT DESNODELONG
              INTO VDESNODELONG3
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP3;

              --NODE 4
              SELECT DESNODE
              INTO VDESNODE4
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP4;
              VDESNODE4 := VDESNODE4 || '_dummy_4';


              SELECT DESNODELONG
              INTO VDESNODELONG4
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP4;
              VDESNODELONG4 := VDESNODELONG4 || '_dummy_4';

             --NODE 5
              SELECT DESNODE
              INTO VDESNODE5
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP5;
              VDESNODE5 := VDESNODE5 || '_dummy_5';

              SELECT DESNODELONG
              INTO VDESNODELONG5
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP5;
              VDESNODELONG5 := VDESNODELONG5 || '_dummy_5';

              --INSERT NODES
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VCODNODE , '0' , VCODDIV ,VDESNODE0 , VDESNODELONG0 , '0' );

               --INSERT NODES
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VSUP1 , '1' , VCODDIV ,VDESNODE1 , VDESNODELONG1 , '0' );

               --INSERT HIER REL
               INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VCODNODE , '0' , '1' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VSUP1,'0' ,'1' ,SYSDATE , 'SYS') ;


             -- LEVEL 1
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VSUP2 , '2' , VCODDIV ,VDESNODE2 , VDESNODELONG2 , '0' );

              INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VSUP1 , '1' , '2' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VSUP2,'1' ,'2' ,SYSDATE , 'SYS') ;


             -- LEVEL 2
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VSUP3 , '3' , VCODDIV ,VDESNODE3 , VDESNODELONG3 , '0' );

              INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VSUP2 , '2' , '3' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VSUP3,'2' ,'3' ,SYSDATE , 'SYS') ;

             --LIVELLO 3
              INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VSUP3 , '3' , '4' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VSUP4,'3' ,'4' ,SYSDATE , 'SYS') ;

               --LIVELLO 4
                INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VSUP5 , '4' , '5' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VSUP5,'4' ,'5' ,SYSDATE , 'SYS') ;

               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VSUP4 , '4' , VCODDIV ,VDESNODE4 , VDESNODELONG4 , '0' );

               --LIVELLO 5
               INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VSUP5 , '5' , '6' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),'TOT_TERRI','5' ,'6' ,SYSDATE , 'SYS') ;

               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VSUP5 , '5' , VCODDIV ,VDESNODE5 , VDESNODELONG5 , '0' );

               --ULITMO LIVELLO 6 FAKE GROUP TOTALE
               INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,'TOT_TERRI' , '6' , '6' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),'TOT_TERRI','6' ,'6' ,SYSDATE , 'SYS') ;

               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES ('TOT_TERRI' , '6' , VCODDIV ,'Terri Group' , 'Terri Group' , '0' );

               COMMIT;

        END LOOP; --END LOOP

END SP_USERS_DELIVERY;

PROCEDURE SP_USERS_B2B(VCODDIV IN VARCHAR2) AS

CURSOR VCUR IS
              -- query B2B
              SELECT DISTINCT
                      T030.CODUSR AS CODNODE
                     , T031.CODDIV AS CODDIV
                     , GRP.USRGROUP AS USRGROUP
                     , GRP.USRTYPE -1 AS CODLEVEL
                     , T031.CODUSRSUP1 AS SUP1
                     , T031.CODUSRSUP2 AS SUP2
                     , T031.CODUSRSUP3 AS SUP3
                     , T031.CODUSRSUP4 AS SUP4
                     , T031.CODUSRSUP5 AS SUP5
              FROM T031USERDIV T031
              INNER JOIN T033GROUPS GRP
              ON GRP.USRGROUP = T031.USRGROUP
              INNER JOIN T030USER T030
              ON T030.CODUSR = T031.CODUSR
              WHERE T030.CODUSR IN
              ( SELECT NOD.CODNODE
                FROM NODES_SRC_USER_C NOD
                INNER JOIN T030USER USR
                ON NOD.CODNODE = USR.CODUSR
                AND NOD.CODDIV = VCODDIV
                INNER JOIN T031USERDIV DIV
                ON DIV.CODUSR = NOD.CODNODE
                AND NOD.CODDIV = VCODDIV
              )
              AND GRP.USRTYPE -1 >= 0
              AND T031.CODDIV = VCODDIV
              /*
              -- FILTERS ACCORDING WITH DAL B2B
              AND T031.CODUSRSUP1 IS NOT NULL
              AND T031.CODUSRSUP2 IS NOT NULL
              AND T031.CODUSRSUP3 IS NOT NULL
              AND T031.CODUSRSUP4 IS NOT NULL
              */
              AND T031.Usrgroup IN  ( SELECT COD FROM QTABS_C WHERE CODSYS = VCODDIV AND CODTAB = 'B2B_GRP')
              -- ORDER BY CODLEVEL  ASC
              ;

      BEGIN


        FOR XIN_R IN VCUR
            LOOP

                              VCODNODE := XIN_R.SUP2; -- livello 0 dummy from sup2 B2B
                              VCODNODE := VCODNODE || '_dummy_0';

                              VUSRGROUP := XIN_R.USRGROUP;

                              VSUP1 := XIN_R.SUP2; -- livello 1 dummy from sup2 B2B
                              VSUP1 := VSUP1 ||'_dummy_1';

                              VSUP2 := XIN_R.SUP2;
                              VSUP3 := XIN_R.SUP3;
                              VSUP4 := XIN_R.SUP4;
                              VSUP5 := XIN_R.SUP5;

                    -- CONTROLLO MISSING LEVEL 5
                   IF VSUP5 IS NULL
                     THEN VCHECKSUP5 := VSUP4;
                   ELSE
                     VCHECKSUP5 := VSUP5;
                   END IF;

            -- NODE 0
              SELECT DESNODE
              INTO VDESNODE0
              FROM NODES_SRC_USER_C
              WHERE CODNODE = XIN_R.SUP2;

              VDESNODE0 := VCODNODE || '_dummy_0';

              SELECT DESNODELONG
              INTO VDESNODELONG0
              FROM NODES_SRC_USER_C
              WHERE CODNODE = XIN_R.SUP2;

              VDESNODELONG0 := VCODNODE || '_dummy_0';

              -- NODE 1
              SELECT DESNODE
              INTO VDESNODE1
              FROM NODES_SRC_USER_C
              WHERE CODNODE = XIN_R.SUP2;

              VDESNODE1 := VCODNODE || '_dummy_1';

              SELECT DESNODELONG
              INTO VDESNODELONG1
              FROM NODES_SRC_USER_C
              WHERE CODNODE = XIN_R.SUP2;

              VDESNODELONG1 := VCODNODE || '_dummy_1';

              --NODE 2
              SELECT DESNODE
              INTO VDESNODE2
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP2;

              SELECT DESNODELONG
              INTO VDESNODELONG2
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP2;

              --NODE 3
              SELECT DESNODE
              INTO VDESNODE3
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP3;

              SELECT DESNODELONG
              INTO VDESNODELONG3
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP3;

              --NODE 4
              SELECT DESNODE
              INTO VDESNODE4
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP4;

              SELECT DESNODELONG
              INTO VDESNODELONG4
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP4;

             --NODE 5
              SELECT DESNODE
              INTO VDESNODE5
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VCHECKSUP5;

              SELECT DESNODELONG
              INTO VDESNODELONG5
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VCHECKSUP5;

              --INSERT NODES
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VCODNODE , '0' , VCODDIV ,VDESNODE0 , VDESNODELONG0 , '0' );

               --INSERT NODES
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VSUP1 , '1' , VCODDIV ,VDESNODE1 , VDESNODELONG1 , '0' );

               --INSERT HIER REL
               INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VCODNODE , '0' , '1' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VSUP1,'0' ,'1' ,SYSDATE , 'SYS') ;


             -- LEVEL 1
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VSUP2 , '2' , VCODDIV ,VDESNODE2 , VDESNODELONG2 , '0' );

              INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VSUP1 , '1' , '2' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VSUP2,'1' ,'2' ,SYSDATE , 'SYS') ;


             -- LEVEL 2
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VSUP3 , '3' , VCODDIV ,VDESNODE3 , VDESNODELONG3 , '0' );

              INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VSUP2 , '2' , '3' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VSUP3,'2' ,'3' ,SYSDATE , 'SYS') ;

             --LIVELLO 3
              INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VSUP3 , '3' , '4' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VSUP4,'3' ,'4' ,SYSDATE , 'SYS') ;

               --LIVELLO 4
                INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VSUP4 , '4' , '5' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VCHECKSUP5,'4' ,'5' ,SYSDATE , 'SYS') ;

               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VSUP4 , '4' , VCODDIV ,VDESNODE4 , VDESNODELONG4 , '0' );

               --LIVELLO 5
               INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VCHECKSUP5 , '5' , '6' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),'','5' ,'6' ,SYSDATE , 'SYS') ;

               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VCHECKSUP5 , '5' , VCODDIV ,'TOT_TERRI' , VDESNODELONG5 , '0' );

               --ULITMO LIVELLO 6 FAKE GROUP TOTALE
               INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,'TOT_TERRI' , '6' , '6' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),'TOT_TERRI','6' ,'6' ,SYSDATE , 'SYS') ;

               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES ('TOT_TERRI' , '6' , VCODDIV ,'Terri Group' , 'Terri Group' , '0' );

               COMMIT;

         END LOOP; --END LOOP

END SP_USERS_B2B;

PROCEDURE SP_USERS_MERCH_AUDIT (VCODDIV IN VARCHAR2) AS

CURSOR VCUR IS
              -- query Merchandiser and Audit
              SELECT DISTINCT
                      T030.CODUSR AS CODNODE
                     , T031.CODDIV AS CODDIV
                     , GRP.USRGROUP AS USRGROUP
                     , GRP.USRTYPE -1 AS CODLEVEL
                     , T031.CODUSRSUP1 AS SUP1
                     , T031.CODUSRSUP2 AS SUP2
                     , T031.CODUSRSUP3 AS SUP3
                     , T031.CODUSRSUP4 AS SUP4
                     , T031.CODUSRSUP5 AS SUP5
              FROM T031USERDIV T031
              INNER JOIN T033GROUPS GRP
              ON GRP.USRGROUP = T031.USRGROUP
              INNER JOIN T030USER T030
              ON T030.CODUSR = T031.CODUSR
              WHERE T030.CODUSR IN
              ( SELECT NOD.CODNODE
                FROM NODES_SRC_USER_C NOD
                INNER JOIN T030USER USR
                ON NOD.CODNODE = USR.CODUSR
                AND NOD.CODDIV = VCODDIV
                INNER JOIN T031USERDIV DIV
                ON DIV.CODUSR = NOD.CODNODE
                AND NOD.CODDIV = VCODDIV
              )
              AND GRP.USRTYPE -1 >= 0
              AND T031.CODDIV = VCODDIV
              /*
              -- FILTERS ACCORDING WITH DAL Merchandiser & Audit
              AND T031.CODUSRSUP2 IS NOT NULL
              AND T031.CODUSRSUP3 IS NOT NULL
              AND T031.CODUSRSUP4 IS NOT NULL
               */
              AND T031.Usrgroup IN  ( SELECT COD FROM QTABS_C WHERE CODSYS = VCODDIV AND CODTAB = 'MERCH_AUDIT_GRP')
              -- ORDER BY CODLEVEL  ASC
              ;

      BEGIN


        FOR XIN_R IN VCUR
            LOOP

                              VCODNODE := XIN_R.CODNODE;
                              VUSRGROUP := XIN_R.USRGROUP;
                              VSUP1 := XIN_R.SUP1;
                              VSUP2 := XIN_R.SUP2;
                              VSUP3 := XIN_R.SUP3;
                              VSUP4 := XIN_R.SUP4;
                              VSUP5 := XIN_R.SUP5;

                    -- CONTROLLO MISSING SUP1
                    IF VSUP1 IS NULL
                      THEN VCHECKSUP1 := VSUP2;

                     ELSE
                        VCHECKSUP1 := VSUP1;
                    END IF;

                    -- CONTROLLO MISSING LEVEL 5
                   IF VSUP5 IS NULL
                     THEN VCHECKSUP5 := VSUP4;
                   ELSE
                     VCHECKSUP5 := VSUP5;
                   END IF;

            -- NODE 0
              SELECT DESNODE
              INTO VDESNODE0
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VCODNODE;

              SELECT DESNODELONG
              INTO VDESNODELONG0
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VCODNODE;

              -- NODE 1
              SELECT DESNODE
              INTO VDESNODE1
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VCHECKSUP1;

              SELECT DESNODELONG
              INTO VDESNODELONG1
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VCHECKSUP1;

              --NODE 2
              SELECT DESNODE
              INTO VDESNODE2
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP2;

              SELECT DESNODELONG
              INTO VDESNODELONG2
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP2;

              --NODE 3
              SELECT DESNODE
              INTO VDESNODE3
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP3;

              SELECT DESNODELONG
              INTO VDESNODELONG3
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP3;

              --NODE 4
              SELECT DESNODE
              INTO VDESNODE4
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP4;

              SELECT DESNODELONG
              INTO VDESNODELONG4
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP4;

             --NODE 5
              SELECT DESNODE
              INTO VDESNODE5
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VCHECKSUP5;

              SELECT DESNODELONG
              INTO VDESNODELONG5
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VCHECKSUP5;

              --INSERT NODES
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VCODNODE , '0' , VCODDIV ,VDESNODE0 , VDESNODELONG0 , '0' );

               --INSERT NODES
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VCHECKSUP1 , '1' , VCODDIV ,VDESNODE1 , VDESNODELONG1 , '0' );

               --INSERT HIER REL
               INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VCODNODE , '0' , '1' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VCHECKSUP1,'0' ,'1' ,SYSDATE , 'SYS') ;


             -- LEVEL 1
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VSUP2 , '2' , VCODDIV ,VDESNODE2 , VDESNODELONG2 , '0' );

              INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VSUP1 , '1' , '2' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VSUP2,'1' ,'2' ,SYSDATE , 'SYS') ;


             -- LEVEL 2
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VSUP3 , '3' , VCODDIV ,VDESNODE3 , VDESNODELONG3 , '0' );

              INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VSUP2 , '2' , '3' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VSUP3,'2' ,'3' ,SYSDATE , 'SYS') ;

             --LIVELLO 3
              INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VSUP3 , '3' , '4' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VSUP4,'3' ,'4' ,SYSDATE , 'SYS') ;

               --LIVELLO 4
                INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VSUP4 , '4' , '5' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VCHECKSUP5,'4' ,'5' ,SYSDATE , 'SYS') ;

               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VSUP4 , '4' , VCODDIV ,VDESNODE4 , VDESNODELONG4 , '0' );

               --LIVELLO 5
               INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VCHECKSUP5 , '5' , '6' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),'TOT_TERRI','5' ,'6' ,SYSDATE , 'SYS') ;

               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VCHECKSUP5 , '5' , VCODDIV ,VDESNODE5 , VDESNODELONG5 , '0' );

               --ULITMO LIVELLO 6 FAKE GROUP TOTALE
               INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,'TOT_TERRI' , '6' , '6' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),'TOT_TERRI','6' ,'6' ,SYSDATE , 'SYS') ;

               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES ('TOT_TERRI' , '6' , VCODDIV ,'Terri Group' , 'Terri Group' , '0' );

               COMMIT;

         END LOOP; --END LOOP


END SP_USERS_MERCH_AUDIT;

PROCEDURE SP_USERS_B2C(VCODDIV IN VARCHAR2) AS

CURSOR VCUR IS
            -- query B2C
              SELECT DISTINCT
                      T030.CODUSR AS CODNODE
                     , T031.CODDIV AS CODDIV
                     , GRP.USRGROUP AS USRGROUP
                     , GRP.USRTYPE -1 AS CODLEVEL
                     , T031.CODUSRSUP1 AS SUP1
                     , T031.CODUSRSUP2 AS SUP2
                     , T031.CODUSRSUP3 AS SUP3
                     , T031.CODUSRSUP4 AS SUP4
                     , T031.CODUSRSUP5 AS SUP5
              FROM T031USERDIV T031
              INNER JOIN T033GROUPS GRP
              ON GRP.USRGROUP = T031.USRGROUP
              INNER JOIN T030USER T030
              ON T030.CODUSR = T031.CODUSR
              WHERE T030.CODUSR IN
              ( SELECT NOD.CODNODE
                FROM NODES_SRC_USER_C NOD
                INNER JOIN T030USER USR
                ON NOD.CODNODE = USR.CODUSR
                AND NOD.CODDIV = VCODDIV
                INNER JOIN T031USERDIV DIV
                ON DIV.CODUSR = NOD.CODNODE
                AND NOD.CODDIV = VCODDIV
              )
              AND GRP.USRTYPE -1 >= 0
              AND T031.CODDIV = VCODDIV
              /*
              -- FILTERS ACCORDING WITH DAL B2C
              AND T031.CODUSRSUP2 IS NOT NULL
              AND T031.CODUSRSUP3 IS NOT NULL
              AND T031.CODUSRSUP4 IS NOT NULL
              */
              AND T031.Usrgroup IN  ( SELECT COD FROM QTABS_C WHERE CODSYS = VCODDIV AND CODTAB = 'B2C_GRP')
              ;

      BEGIN


        FOR XIN_R IN VCUR
            LOOP

                              VCODNODE := XIN_R.CODNODE;
                              VUSRGROUP := XIN_R.USRGROUP;
                              VSUP1 := XIN_R.SUP2; --takes from sup2 for the B2C
                              VSUP1 := VSUP1 || '_dummy';

                              VSUP2 := XIN_R.SUP2;
                              VSUP3 := XIN_R.SUP3;
                              VSUP4 := XIN_R.SUP4;
                              VSUP5 := XIN_R.SUP5;



                    -- CONTROLLO MISSING LEVEL 5
                   IF VSUP5 IS NULL
                     THEN VCHECKSUP5 := VSUP4;
                   ELSE
                     VCHECKSUP5 := VSUP5;
                   END IF;

            -- NODE 0
              SELECT DESNODE
              INTO VDESNODE0
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VCODNODE;

              SELECT DESNODELONG
              INTO VDESNODELONG0
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VCODNODE;

              -- NODE 1
              SELECT DESNODE
              INTO VDESNODE1
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP1;
              VDESNODE1 := VDESNODE1 || '_dummy';

              SELECT DESNODELONG
              INTO VDESNODELONG1
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP1;
              VDESNODELONG1 := VDESNODELONG1 || '_dummy';

              --NODE 2
              SELECT DESNODE
              INTO VDESNODE2
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP2;

              SELECT DESNODELONG
              INTO VDESNODELONG2
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP2;

              --NODE 3
              SELECT DESNODE
              INTO VDESNODE3
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP3;

              SELECT DESNODELONG
              INTO VDESNODELONG3
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP3;

              --NODE 4
              SELECT DESNODE
              INTO VDESNODE4
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP4;

              SELECT DESNODELONG
              INTO VDESNODELONG4
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VSUP4;

             --NODE 5
              SELECT DESNODE
              INTO VDESNODE5
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VCHECKSUP5;

              SELECT DESNODELONG
              INTO VDESNODELONG5
              FROM NODES_SRC_USER_C
              WHERE CODNODE = VCHECKSUP5;

              --INSERT NODES
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VCODNODE , '0' , VCODDIV ,VDESNODE0 , VDESNODELONG0 , '0' );

               --INSERT NODES
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VSUP1 , '1' , VCODDIV ,VDESNODE1 , VDESNODELONG1 , '0' );

               --INSERT HIER REL
               INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VCODNODE , '0' , '1' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VSUP1,'0' ,'1' ,SYSDATE , 'SYS') ;


             -- LEVEL 1
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VSUP2 , '2' , VCODDIV ,VDESNODE2 , VDESNODELONG2 , '0' );

              INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VSUP1 , '1' , '2' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VSUP2,'1' ,'2' ,SYSDATE , 'SYS') ;


             -- LEVEL 2
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VSUP3 , '3' , VCODDIV ,VDESNODE3 , VDESNODELONG3 , '0' );

              INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VSUP2 , '2' , '3' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VSUP3,'2' ,'3' ,SYSDATE , 'SYS') ;

             --LIVELLO 3
              INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VSUP3 , '3' , '4' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VSUP4,'3' ,'4' ,SYSDATE , 'SYS') ;

               --LIVELLO 4
                INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VSUP4 , '4' , '5' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VCHECKSUP5,'4' ,'5' ,SYSDATE , 'SYS') ;

               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VSUP4 , '4' , VCODDIV ,VDESNODE4 , VDESNODELONG4 , '0' );

               --LIVELLO 5
               INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,VCHECKSUP5 , '5' , '6' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),'TOT_TERRI','5' ,'6' ,SYSDATE , 'SYS') ;

               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VCHECKSUP5 , '5' , VCODDIV ,VDESNODE5 , VDESNODELONG5 , '0' );

               --ULITMO LIVELLO 6 FAKE GROUP TOTALE
               INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'TERRI' ,'TOT_TERRI' , '6' , '6' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),'TOT_TERRI','6' ,'6' ,SYSDATE , 'SYS') ;

               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES ('TOT_TERRI' , '6' , VCODDIV ,'Terri Group' , 'Terri Group' , '0' );

               COMMIT;

         END LOOP; --END LOOP

END SP_USERS_B2C;

PROCEDURE SP_USERS_AREA(VCODDIV IN VARCHAR2) AS

CURSOR VCUR IS
                 SELECT
                 IMP.CODUSRTO AS ROUTE,
                 IMP.CODUSR AS SALESMAN,
                 DIV.CODUSRSUP2 AS SUPERVISOR,
                 IMP.CODDIV AS CODDIV,
                 GRP.USRTYPE - 1 AS CODLEVEL,
                 USR.DESUSR AS DESNODE,
                 USR.DESUSR AS DESNODELONG,
                 '0'AS ATTR3
          FROM T037USRIMP IMP
          INNER JOIN T030USER USR
          ON USR.CODUSR = IMP.CODUSRTO
          INNER JOIN T031USERDIV DIV
          ON DIV.CODUSR = USR.CODUSR
          INNER JOIN T033GROUPS GRP
          ON GRP.USRGROUP = DIV.USRGROUP
          WHERE DIV.CODDIV = VCODDIV;


      BEGIN


        FOR XIN_R IN VCUR
            LOOP

                        VCODNODE := XIN_R.ROUTE;
                        VSALESMAN := XIN_R.SALESMAN;
                        VSUP2 := XIN_R.SUPERVISOR;
                        VDIV := XIN_R.CODDIV;
                        VCODLEVEL := XIN_R.CODLEVEL;
                        VDESNODE0 := XIN_R.DESNODE;
                        VDESNODELONG0 := XIN_R.DESNODELONG;
                        VUSRAREA := 'TOT_AREA';

                        -- NODE 1 SALESMAN
                        SELECT DESNODE
                        INTO VDESNODE1
                        FROM NODES_SRC_USER_C
                        WHERE CODNODE = VSALESMAN;


                        SELECT DESNODELONG
                        INTO VDESNODELONG1
                        FROM NODES_SRC_USER_C
                        WHERE CODNODE = VSALESMAN;

                        -- NODE 2 SUPERVISOR
                        SELECT DESNODE
                        INTO VDESNODE2
                        FROM NODES_SRC_USER_C
                        WHERE CODNODE = VSUP2;


                        SELECT DESNODELONG
                        INTO VDESNODELONG2
                        FROM NODES_SRC_USER_C
                        WHERE CODNODE = VSUP2;


              --INSERT NODES LIV 0
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VCODNODE , '0' , VCODDIV ,VDESNODE0 , VDESNODELONG0 , '0' );

               --INSERT NODES LIV 1 --> SALESMAN
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VSALESMAN , '1' , VCODDIV ,VDESNODE1 , VDESNODELONG1 , '0' );

               --INSERT NODES LIV 2 --> SUPERVISOR
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VSALESMAN , '2' , VCODDIV ,VDESNODE2 , VDESNODELONG2 , '0' );

               --INSERT NODES LIV 3 FAKE USR TO GROUP --> TOT_AREA
               INSERT INTO TZ_HIER_NODES_SRC_USER_C (CODNODE, CODLEVEL , CODDIV , DESNODE , DESNODELONG ,Attr3 )
               VALUES (VUSRAREA , '3' , VCODDIV ,'Area Group' , 'Area Group' , '0' );

               --INSERT HIER REL LIV 0 - ROUTE
               INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'AREA' ,VCODNODE , '0' , '1' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VSALESMAN,'0' ,'1' ,SYSDATE , 'SYS') ;

               --INSERT HIER REL LIV 1 - SALESMAN -
               INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'AREA' ,VSALESMAN , '1' , '2' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VSUP2,'1' ,'2' ,SYSDATE , 'SYS') ;

                --INSERT HIER REL LIV 2 - SUPERVISOR -
               INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'AREA' ,VSUP2 , '2' , '3' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VUSRAREA,'2' ,'3' ,SYSDATE , 'SYS') ;

                --INSERT HIER REL SE STESSO FAKE USER --> TOT_AREA
               INSERT INTO TZ_HIER_REL_SRC_USER_C (CODDIV, CODHIER , CODNODE , IDLEVEL , IDPARENTLEVEL , DTESTART
                        , DTEEND ,CODPARENTNODE, CODLEVEL,CODLEVELPARENT,DTEMOD,CODUSRMOD)
               VALUES (VCODDIV , 'AREA' ,VUSRAREA , '3' , '3' , TO_DATE('01/01/1970','DD/MM/YYYY'),
                   TO_DATE('31/12/2099','DD/MM/YYYY'),VUSRAREA,'3' ,'3' ,SYSDATE , 'SYS') ;

               COMMIT;

         END LOOP; --END LOOP

END SP_USERS_AREA;



PROCEDURE REMOVE_ROWS AS
  BEGIN

                 -- DELETE DUBLICATED ROWS RELA
                 DELETE FROM TZ_HIER_REL_SRC_USER_C A
                  WHERE
                    A.ROWID >
                     ANY (
                       SELECT
                          B.rowid
                       FROM
                          TZ_HIER_REL_SRC_USER_C B
                       WHERE
                          A.CODNODE = B.CODNODE
                       AND
                          A.CODLEVEL = B.CODLEVEL
                       AND
                          A.CODDIV  =  B.CODDIV
                       AND
                          A.CODHIER = B.CODHIER
                          );



                  -- DELETE DUBLICATED ROWS NODES
                  DELETE FROM
                     TZ_HIER_NODES_SRC_USER_C A
                  WHERE
                    A.ROWID >
                     ANY (
                       SELECT
                          B.rowid
                       FROM
                          TZ_HIER_NODES_SRC_USER_C B
                       WHERE
                          A.CODNODE = B.CODNODE
                       AND
                          A.CODLEVEL = B.CODLEVEL
                       AND
                          A.CODDIV  =  B.CODDIV
                          );

           -- DELETE NOT FOUND COD PARENT NODE AND CODNODE
          DELETE FROM TZ_HIER_REL_SRC_USER_C WHERE CODNODE IS NULL;
          DELETE FROM TZ_HIER_REL_SRC_USER_C WHERE CODPARENTNODE IS NULL;


          COMMIT;

  END REMOVE_ROWS;

END PKG_SM1V4HIER_CUST;
