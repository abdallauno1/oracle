create or replace package BODY PKG_DANITA_IMPORT AS

  formatDate      VARCHAR2(21) := 'MM/DD/YYYY HH24:MI:SS';
  codDimCust      VARCHAR2(4)  := 'CUST';
  codDimUser      VARCHAR2(4)  := 'USER';
  codHierUser     VARCHAR2(5)  := 'TERRI';
  codHierCust     VARCHAR2(4)  := 'COMM';

   FUNCTION getIdParent (id NUMBER, HKUNNR VARCHAR2) RETURN NUMBER
     AS
        parentId NUMBER;
     BEGIN
        IF HKUNNR IS NULL THEN
           parentId := id;
        ELSE
           parentId := id+1;
        END IF;

        RETURN parentId;
   END;

   FUNCTION getCodDim(codHier VARCHAR2) RETURN varchar2
     AS
       codDim varchar2(5);
     BEGIN
       IF codHier = codHierCust THEN
         codDim := codDimCust;
       ELSIF codHier = codHierUser THEN
         codDim := codDimUser;
       END IF;

       RETURN codDim;
   END;

   FUNCTION getInvoicePayment(xin XIN_TZ_TA7091PAYMENTS%ROWTYPE) RETURN TZ_TA7091PAYMENTS%ROWTYPE
     AS
        tz_ta7091 TZ_TA7091PAYMENTS%ROWTYPE;
     BEGIN
        tz_ta7091.numord := xin.numord;
        tz_ta7091.datapag := xin.datapag;
        tz_ta7091.statopag := xin.statopag;
        tz_ta7091.tipodoc := xin.tipodoc;
        tz_ta7091.numdocref := xin.numdocref;
        tz_ta7091.coddiv := '4770';
        tz_ta7091.DTECRE := sysdate;
        tz_ta7091.DTEMOD := sysdate;
        tz_ta7091.payment_key := xin.numord || xin.tipodoc;
        RETURN tz_ta7091;
    END;

    PROCEDURE update_elab_hierarchy(kunnr VARCHAR2, flg_elab number)
      AS
      BEGIN
        UPDATE customer_hierarchy_file f
        SET f.flg_elab = flg_elab
        WHERE f.kunnr = kunnr;
    END;

   PROCEDURE pre_elab_customers_hierarchy AS

     CURSOR hier_cur IS
     SELECT fs.kunnr as livs, f0.kunnr as liv0, f1.kunnr as liv1,
            f2.kunnr as liv2, f3.kunnr as liv3, f4.kunnr as liv4,
            f5.kunnr as liv5
     FROM  customer_hierarchy_file fs
           JOIN
           customer_hierarchy_file f0
           ON f0.kunnr = fs.hkunnr
           AND getDanitaDiv(f0.vkorg, f0.vtweg, f0.spart) = getDanitaDiv(fs.vkorg, fs.vtweg, fs.spart)
           AND f0.hityp = fs.hityp
           JOIN
           customer_hierarchy_file f1
           ON f1.kunnr = f0.hkunnr
           AND getDanitaDiv(f1.vkorg, f1.vtweg, f1.spart) = getDanitaDiv(f0.vkorg, f0.vtweg, f0.spart)
           AND f1.hityp = f0.hityp
           JOIN
           customer_hierarchy_file f2
           ON f2.kunnr = f1.hkunnr
           AND getDanitaDiv(f2.vkorg, f2.vtweg, f2.spart) = getDanitaDiv(f1.vkorg, f1.vtweg, f1.spart)
           AND f2.hityp = f1.hityp
           JOIN
           customer_hierarchy_file f3
           ON f3.kunnr = f2.hkunnr
           AND getDanitaDiv(f3.vkorg, f3.vtweg, f3.spart) = getDanitaDiv(f2.vkorg, f2.vtweg, f2.spart)
           AND f3.hityp = f2.hityp
           JOIN
           customer_hierarchy_file f4
           ON f4.kunnr = f3.hkunnr
           AND getDanitaDiv(f4.vkorg, f4.vtweg, f4.spart) = getDanitaDiv(f3.vkorg, f3.vtweg, f3.spart)
           AND f4.hityp = f3.hityp
           JOIN
           customer_hierarchy_file f5
           ON f5.kunnr = f4.hkunnr
           AND getDanitaDiv(f5.vkorg, f5.vtweg, f5.spart) = getDanitaDiv(f4.vkorg, f4.vtweg, f4.spart)
           AND f5.hityp = f4.hityp
     WHERE fs.hzuor = -1
           AND f0.hzuor = 0
           AND f1.hzuor = 1
           AND f2.hzuor = 2
           AND f3.hzuor = 3
           AND f4.hzuor = 4
           AND f5.hzuor = 5
           AND fs.hityp IN (codHierCust, codHierUser);

     counter_commit number :=0;
     BEGIN

     FOR h IN hier_cur
       LOOP

         update_elab_hierarchy(h.livs, 0);
         update_elab_hierarchy(h.liv0, 0);
         update_elab_hierarchy(h.liv1, 0);
         update_elab_hierarchy(h.liv2, 0);
         update_elab_hierarchy(h.liv3, 0);
         update_elab_hierarchy(h.liv4, 0);
         update_elab_hierarchy(h.liv5, 0);
         counter_commit := counter_commit +1;

         IF counter_commit>10 THEN
           COMMIT;
           counter_commit := 0;
         END IF;

     END LOOP;

     COMMIT;

     UPDATE customer_hierarchy_file f
     SET f.flg_elab = -1
     WHERE f.flg_elab IS NULL;

     COMMIT;

   END;


   PROCEDURE insert_customers_hierarchy
   AS
      formatDateHier   VARCHAR2 (10) := 'YYYYMMDD';
      partyFound       NUMBER := 0;
      xin_TB0042       XIN_TB0042RELATIONS_ST%ROWTYPE;
      tb0042           TB0042RELATIONS_CUST%ROWTYPE;
      countcommit      NUMBER := 0;
      danita_div       VARCHAR(4);
     BEGIN

     DELETE FROM XIN_TB0042RELATIONS_ST;
     commit;
     --pre_elab_customers_hierarchy;

     FOR r IN (SELECT * FROM customer_hierarchy_file)
      LOOP
          danita_div := getDanitaDiv(r.vkorg,r.vtweg,r.spart);
          BEGIN
             SELECT 1
               INTO partyFound
               FROM XIN_TB0042RELATIONS_ST
             WHERE Codnode = r.KUNNR
             AND Coddiv = danita_div
            -- AND codparentnode = r.KUNNR
            group by r.KUNNR;
          EXCEPTION WHEN NO_DATA_FOUND THEN
             partyFound := 0;
          END;

         IF (partyFound = 0) THEN

           xin_TB0042.Coddiv := danita_div;
           xin_TB0042.Coddim := getCodDim(r.hityp);
           xin_TB0042.Codhier := r.hityp;
           xin_TB0042.Codnode := r.KUNNR;
           xin_TB0042.Idlevel := r.hzuor;
           xin_TB0042.Idparentlevel := getIdParent(r.hzuor,r.HKUNNR);
           xin_TB0042.Dtestart := r.DATAB;
           xin_TB0042.Dteend := r.DATBI;
           xin_TB0042.Codparentnode := r.HKUNNR;
           xin_TB0042.Codlevel := r.hzuor;
           xin_TB0042.Codlevelparent := getIdParent(r.hzuor,r.HKUNNR);

           insert into XIN_TB0042RELATIONS_ST values xin_TB0042;
           countcommit := countcommit +1;


            BEGIN

              SELECT *
              INTO tb0042
              FROM TB0042RELATIONS_CUST
              WHERE CODNODE = r.KUNNR
              AND Coddiv = danita_div
              AND CODPARENTNODE <> r.HKUNNR
              AND trunc(DTEEND) = (SELECT trunc(nvl(MAX(DTEEND), to_date('01/01/1970','DD/MM/YYYY')))
                            FROM TB0042RELATIONS_CUST
                            WHERE CODNODE = r.KUNNR);


              xin_TB0042.Coddiv := danita_div;
              xin_TB0042.Coddim := getCodDim(r.hityp);
              xin_TB0042.Codhier := r.hityp;
              xin_TB0042.Codnode := r.KUNNR;
              xin_TB0042.Idlevel := tb0042.idlevel;
              xin_TB0042.Idparentlevel := tb0042.idparentlevel;
              xin_TB0042.Dtestart := to_char(tb0042.dtestart,'MM/DD/YYYY HH24:MI:SS');
              xin_TB0042.Dteend := to_char(to_date(r.DATAB,'MM/DD/YYYY HH24:MI:SS')-1);
              xin_TB0042.Codparentnode := tb0042.CODPARENTNODE;
              xin_TB0042.Codlevel := tb0042.idlevel;
              xin_TB0042.Codlevelparent := tb0042.idparentlevel;

              insert into XIN_TB0042RELATIONS_ST values xin_TB0042;
              countcommit := countcommit +1;

            EXCEPTION WHEN NO_DATA_FOUND THEN
              countcommit := countcommit +1;
            END;



           if countcommit > 100 then
             commit;
             countcommit := 0;
           end if;

         END IF;
      END LOOP;

      commit;
      --delete from customer_hierarchy_file;
      commit;
      --import_master_data('HIERARCHIES');

   END;

   PROCEDURE insert_invoice_payment AS

     tz_ta7091           TZ_TA7091PAYMENTS%ROWTYPE;
     invoiceFound        NUMBER := 0;
     invoiceRefFound     NUMBER := 0;

     BEGIN

     FOR xin IN (SELECT * FROM XIN_TZ_TA7091PAYMENTS) LOOP

         BEGIN
           SELECT att.numord
           INTO invoiceFound
           FROM T660CONT_ATT att
           WHERE att.numord = xin.numord
           GROUP BY att.numord;
         EXCEPTION WHEN NO_DATA_FOUND THEN
            invoiceFound := 0;
         END;

         IF invoiceFound <> 0 THEN

            IF xin.numdocref IS NOT NULL THEN

              BEGIN
               SELECT att.numord
               INTO invoiceFound
               FROM T660CONT_ATT att
               WHERE att.numord = xin.numord
               GROUP BY att.numord;
              EXCEPTION WHEN NO_DATA_FOUND THEN
               invoiceRefFound := 0;
              END;

              IF invoiceRefFound <> 0 THEN
                tz_ta7091 := getInvoicePayment(xin);
              ELSE
                INSERT INTO XIN_TZ_TA7091PAYMENTS_LOG VALUES xin;
              END IF;

            ELSE
             tz_ta7091 := getInvoicePayment(xin);
            END IF;

          INSERT INTO TZ_TA7091PAYMENTS VALUES tz_ta7091;

         ELSE
          INSERT INTO XIN_TZ_TA7091PAYMENTS_LOG VALUES xin;
         END IF;

      END LOOP;

      commit;
      delete from XIN_TZ_TA7091PAYMENTS;
      commit;
   END;

  PROCEDURE insert_suppliers AS

     BEGIN

    delete from xin_ta1056suppliers;
    delete from xin_ta1057supplierdivi;
    commit;

     insert into xin_ta1056suppliers (
            SUPPLIERCODE
            ,DESSUPPLIER
            ,ADDRESS
            ,CITY
            ,ZONECODE
            ,PHONENUMBER
            ,VATCODE
            ,FLGANN
            ,STARTDATE
            ,ENDDATE)
     select t0032.codnode
       ,t0032.desnode
       ,t042.desaddr1
       ,substr(t042.desloc1,0,30)
       ,t042.codzone
       ,t042.numphone1
       ,t040.codvat
       ,t040.flgann
       ,to_char(t0032.dtestart, 'MM/DD/YYYY HH24:MI:SS')
       ,to_char(t0032.dteend, 'MM/DD/YYYY HH24:MI:SS')
     from tb0032hierflatdes_cust t0032
         inner join t040party t040
         on t0032.codnode = t040.codparty
         inner join t042partyaddr t042
         on t040.codparty = t042.codparty
     where nvl(codtitle,0) not in ('AGE','PROP')
     and t0032.coddiv = '4770';

     commit;

     insert into xin_ta1057supplierdivi (SUPPLIERCODE,CODDIV)
     select ta1056.SUPPLIERCODE,'4770' from xin_ta1056suppliers ta1056;

     commit;

     import_master_data('SUPPLIERS');

  END;

  PROCEDURE import_zinvoic02 AS

      tz_invoic02     TZ_INVOIC02_SAP_DOC%ROWTYPE;
      parentkeywf     TA2358WFSTEPS.PARENTKEY%TYPE;

     CURSOR invoic02 IS
        select ZDFRE1EDP02_009.belnr as XTEL_DOCUMENTCODE
              , ZDFRE1EDP02_002.belnr as SAP_NUMORD
              , ZDFRE1EDP02_005.belnr as SAP_DOCUMENTCODE
              , E1EDKA1.PARTN as CODPARTY
              , E1EDK14.ORGID as DOC_TYPE
              , ZDFRE1EDP02_009.DOCNUM
        from XIN_ZINVOIC02_ZDFRE1EDP02 ZDFRE1EDP02_009
          inner join XIN_ZINVOIC02_ZDFRE1EDP02 ZDFRE1EDP02_002
            on ZDFRE1EDP02_009.DOCNUM = ZDFRE1EDP02_002.DOCNUM
            and ZDFRE1EDP02_009.qualf = '009'
            and ZDFRE1EDP02_002.qualf = '002'
          inner join  XIN_ZINVOIC02_ZDFRE1EDP02 ZDFRE1EDP02_005
            on ZDFRE1EDP02_009.DOCNUM = ZDFRE1EDP02_005.DOCNUM
            and ZDFRE1EDP02_005.qualf = '005'
          inner join XIN_ZINVOIC02_E1EDKA1 E1EDKA1
            on ZDFRE1EDP02_009.DOCNUM = E1EDKA1.DOCNUM
            and E1EDKA1.PARVW = 'AG'
          inner join XIN_ZINVOIC02_E1EDK14 E1EDK14
            on ZDFRE1EDP02_009.DOCNUM = E1EDK14.DOCNUM
            and E1EDK14.QUALF = '015';

     BEGIN

      FOR inv IN invoic02 LOOP

        tz_invoic02.XTEL_DOCUMENTCODE := inv.XTEL_DOCUMENTCODE;
        tz_invoic02.SAP_NUMORD := inv.SAP_NUMORD;
        tz_invoic02.SAP_DOCUMENTCODE := inv.SAP_DOCUMENTCODE;
        tz_invoic02.DOC_TYPE := inv.DOC_TYPE;
        tz_invoic02.CODPARTY := inv.CODPARTY;
        tz_invoic02.dtecre := sysdate;

        UPDATE TZ_INVOIC02_SAP_DOC
            SET SAP_NUMORD = tz_invoic02.SAP_NUMORD,
                SAP_DOCUMENTCODE = tz_invoic02.SAP_DOCUMENTCODE
            WHERE XTEL_DOCUMENTCODE = tz_invoic02.XTEL_DOCUMENTCODE
            AND CODPARTY = tz_invoic02.CODPARTY
            AND DOC_TYPE = tz_invoic02.DOC_TYPE;

        IF ( sql%rowcount = 0 ) THEN
           insert into TZ_INVOIC02_SAP_DOC values tz_invoic02;
        END IF;

        commit;

        /* EFFETTUO IL CAMBIO DI STATO DELLE FATTURE RICEVUTE */

        update TA1050PASSIVEINVOICES inv
        set inv.IDWFSTATE = 3
        where inv.DOCUMENTCODE = tz_invoic02.XTEL_DOCUMENTCODE
        and inv.SUPPLIERCODE = tz_invoic02.CODPARTY
        --and inv.IDWFSTATE = 5
        ;

        commit;

        BEGIN
          select 'PassiveInvoice|' || documentid into parentkeywf
          from TA1050PASSIVEINVOICES
          where DOCUMENTCODE = tz_invoic02.XTEL_DOCUMENTCODE
          and SUPPLIERCODE = tz_invoic02.CODPARTY
          and IDWFSTATE = 3;


          Insert into TA2358WFSTEPS
                (PARENTKEY
                ,STEPID
                ,WORKFLOWID
                ,TRANSITIONID
                ,RESULTSTATEID
                ,STEPDATE
                ,COMPLETIONDATE
                ,CODUSR
                ,STEPCOMMENT
                ,CODUSRREAL)
          values (parentkeywf
                  ,(select max(stepid)+1 from TA2358WFSTEPS where parentkey = parentkeywf)
                  ,'2000'
                  ,'4'
                  ,'3'
                  ,sysdate
                  ,sysdate
                  ,'SYS'
                  ,'Ritorno SAP'
                  ,'SYS');

        EXCEPTION WHEN NO_DATA_FOUND THEN
          /* il documento di ritorno non è in xtel */

          UPDATE TZ_INVOIC02_SAP_DOC
            SET SAP_NUMORD = tz_invoic02.SAP_NUMORD,
                SAP_DOCUMENTCODE = tz_invoic02.SAP_DOCUMENTCODE
            WHERE XTEL_DOCUMENTCODE = tz_invoic02.XTEL_DOCUMENTCODE
            AND CODPARTY = tz_invoic02.CODPARTY
            AND DOC_TYPE = 'ERR';
        END;

        delete from XIN_IDOC_CONTROL where docnum = inv.DOCNUM;
        delete from XIN_IDOC_DATA where docnum = inv.DOCNUM;

      END LOOP;

      commit;

      /* update stato fatture passive inviate

      FOR invoic IN (select * from TZ_INVOIC02_SAP_DOC where DOC_TYPE in ('ZZC','ZZD','ZZ3','ZZ4') ) LOOP


        update TA1050PASSIVEINVOICES inv
        set inv.IDWFSTATE = 3
        where inv.DOCUMENTCODE = invoic.XTEL_DOCUMENTCODE
        and inv.SUPPLIERCODE = invoic.CODPARTY
        and inv.IDWFSTATE = 5;

      END LOOP;

     commit;*/

  END;

  PROCEDURE UPDATE_T660_CODCUSTDELIV(tipologia      IN VARCHAR2) IS

   BEGIN

    IF tipologia = 'UPDATE' THEN

      /* SERVE PER SA IN MODO TALE DA AVERE UN DELIV RICONOSCIBILE COME LIVELLO -1 DELLA GERARCHIA CLIENTI */
      update t660cont_att
      set codcustdeliv = codcustdeliv || '_SH', codusr = 'SP_CODCUST'
      where codcustdeliv = codcustinv
      and coddiv = '4770'
      and dteinv > to_date('31/12/2016','DD/MM/YYYY');

      FOR grp IN (select div.Z_GRPPROV, sh.codparty
            from T041PARTYDIV div, (select codparty, rtrim(codparty,'_SH') as codparty_2
                                from T041PARTYDIV
                                where coddiv = '4770'
                                and codparty like '%_SH') sh
            where div.codparty = sh.codparty_2
            and coddiv = '4770')
      LOOP

        UPDATE T041PARTYDIV
        SET Z_GRPPROV = grp.Z_GRPPROV
        WHERE codparty = grp.codparty
        AND coddiv = '4770';

      END LOOP;

      commit;

    ELSIF tipologia = 'ROLLBACK' THEN
      /* EFFETTUA UN ROLLBACK  solo di quelli modificati dalla procedura*/
      update t660cont_att
      set codcustdeliv = codcustinv, codusr = null
      where codcustdeliv like '%_SH'
      and codusr = 'SP_CODCUST';

    END IF;

    commit;

  END;

  PROCEDURE main_import (cdTypeFile VARCHAR2,
                         bFirstInvoiceImport BOOLEAN DEFAULT FALSE)
  AS
    PO_CODPROCESS    NUMBER;
    PO_MSG           VARCHAR2(2000);
    PO_STATUS        NUMBER;
  BEGIN

    IF cdTypeFile = 'HIERARCHIES' THEN
      insert_customers_hierarchy;
    ELSIF cdTypeFile = 'PAYMENT' THEN
      insert_invoice_payment;
    ELSIF cdTypeFile = 'SUPPLIERS' THEN
      insert_suppliers;
    ELSIF cdTypeFile = 'INVOIC02' THEN
      import_zinvoic02;
    ELSIF cdTypeFile = 'UPDATE_DELIV' THEN
      UPDATE_T660_CODCUSTDELIV('UPDATE');
    END IF;

  END;


END;