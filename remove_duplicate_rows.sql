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