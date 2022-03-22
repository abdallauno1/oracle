CREATE OR REPLACE FUNCTION get_product_code   (v_coddiv IN VARCHAR2,
                                                v_prod_code IN VARCHAR2)
  RETURN VARCHAR2 DETERMINISTIC PARALLEL_ENABLE

IS

  v_erp_prod VARCHAR2(255);

  BEGIN
      SELECT erp_prod into v_erp_prod
      FROM tz_erp_sm1 
      WHERE sm1_prod = v_prod_code
      AND coddiv = v_coddiv;

      RETURN v_erp_prod;
      
  EXCEPTION
      WHEN OTHERS THEN
          RETURN v_prod_code;
          
END get_product_code;