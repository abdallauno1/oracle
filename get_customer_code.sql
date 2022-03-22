CREATE OR REPLACE FUNCTION get_customer_code (v_coddiv IN VARCHAR2,
                                             v_cust_code IN VARCHAR2)
  RETURN VARCHAR2 DETERMINISTIC PARALLEL_ENABLE

IS

  v_erp_cust VARCHAR2(255);

  BEGIN
       
      SELECT erp_cust into v_erp_cust 
      FROM tz_erp_sm1 
      WHERE sm1_cust = v_cust_code
      AND coddiv = v_coddiv;
      RETURN v_erp_cust;
      
  EXCEPTION
      WHEN OTHERS THEN
          RETURN v_cust_code;
          
END get_customer_code;