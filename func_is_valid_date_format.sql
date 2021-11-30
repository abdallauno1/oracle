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