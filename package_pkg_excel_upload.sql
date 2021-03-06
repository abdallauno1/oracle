CREATE OR REPLACE PACKAGE PKG_EXCEL_UPLOAD IS

FUNCTION IS_NUMBER( p_str IN VARCHAR2 )
  RETURN VARCHAR2 DETERMINISTIC PARALLEL_ENABLE;

FUNCTION is_valid_date_format( d_format in VARCHAR2 )
  RETURN VARCHAR2 DETERMINISTIC PARALLEL_ENABLE;

PROCEDURE SP_UPLOAD_B2B_C (VERRORCODE OUT VARCHAR2 );
PROCEDURE SP_VALIDATE_B2B_C (VERRORCODE OUT VARCHAR2 );

PROCEDURE SP_UPLOAD_SICK_LEAVE_C (VERRORCODE OUT VARCHAR2 );
PROCEDURE SP_VALIDATE_SICK_LEAVE_C (VERRORCODE OUT VARCHAR2);

PROCEDURE SP_UPLOAD_SUP_RATE_C (VERRORCODE OUT VARCHAR2 );
PROCEDURE SP_VALIDATE_SUP_RATE_C (VERRORCODE OUT VARCHAR2);

-- TARGET PROCEDURES --
PROCEDURE SP_UPLOAD_B2B_TRG_C (VERRORCODE OUT VARCHAR2 );
PROCEDURE SP_VALIDATE_B2B_TRG_C (VERRORCODE OUT VARCHAR2 );

PROCEDURE SP_UPLOAD_B2C_TRG_C (VERRORCODE OUT VARCHAR2 );
PROCEDURE SP_VALIDATE_B2C_TRG_C (VERRORCODE OUT VARCHAR2 );

PROCEDURE SP_UPLOAD_MERCH_TRG_C (VERRORCODE OUT VARCHAR2 );
PROCEDURE SP_VALIDATE_MERCH_TRG_C (VERRORCODE OUT VARCHAR2 );

PROCEDURE SP_UPLOAD_SA_TRG_C (VERRORCODE OUT VARCHAR2 );
PROCEDURE SP_VALIDATE_SA_TRG_C (VERRORCODE OUT VARCHAR2 );

END PKG_EXCEL_UPLOAD;