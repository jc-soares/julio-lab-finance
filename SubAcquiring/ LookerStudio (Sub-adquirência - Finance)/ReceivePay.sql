SELECT
  *
FROM
  Finance.Table_LookerStudio_SubAcquiring_ReceivePay
WHERE
  referenceDate BETWEEN PARSE_DATE("%Y%m%d",@DS_START_DATE) AND PARSE_DATE("%Y%m%d",@DS_END_DATE)