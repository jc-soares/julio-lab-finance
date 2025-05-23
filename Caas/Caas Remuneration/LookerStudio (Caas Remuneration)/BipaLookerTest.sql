DECLARE DS_START_DATE STRING DEFAULT "20250101";
DECLARE DS_END_DATE STRING DEFAULT CURRENT_DATE;

WITH 
  PurchasesLog AS 
    SELECT
      ROW_NUMBER() OVER(PARTITION BY endToEndId, type ORDER BY logCreated ASC) AS rowNumber,
      endToEndId,
      logCreated,
      IssuingCard.subIssuerTaxId,
      logAmount,
      type,
      purchaseConfirmed,
    FROM
      Card.IssuingPurchaseLog
      LEFT JOIN Card.IssuingCard USING(issuingCardId)
    WHERE
      type IN ("confirmed","reversed")
      AND purpose = "purchase"
      AND merchantCurrencyCode = "BRL"
      AND IssuingCard.subIssuerTaxId != "20.018.183/0001-80" 
      AND DATE(logCreated) BETWEEN DATE_SUB(PARSE_DATE("%Y%m%d", DS_START_DATE), INTERVAL 27 DAY) AND PARSE_DATE("%Y%m%d", DS_END_DATE)
    --   AND DATE(logCreated) BETWEEN DATE_SUB(PARSE_DATE("%Y%m%d", @DS_START_DATE), INTERVAL 27 DAY) AND PARSE_DATE("%Y%m%d", @DS_END_DATE)
  ),
  ConfirmationDates AS (
    SELECT
      endToEndId,
      purchaseConfirmed AS confirmationDate,
      DATE_ADD(logCreated, INTERVAL 25 DAY) AS dueDate,
      DATE_ADD(logCreated, INTERVAL 27 DAY) AS settlementDate,
    FROM
      PurchasesLog
    WHERE 
      rowNumber = 1
      AND type = "confirmed"
  ),
  Confirmed AS (
    SELECT
      SUM(logAmount) as logAmount,
      DATE(dueDate) as referenceDate,
      subIssuerTaxId,
    FROM
      PurchasesLog
      LEFT JOIN ConfirmationDates USING(endToEndId)
    WHERE
      type = "confirmed"
    GROUP BY ALL
  ),
  ReversedBeforeSettlement AS (
    SELECT
      SUM(logAmount) as logAmount,
      DATE(dueDate) as referenceDate,
      subIssuerTaxId,
    FROM
      PurchasesLog
      LEFT JOIN ConfirmationDates USING(endToEndId)
    WHERE
      type = "reversed"
      AND logCreated BETWEEN confirmationDate AND settlementDate
    GROUP BY ALL
  ),
  ReversedAfterSettlement AS (
    SELECT
      SUM(logAmount) as logAmount,
      DATE(logCreated) as referenceDate,
      subIssuerTaxId,
    FROM
      PurchasesLog
      LEFT JOIN ConfirmationDates USING(endToEndId)
    WHERE
      type = "reversed"
      AND logCreated > settlementDate
    GROUP BY ALL
  ),
  DateRange AS (
    SELECT
      *
    FROM
      UNNEST(GENERATE_DATE_ARRAY(PARSE_DATE("%Y%m%d", DS_START_DATE), PARSE_DATE("%Y%m%d", DS_END_DATE))) referenceDate
    --   UNNEST(GENERATE_DATE_ARRAY(PARSE_DATE("%Y%m%d", @DS_START_DATE), PARSE_DATE("%Y%m%d", @DS_END_DATE))) referenceDate
  ),
  DateRangeExpanded AS (
    SELECT
      *
    FROM
      UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(PARSE_DATE("%Y%m%d", DS_START_DATE),INTERVAL 27 DAY), DATE_ADD(PARSE_DATE("%Y%m%d", DS_END_DATE), INTERVAL 27 DAY))) referenceDate
      -- UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(PARSE_DATE("%Y%m%d", @DS_START_DATE),INTERVAL 27 DAY), DATE_ADD(PARSE_DATE("%Y%m%d", @DS_END_DATE), INTERVAL 27 DAY))) referenceDate
  ),
  Holidays AS (
    SELECT
      referenceDate,
      IF(HolidayBR.holidayDate IS NOT NULL OR EXTRACT(DAYOFWEEK FROM referenceDate) = 1 OR EXTRACT(DAYOFWEEK FROM referenceDate) = 7, FALSE, TRUE) AS isWorkingday,
    FROM
      DateRangeExpanded
      LEFT JOIN GeneralTeam.HolidayBR ON DateRangeExpanded.referenceDate = HolidayBR.holidayDate
  ),
  LastCdi AS (
    SELECT
      value AS indexCdi,
      MAX(DATE(referenceDate,"America/Sao_Paulo")) OVER() AS maxDate,
    FROM
      Winterfell.IndexCdi
    WHERE
      DATE(referenceDate,"America/Sao_Paulo") <= current_date("America/Sao_Paulo")
      AND value != 1
    ORDER BY 
      referenceDate DESC
    LIMIT 1
  ),
  CdiReference AS (
    SELECT
      DateRangeExpanded.referenceDate,
      IF(DateRangeExpanded.referenceDate > (SELECT maxDate FROM LastCdi), IF(Holidays.isWorkingday = FALSE, 1, (SELECT indexCdi FROM LastCdi)), COALESCE(IndexCdi.value,1)) AS indexCdi,
      IF(DateRangeExpanded.referenceDate > (SELECT maxDate FROM LastCdi), TRUE, FALSE) AS isCdiForecast,
    FROM
      DateRangeExpanded
      LEFT JOIN Winterfell.IndexCdi ON DateRangeExpanded.referenceDate = DATE(IndexCdi.referenceDate)
      LEFT JOIN Holidays ON DateRangeExpanded.referenceDate = Holidays.referenceDate
  ),
  CdiReturnRaw AS (
      SELECT
        referenceDate,
        EXP(SUM(LN(indexCdi)) OVER (ORDER BY referenceDate ROWS BETWEEN 25 PRECEDING AND 1 PRECEDING)) AS cdiAcum,
    	indexCdi,
      isCdiForecast,
      FROM
        CdiReference
  ),
  CdiReturn AS (
    SELECT
      referenceDate,
      cdiAcum,
      indexCdi,
      isCdiForecast,
    FROM
      CdiReturnRaw
    WHERE
      referenceDate >= (SELECT MIN(referenceDate) FROM DateRange)
  ),
  SubIssuers AS (
    SELECT DISTINCT
      subIssuerTaxId,
    FROM
      PurchasesLog
  ),
  DateRangeSubIssuers AS (
    SELECT
      referenceDate,
      subIssuerTaxId,
    FROM
      DateRange
      CROSS JOIN SubIssuers
  ),
  SubIssuerNames AS (
    SELECT
      subIssuerTaxId,
      subIssuerName,
      ROW_NUMBER() OVER(PARTITION BY subIssuerTaxId) AS rowNumber,
    FROM
      Card.SubIssuer
  ),
  Remuneration AS (
    SELECT
      DateRangeSubIssuers.referenceDate,
      DateRangeSubIssuers.subIssuerTaxId,
      SubIssuerNames.subIssuerName,
      CdiReturn.cdiAcum,
      CdiReturn.indexCdi,
      CdiReturn.isCdiForecast,
      COALESCE(Confirmed.logAmount,0) - COALESCE(ReversedBeforeSettlement.logAmount,0) AS baseValue,
      CAST(ROUND((COALESCE(Confirmed.logAmount * CdiReturn.cdiAcum - Confirmed.logAmount, 0) - COALESCE(ReversedBeforeSettlement.logAmount * CdiReturn.cdiAcum - ReversedBeforeSettlement.logAmount, 0))* 0.6) AS INT64) AS remuneration,
      COALESCE(Confirmed.logAmount,0) AS confirmed,
      COALESCE(ReversedBeforeSettlement.logAmount,0) AS reversedBeforeSettlement,
      COALESCE(ReversedAfterSettlement.logAmount,0) AS reversedAfterSettlement,
      CAST(ROUND((COALESCE(Confirmed.logAmount * CdiReturn.cdiAcum - Confirmed.logAmount, 0))* 0.6) AS INT64) AS remunerationConfirmed,
      CAST(ROUND((COALESCE(ReversedBeforeSettlement.logAmount * CdiReturn.cdiAcum - ReversedBeforeSettlement.logAmount, 0))* 0.6) AS INT64) AS remunerationReversedBeforeSettlement,
    FROM
      DateRangeSubIssuers
      LEFT JOIN Confirmed USING(referenceDate, subIssuerTaxId)
      LEFT JOIN ReversedBeforeSettlement USING(referenceDate, subIssuerTaxId)
      LEFT JOIN ReversedAfterSettlement USING(referenceDate, subIssuerTaxId)
      LEFT JOIN CdiReturn USING(referenceDate)
      LEFT JOIN SubIssuerNames USING(subIssuerTaxId)
    WHERE 
      rowNumber = 1
  )
SELECT
  *
FROM
  Remuneration
