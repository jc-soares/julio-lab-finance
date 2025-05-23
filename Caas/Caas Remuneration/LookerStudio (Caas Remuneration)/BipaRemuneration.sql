DECLARE DS_START_DATE STRING DEFAULT "20250101";
DECLARE DS_END_DATE STRING DEFAULT CAST(CURRENT_DATE AS STRING FORMAT "YYYYMMDD");

WITH 
  PurchasesLogRaw AS (
    SELECT
      endToEndId,
      DATE(logCreated) AS logCreated,
      -- LAST_VALUE(IF(type = "confirmed", IF(DATE(purchaseConfirmed) <= DATE_SUB(DATE(logCreated), INTERVAL 27 DAY), DATE(logCreated), DATE(purchaseConfirmed)),NULL) IGNORE NULLS) OVER(PARTITION BY endToEndId ORDER BY logCreated ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS purchaseConfirmed,
      LAST_VALUE(IF(type = "confirmed", DATE(logCreated), NULL) IGNORE NULLS) OVER(PARTITION BY endToEndId ORDER BY logCreated ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS purchaseConfirmed,
      IssuingCard.subIssuerTaxId,
      logAmount,
      type,
      -- DATE(purchaseConfirmed) AS purchaseConfirmedRaw,
    FROM
      Card.IssuingPurchaseLog
      LEFT JOIN Card.IssuingCard USING(issuingCardId)
    WHERE
      type IN ("confirmed","reversed")
      AND purpose = "purchase"
      AND merchantCurrencyCode = "BRL"
      AND IssuingCard.subIssuerTaxId = "37.008.710/0001-78" 
      AND DATE(logCreated) BETWEEN DATE_SUB(PARSE_DATE("%Y%m%d", DS_START_DATE), INTERVAL 27 DAY) AND PARSE_DATE("%Y%m%d", DS_END_DATE)
  ),
  PurchasesLog AS (
    SELECT
      *,
      CONCAT(endToEndId, CAST(purchaseConfirmed AS STRING)) AS InternalId,
      ROW_NUMBER() OVER(PARTITION BY CONCAT(endToEndId, CAST(purchaseConfirmed AS STRING)), type ORDER BY logCreated ASC) AS rowNumber,
    FROM
      PurchasesLogRaw
  ),
  ConfirmationDates AS (
    SELECT
      InternalId,
      endToEndId,
      purchaseConfirmed AS confirmationDate,
      -- purchaseConfirmedRaw,
      DATE_ADD(purchaseConfirmed, INTERVAL 25 DAY) AS dueDate,
      DATE_ADD(purchaseConfirmed, INTERVAL 27 DAY) AS settlementDate,
    FROM
      PurchasesLog
    WHERE 
      rowNumber = 1
      AND type = "confirmed"
  ),
  PurchasesCollection AS (
    SELECT DISTINCT
      InternalId,
      endToEndId,
    FROM
      ConfirmationDates
  ),
  ReversedLogs AS (
    SELECT
      PurchasesLog.internalId,
      PurchasesLog.endToEndId,
      logAmount,
      purchaseConfirmed AS confirmationDate,
      logCreated,
      IF(logCreated < ConfirmationDates.settlementDate, "reversedBeforeSettlement", "reversedAfterSettlement") AS reverstionSituation,
    FROM
      PurchasesLog
      LEFT JOIN ConfirmationDates USING(internalId)
    WHERE 
      type = "reversed"
  ),
  ReversedBeforeSettlement AS (
    SELECT
      PurchasesCollection.internalId,
      SUM(logAmount) AS reversedAmountBeforeSettlement,
    FROM
      PurchasesCollection
      LEFT JOIN ReversedLogs ON PurchasesCollection.InternalId = ReversedLogs.InternalId AND reverstionSituation = "reversedBeforeSettlement"
    GROUP BY ALL
  ),
  ReversedAfterSettlement AS (
    SELECT
      PurchasesCollection.InternalId,
      SUM(logAmount) AS reversedAmountAfterSettlement,
    FROM
      PurchasesCollection
      LEFT JOIN ReversedLogs ON PurchasesCollection.InternalId = ReversedLogs.InternalId AND reverstionSituation = "reversedAfterSettlement"
    GROUP BY ALL
  ),
  Confirmed AS (
    SELECT
      PurchasesCollection.InternalId,
      SUM(logAmount) AS confirmedAmount,
    FROM
      PurchasesCollection
      LEFT JOIN PurchasesLog ON PurchasesCollection.internalId = PurchasesLog.internalId AND type = "confirmed"
    GROUP BY ALL
  ),
  DateRange AS (
    SELECT
      *
    FROM
      UNNEST(GENERATE_DATE_ARRAY(PARSE_DATE("%Y%m%d", DS_START_DATE), PARSE_DATE("%Y%m%d", DS_END_DATE))) referenceDate
  ),
  DateRangeExpanded AS (
    SELECT
      *
    FROM
      UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(PARSE_DATE("%Y%m%d", DS_START_DATE),INTERVAL 27 DAY), DATE_ADD(PARSE_DATE("%Y%m%d", DS_END_DATE), INTERVAL 27 DAY))) referenceDate
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
      `api-ms-data-sbx.Winterfell.IndexCdi` IndexCdi
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
      LEFT JOIN `api-ms-data-sbx.Winterfell.IndexCdi` IndexCdi ON DateRangeExpanded.referenceDate = DATE(IndexCdi.referenceDate)
      LEFT JOIN Holidays ON DateRangeExpanded.referenceDate = Holidays.referenceDate
  ),
  CdiReturnRaw AS (
      SELECT
        referenceDate,
        EXP(SUM(LN(indexCdi)) OVER (ORDER BY referenceDate ROWS BETWEEN 27 PRECEDING AND 3 PRECEDING)) AS cdiAcum,
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
  )
SELECT
  PurchasesCollection.internalId,
  PurchasesCollection.endToEndId,
  -- ConfirmationDates.purchaseConfirmedRaw,
  ConfirmationDates.confirmationDate,
  ConfirmationDates.dueDate,
  ConfirmationDates.settlementDate,
  Confirmed.confirmedAmount,
  ReversedBeforeSettlement.reversedAmountBeforeSettlement,
  ReversedAfterSettlement.reversedAmountAfterSettlement,
  CdiReturn.cdiAcum,
  confirmedAmount - COALESCE(reversedAmountBeforeSettlement, 0) AS NetAmountBeforeSettlement,
  CAST(ROUND((COALESCE((Confirmed.confirmedAmount - COALESCE(ReversedBeforeSettlement.reversedAmountBeforeSettlement,0)) * CdiReturn.cdiAcum - (Confirmed.confirmedAmount - COALESCE(ReversedBeforeSettlement.reversedAmountBeforeSettlement,0)), 0))* 0.6) AS INT64) AS remuneration,
  GREATEST(CAST(ROUND((COALESCE((Confirmed.confirmedAmount - COALESCE(ReversedBeforeSettlement.reversedAmountBeforeSettlement,0)) * CdiReturn.cdiAcum - (Confirmed.confirmedAmount - COALESCE(ReversedBeforeSettlement.reversedAmountBeforeSettlement,0)), 0))* 0.6) AS INT64), 0) AS nonNegativeRemuneration,
FROM
  PurchasesCollection
  LEFT JOIN ConfirmationDates USING(internalId)
  LEFT JOIN Confirmed USING(internalId)
  LEFT JOIN ReversedBeforeSettlement USING(internalId)
  LEFT JOIN ReversedAfterSettlement USING(internalId)
  LEFT JOIN CdiReturn ON ConfirmationDates.settlementDate = CdiReturn.referenceDate