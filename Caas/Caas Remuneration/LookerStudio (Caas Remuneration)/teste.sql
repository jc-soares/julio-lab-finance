DECLARE DS_START_DATE STRING DEFAULT "20250101";
DECLARE DS_END_DATE STRING DEFAULT "20250331";

WITH 
  PurchasesLog AS (
    SELECT
      ROW_NUMBER() OVER(PARTITION BY endToEndId, type ORDER BY logCreated ASC) AS rowNumber,
      endToEndId,
      COALESCE(DATE(purchaseConfirmed), DATE(logCreated)) AS logCreated,
      -- IssuingCard.subIssuerTaxId,
      -- logAmount,
      type,
      DATE(purchaseConfirmed) AS purchaseConfirmed,
      last_value(if(type = "reversed", null, date(logCreated)) ignore nulls) over(coisa) as data_arrumada,
      last_value(if(type = "reversed", null, issuingPurchaseLogId) ignore nulls) over(coisa) as issuingPurchaseLogIdAnterior,
    FROM
      Card.IssuingPurchaseLog
      LEFT JOIN Card.IssuingCard USING(issuingCardId)
    WHERE
      type IN ("confirmed","reversed")
      AND purpose = "purchase"
      AND merchantCurrencyCode = "BRL"
      AND IssuingCard.subIssuerTaxId = "37.008.710/0001-78" 
      AND DATE(logCreated) BETWEEN DATE_SUB(PARSE_DATE("%Y%m%d", DS_START_DATE), INTERVAL 27 DAY) AND PARSE_DATE("%Y%m%d", DS_END_DATE)
    window coisa as (
      partition by endToEndId
      order by logCreated ASC
      rows between unbounded preceding and current row)
  )
select
  PurchasesLog.*,
  issuingPurchaseLog.type as typeAntigo,
  issuingPurchaseLog.logCreated as dataAntiga,
from
  PurchasesLog
  left join Card.IssuingPurchaseLog on IssuingPurchaseLog.issuingPurchaseLogId = PurchasesLog.issuingPurchaseLogIdAnterior
where 
PurchasesLog.type = "reversed"