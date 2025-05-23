WITH
-- TODO: criar data do purchaseConfirmed

Purchases AS (
  SELECT
    type,
    purchase.subIssuerId,
    purchase.holderId,
    purchase.cardId,
    endToEndId,
    walletId,
    -- purchaseId AS issuingPurchaseId,
    -- TODO: esse deve mudar por conta da mudança de dataset de origem
    DATE(purchase.created, "America/Sao_Paulo") AS purchaseCreated,
    -- DATE(purchase.confirmed, "America/Sao_Paulo") AS purchaseConfirmed,
    logCreated,
    -- cycle,
    -- purchase.networkCurrencyCode,
    purchase.merchantCurrencyCode,
    -- purchase.merchantCountryCode,
    -- purchase.merchantCategoryCode,
    IF(type = "reversed", -1, 1) * purchase.logAmount AS logAmount,
    -- purchase.issuerAmount,
    -- purchase.merchantAmount,
    -- purchase.networkAmount,
    -- purchase.fee,
    -- purchase.merchantFee,
    -- purchase.currencyFee,
    -- purchase.tax,
    purchase.purpose,
    purchase.methodCode,
  FROM
    Card.IssuingPurchaseLog
  WHERE
    type IN ("confirmed", "reversed")
),

-- TODO: checar a partir daqui
CalculatedColumns AS (
  SELECT
    *,
    IF(networkAmount = 0, 0, ROUND(issuerAmount / networkAmount, 4)) AS conversionRate,
    IF(merchantCurrencyCode = "BRL", 0, IF(DATE(purchaseConfirmed) < "2022-03-01", 0.04, 0.02)) AS spread,
    IF(cycle = 1, DATE_ADD(purchaseConfirmed, INTERVAL -1 DAY), purchaseConfirmed) AS purchaseConciliated,
  FROM
    Purchases
),
ConvertedColumns AS (
  SELECT
    * EXCEPT(merchantFee, cycle),
    merchantFee AS merchantFeeBRL,
    ROUND(fee * conversionRate, 2) AS feeBRL,
    ROUND(conversionRate * (1 + spread), 4) AS conversionRateIssuer,
  FROM
    CalculatedColumns
),
WorkpaceInfo AS (
  SELECT
    ConvertedColumns.*,
    CorporateHolder.workspaceId,
    CorporateHolder.name AS holderName,
    CorporateHolder.centerId,
    Workspace.companyName,
    Workspace.companyTaxId,
  FROM
    ConvertedColumns
    LEFT JOIN WinterfellUS.CorporateHolder ON holderId = issuingHolderId
    LEFT JOIN General.Workspace USING(workspaceId)
),
CardInfo AS (
  SELECT
    WorkpaceInfo.*,
    IssuingCard.binId,
    IssuingCard.type AS cardType,
  FROM
    WorkpaceInfo
    LEFT JOIN WinterfellUS.IssuingCard ON cardId = CAST(IssuingCard.__key__.id AS STRING)
),
SubIssuer AS (
  SELECT
    CAST(__key__.id AS STRING) AS subIssuerId,
    workspaceId AS subIssuerWorkspaceId,
    Workspace.companyName AS subIssuerName,
  FROM
    WinterfellUS.SubIssuer
    LEFT JOIN General.Workspace USING(workspaceId)
)
SELECT
  CardInfo.*,
  SubIssuer.* EXCEPT(subIssuerId),
  MerchantCategory.number AS merchantCategoryNumber,
FROM
  CardInfo
  LEFT JOIN SubIssuer USING(subIssuerId)
  LEFT JOIN WinterfellUS.MerchantCategory ON CardInfo.merchantCategoryCode = MerchantCategory.code


  -- Esse codigo eh a base para a tabela usada pelo codigo abaixo:

  SELECT
  date_trunc(purchaseCreated, week) as week_transactionDate,
  date_diff(purchaseCreated, CURRENT_DATE("America/Sao_Paulo"),week)*-1 as week_delay,
concat(format_date("%d/%b",date_trunc(purchaseCreated,week)),' to ', format_date("%d/%b",date_add(date_trunc(purchaseCreated,week), interval 6 day))) as week_desc,
concat(date_diff(purchaseCreated, CURRENT_DATE("America/Sao_Paulo"),week)*-1,". ",format_date("%d/%b",date_trunc(purchaseCreated,week)),' to ', format_date("%d/%b",date_add(date_trunc(purchaseCreated,week), interval 6 day))) as week_desc_concat,
  subIssuerName,
  networkCurrencyCode,
  purpose,
  case 
    when cardType = 'physical' then 'Physical'
    when cardType = 'virtual' then 'Virtual'
  end as cardType,
  case when networkCurrencyCode = "BRL" then sum(amount)/100 end as nationalTPV,
  case when networkCurrencyCode != "BRL" then sum(amount)/100 end as internationalTPV,
  case when networkCurrencyCode = "BRL" then 0.005*sum(amount)/100 + 25/100 end as nationalRevenue,
  case when networkCurrencyCode != "BRL" then 0.014*sum(amount)/100 + 50/100 end as internationalRevenue,
  case when networkCurrencyCode = "BRL" then COUNT(issuingPurchaseId) end as nationalTRX,
  case when networkCurrencyCode != "BRL" then COUNT(issuingPurchaseId) end as internationalTRX,
  sum(amount)/100 as totalTPV,
  case 
    when networkCurrencyCode = "BRL" then 0.005*sum(amount)/100 + 25/100
    when networkCurrencyCode != "BRL" then 0.014*sum(amount)/100 + 50/100
  end as totalRevenue,
  COUNT(issuingPurchaseId) as totalTRX
FROM Accounting.PurchaseConciliation
WHERE subIssuerId != "5634161670881280"
--AND companyTaxId not in ('43.853.248/0001-08','49.411.582/0001-98','38.232.533/0001-71','20.018.183/0001-80')
group by 1,2,3,4,5,6,7,8