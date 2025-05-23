-- pegar quantidade de horlders com >=1 cartoes ativos (desbloqueados nao cancelados, para os fisicos, e emitidos nao cancelados para os virtuais) em algum momento do trimestre
DECLARE DS_END_DATE STRING DEFAULT FORMAT_DATE("%Y%m%d", CURRENT_DATE);

WITH 
SubIssuerNames AS (
SELECT
    subIssuerTaxId,
    subIssuerName,
    ROW_NUMBER() OVER(PARTITION BY subIssuerTaxId) AS rowNumber,
FROM
    Card.SubIssuer
WHERE 
    subIssuerTaxid IS NOT NULL
    -- AND subIssuerTaxId = "20.018.183/0001-80"
),
CardExpiration AS (
    SELECT DISTINCT
        issuingCardId,
        cardType,
        "expiration" AS type,
        expiration AS logCreated,
        ROW_NUMBER() OVER(PARTITION BY issuingCardId ORDER BY created ASC) AS rowNumber,
        issuingHolderId,
        IssuingCardLog.subIssuertaxId,
    FROM
        Card.IssuingCardLog
    -- WHERE
    --     IssuingCardLog.subIssuerTaxId = "20.018.183/0001-80"
),
CardEvents AS (
    SELECT
        issuingCardId,
        cardType,
        type,
        logCreated,
        ROW_NUMBER() OVER(PARTITION BY issuingCardId, type ORDER BY logCreated ASC) AS rowNumber,
    FROM
        Card.IssuingCardLog
    WHERE
        type IN ("created", "canceled", "unblocked")
        -- AND IssuingCardLog.subIssuerTaxId = "20.018.183/0001-80"
),
CardEventsAll AS (
    SELECT
        issuingCardId,
        cardType,
        type,
        logCreated,
    FROM
        CardEvents
    WHERE
        rowNumber = 1
    
    UNION ALL

    SELECT
        issuingCardId,
        cardType,
        type,
        logCreated,
    FROM
        CardExpiration
    WHERE
        rowNumber = 1
        AND logCreated <= PARSE_DATE("%Y%m%d", DS_END_DATE)
),
CardEventsCounter AS (
    SELECT
        CardEventsAll.issuingCardId,
        CardEventsAll.cardType,
        CardEventsAll.type,
        SubIssuerNames.subIssuerName,
        CASE
            WHEN CardEventsAll.type = "canceled" OR CardEventsAll.type = "expiration" THEN DATE_ADD(DATE_TRUNC(DATE(CardEventsAll.logCreated), QUARTER), INTERVAL 1 QUARTER)
            ELSE DATE_TRUNC(DATE(CardEventsAll.logCreated), QUARTER)
        END AS logCreated,
        CASE
            WHEN CardEventsAll.type = "created" AND CardEventsAll.cardType = "virtual" THEN 1
            WHEN CardEventsAll.type = "unblocked" AND CardEventsAll.cardType = "physical" THEN 1
            WHEN (CardEventsAll.type = "canceled" OR CardEventsAll.type = "expiration") AND CardEventsAll.cardType = "virtual" THEN -1
            WHEN (CardEventsAll.type = "canceled" OR CardEventsAll.type = "expiration") AND CardEventsAll.cardType = "physical" AND LAST_VALUE(CardEventsAll.type) OVER(PARTITION BY CardEventsAll.issuingCardId ORDER BY CardEventsAll.logCreated ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) = "unblocked" THEN -1
            ELSE 0
        END AS counter,
    FROM
        CardEventsAll
        LEFT JOIN CardExpiration ON CardEventsAll.issuingCardId = CardExpiration.issuingCardId AND rowNumber = 1
        LEFT JOIN SubIssuerNames ON SubIssuerNames.subIssuerTaxId = CardExpiration.subIssuerTaxId AND SubIssuerNames.rowNumber = 1
),
CardActivitySummary AS (
    SELECT
        CardExpiration.issuingHolderId,
        CardExpiration.subIssuertaxId,
        CardEventsCounter.logCreated,
        SUM(CardEventsCounter.counter) as counter,
    FROM
        CardEventsCounter
        LEFT JOIN CardExpiration ON CardEventsCounter.issuingCardId = CardExpiration.issuingCardId AND rowNumber = 1
    GROUP BY ALL
),
CardQuantityAccum AS (
    SELECT
        logCreated,
        issuingHolderId,
        subIssuertaxId,
        SUM(counter) OVER(PARTITION BY issuingHolderId ORDER BY logCreated ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS counter,
        ROW_NUMBER() OVER(PARTITION BY logCreated, issuingHolderId) AS rowNumber,
    FROM
        CardActivitySummary
),
DateTable AS (
    SELECT
        * FROM UNNEST(GENERATE_DATE_ARRAY((SELECT MIN(logCreated) FROM CardQuantityAccum),(SELECT MAX(logCreated) FROM CardQuantityAccum), INTERVAL 1 QUARTER)) AS logCreated
),
CardHolderCollection AS (
    SELECT DISTINCT
        issuingHolderId
    FROM
        CardQuantityAccum
),
CardHolderFull AS (
    SELECT
        DateTable.logCreated,
        CardHolderCollection.issuingHolderId,
        COALESCE (subIssuertaxId, LAST_VALUE(subIssuertaxId IGNORE NULLS) OVER(PARTITION BY CardHolderCollection.issuingHolderId ORDER BY DateTable.logCreated ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS subIssuertaxId,
        COALESCE (counter, LAST_VALUE(counter IGNORE NULLS) OVER(PARTITION BY CardHolderCollection.issuingHolderId ORDER BY DateTable.logCreated ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS counter,
    FROM
        DateTable
        CROSS JOIN CardHolderCollection
        LEFT JOIN CardQuantityAccum ON DateTable.logCreated = CardQuantityAccum.logCreated AND CardHolderCollection.issuingHolderId = CardQuantityAccum.issuingHolderId AND rowNumber = 1
),
HolderQuantityRaw AS (
    SELECT
        logCreated,
        subIssuertaxId,
        COUNT(issuingHolderId) OVER(PARTITION BY subIssuerTaxId, logCreated) AS counter,
        ROW_NUMBER() OVER(PARTITION BY logCreated, subIssuerTaxId) AS rowNumber,
    FROM
        CardHolderFull
    WHERE
        counter > 0            
),
SubIssuers AS (
    SELECT DISTINCT
        subIssuerTaxId
    FROM
        SubIssuerNames
),
HolderQuantity AS (
    SELECT
        DateTable.logCreated,
        SubIssuers.subIssuerTaxId,
        SubIssuerNames.subIssuerName,
        COALESCE (counter, LAST_VALUE(counter IGNORE NULLS) OVER(PARTITION BY SubIssuers.subIssuerTaxId ORDER BY DateTable.logCreated ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS counter,
    FROM
        DateTable
        CROSS JOIN SubIssuers
        LEFT JOIN HolderQuantityRaw ON DateTable.logCreated = HolderQuantityRaw.logCreated AND SubIssuers.subIssuerTaxId = HolderQuantityRaw.SubIssuerTaxId AND HolderQuantityRaw.rowNumber = 1
        LEFT JOIN SubIssuerNames ON SubIssuerNames.subIssuerTaxId = SubIssuers.subIssuerTaxId AND SubIssuerNames.rowNumber = 1
),
cte as (
SELECT
    -- *
    issuingCardId,
    subIssuerName,
    logCreated,
    SUM(counter) OVER(PARTITION BY issuingCardId ORDER BY logCreated ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as acum,
FROM
    -- HolderQuantity
    CardEventsCounter
WHERE 
    subIssuerName = "Astro Instituicao de Pagamento Ltda"
    and logCreated = date("2025-01-01")
--     counter IS NOT NULL
),
cte_2 as (
    select
        status,
        cast(__key__.id  as string) as issuingCardId,
    from
        WinterfellUS.IssuingCard
),
cte_3 as (
    select
    *,
    "da_astro" as origem,
    from
    -- esse deveria colocar no Temp para analisar
    Temp.AstroPay
)
select 
*
from
cte_3
left join cte using(issuingCardId)
left join cte_2 using(issuingCardId)
where
-- origem is null
acum <= 0
-- and logCreated >= date("2024-01-01")