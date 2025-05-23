-- pegar quantidade de horlders com >=1 cartoes ativos (desbloqueados nao cancelados, para os fisicos, e emitidos nao cancelados para os virtuais) em algum momento do trimestre
DECLARE DS_END_DATE STRING DEFAULT FORMAT_DATE("%Y%m%d", CURRENT_DATE);

CREATE OR REPLACE TABLE Temp.AstroDivergences AS (
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
    ),
    CardExpiration AS (
        SELECT DISTINCT
            issuingCardId,
            cardType,
            "expiration" AS type,
            expiration AS logCreated,
            ROW_NUMBER() OVER(PARTITION BY issuingCardId ORDER BY logCreated ASC) AS rowNumber,
            issuingHolderId,
            SubIssuerNames.subIssuertaxId,
        FROM
            Card.IssuingCardLog
            LEFT JOIN SubIssuerNames ON SubIssuerNames.subIssuerTaxId = IssuingCardLog.subIssuerTaxId AND SubIssuerNames.rowNumber = 1
        WHERE
            SubIssuerNames.subIssuerName = "Astro Instituicao de Pagamento Ltda"            
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
            LEFT JOIN SubIssuerNames ON SubIssuerNames.subIssuerTaxId = IssuingCardLog.subIssuerTaxId AND SubIssuerNames.rowNumber = 1
        WHERE
            type IN ("created", "canceled", "unblocked")
            AND SubIssuerNames.subIssuerName = "Astro Instituicao de Pagamento Ltda"            
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
    -- arrumar essa condicao            
                WHEN (CardEventsAll.type = "canceled" OR CardEventsAll.type = "expiration") AND CardEventsAll.cardType = "physical" AND LAST_VALUE(CardEventsAll.type) OVER(PARTITION BY CardEventsAll.issuingCardId ORDER BY CardEventsAll.logCreated ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) = "unblocked" THEN -1
                ELSE 0
            END AS counter,
        FROM
            CardEventsAll
            LEFT JOIN CardExpiration ON CardEventsAll.issuingCardId = CardExpiration.issuingCardId AND rowNumber = 1
            LEFT JOIN SubIssuerNames ON SubIssuerNames.subIssuerTaxId = CardExpiration.subIssuerTaxId AND SubIssuerNames.rowNumber = 1
    ),
    DateTable AS (
        SELECT
            -- * FROM UNNEST(GENERATE_DATE_ARRAY((SELECT MIN(logCreated) FROM CardEventsCounter),(SELECT MAX(logCreated) FROM CardEventsCounter), INTERVAL 1 QUARTER)) AS logCreated
            * FROM UNNEST(GENERATE_DATE_ARRAY((SELECT MIN(logCreated) FROM CardEventsCounter),DATE("2025-01-01"), INTERVAL 1 QUARTER)) AS logCreated
    ),
    CardCollectionRaw AS (
        SELECT
            issuingCardId,
            subIssuerName,
            cardType,
            ROW_NUMBER() OVER(PARTITION BY issuingCardId, subIssuerName, cardType) AS rowNumber,
        FROM
            CardEventsCounter
    ),
    CardCollection AS (
        SELECT
            issuingCardId,
            subIssuerName,
            cardType,
        FROM
            CardCollectionRaw
        WHERE
            rowNumber = 1
    ),
    AstroUntilQuarter AS (
        SELECT
            issuingCardId,
            subIssuerName,
            cardType,
            logCreated,
            counter,
        FROM
            CardEventsCounter
        WHERE 
            logCreated <= DATE("2025-01-01")
    ),
    AstroUntilQuarterAccum AS (
        SELECT
            issuingCardId,
            subIssuerName,
            cardType,
            logCreated,
            SUM(counter) OVER(PARTITION BY issuingCardId ORDER BY counter DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS accum,
        FROM
            AstroUntilQuarter
    ),
    AstroUntilQuarterAccumRow AS (
        SELECT
            issuingCardId,
            subIssuerName,
            cardType,
            logCreated,
            accum,
            ROW_NUMBER() OVER(PARTITION BY issuingCardId, logCreated ORDER BY accum DESC) AS rowNumber,
        FROM
            AstroUntilQuarterAccum
    ),
    AstroUntilQuarterAccumUnique AS (
        SELECT
            issuingCardId,
            subIssuerName,
            cardType,
            logCreated,
            accum,
        FROM
            AstroUntilQuarterAccumRow
        WHERE
            rowNumber = 1
    ),
    CompleteTableQuarters AS (
        SELECT
            DateTable.logCreated,
            CardCollection.issuingCardId,
            CardCollection.subIssuerName,
            CardCollection.cardType,
            COALESCE(accum, LAST_VALUE(accum IGNORE NULLS) OVER(PARTITION BY CardCollection.issuingCardId ORDER BY DateTable.logCreated ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS accum,
        FROM
            DateTable
            CROSS JOIN CardCollection
            LEFT JOIN AstroUntilQuarterAccumUnique USING(issuingCardId, logCreated)
    ),
    AstroQuarter AS (
        SELECT
            issuingCardId,
            subIssuerName,
            cardType,
            logCreated,
            accum,
        FROM
            CompleteTableQuarters
        WHERE 
            logCreated = DATE("2025-01-01")
    ),
    StatusTable AS (
        SELECT
            status,
            CAST(__key__.id  AS STRING) AS issuingCardId,
            ROW_NUMBER() OVER(PARTITION BY __key__.id ORDER BY created ASC) AS rowNumber,
        FROM
            WinterfellUS.IssuingCard
    ),
    CardsFromAstro AS (
        SELECT
            *,
            "da_astro" AS origem,
        FROM
            Temp.AstroPay
    ),

    --olhar a partir daqui
    UnmatchedFromAstro AS (
        SELECT DISTINCT
            CardsFromAstro.*,
            ROW_NUMBER() OVER(PARTITION BY CardsFromAstro.issuingCardId) AS rowNumber,
            COALESCE(AstroQuarter.accum, -1000) AS accum,
            AstroQuarter.cardType,
            StatusTable.status,
        FROM
            CardsFromAstro
            LEFT JOIN AstroQuarter USING(issuingCardId)
            LEFT JOIN StatusTable ON CardsFromAstro.issuingCardId = StatusTable.issuingCardId AND rowNumber = 1 
        WHERE
            accum <= 0 
    ),
    UnmatchedFromUs AS (
        SELECT
            AstroQuarter.issuingCardId,
            CardsFromAstro.origem,
            ROW_NUMBER() OVER(PARTITION BY AstroQuarter.issuingCardId) AS rowNumber,
            AstroQuarter.accum,
            AstroQuarter.cardType,
            StatusTable.status,
        FROM
            AstroQuarter
            LEFT JOIN CardsFromAstro USING(issuingCardId)
            LEFT JOIN StatusTable USING(issuingCardId)
        WHERE
            accum > 0 
            AND origem IS NULL
    ),
    UnmatchedTotal AS (
        SELECT
            *
        FROM
            UnmatchedFromAstro
 
        UNION ALL
 
        SELECT
            *
        FROM
            UnmatchedFromUs
    ),
    FilteredUnmatched AS (
        SELECT
            *EXCEPT(rowNumber)
        FROM
            UnmatchedTotal
        WHERE
            rowNumber = 1
    )
    SELECT
        *
    FROM
        FilteredUnmatched
)