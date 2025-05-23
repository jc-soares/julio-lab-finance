CREATE OR REPLACE TABLE Temp.AstroDivergencesConsolidated
AS (
  WITH
  CardCollection AS (
    SELECT DISTINCT
      issuingCardId,
      issuingHolderId,
      ROW_NUMBER() OVER(PARTITION BY issuingCardid) AS rowNumber,
    FROM
      Card.IssuingCardLog
  )
  SELECT
  AstroDivergencesConsolidatedRaw.issuingCardId,
  CardCollection.issuingHolderId,
  FROM
  Temp.AstroDivergencesConsolidatedRaw
  LEFT JOIN CardCollection ON CardCollection.issuingCardId = AstroDivergencesConsolidatedRaw.issuingCardId AND rowNumber = 1
)