CREATE OR REPLACE TABLE Temp.AstroDivergencesFromUsDetailed
AS (
WITH
DivergenceFromAstro AS (
  SELECT 
    *
  FROM 
    Temp.AstroDivergences 
  WHERE 
    origem IS NULL
),
WithArrays as(
  SELECT
      DivergenceFromAstro.*,
      ARRAY_AGG(COALESCE(IssuingCardLog.logCreated,DATE("1900-01-01"))) OVER(PARTITION BY issuingCardId ORDER BY IssuingCardLog.logCreated ASC) AS eventDates,
      ARRAY_AGG(COALESCE(IssuingCardLog.type,"NULL")) OVER(PARTITION BY issuingCardId ORDER BY IssuingCardLog.logCreated ASC) AS events,
      row_number() over(partition by issuingCardId order by IssuingCardLog.logCreated DESC) as rowNumber,
  FROM
      DivergenceFromAstro
      LEFT JOIN Card.IssuingCardLog USING(issuingCardId)
)
SELECT
  *EXCEPT(rowNumber)
FROM
  WithArrays
WHERE
  rowNumber = 1
)