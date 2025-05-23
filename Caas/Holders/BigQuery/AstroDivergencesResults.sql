WITH
Raw AS (
  SELECT 
    issuingCardId,
    origem,
    accum,
    cardType,
    status,
    eventDate,
    event,
  FROM 
    Temp.AstroDivergencesFromUsDetailed
    JOIN UNNEST(eventDates) AS eventDate
    JOIN UNNEST(events) AS event
),
PreTreated AS (
  SELECT
    issuingCardId,
    origem,
    accum,
    cardType,
    status,
    eventDate,
    event,
    CASE
      WHEN cardType = "physical" AND event = "created" THEN 0
      WHEN cardType = "virtual" AND event = "unblocked" THEN 0
      ELSE 1
    END AS filterEvent,
    ROW_NUMBER() OVER(PARTITION BY issuingCardId, event ORDER BY eventDate ASC) AS rowNumber,
  FROM
    Raw
  WHERE
    event IN("created", "unblocked", "canceled")
),
TreatedCanceled AS (
  SELECT
    issuingCardId,
    origem,
    accum,
    cardType,
    status,
    eventDate,
    event,
  FROM
    PreTreated
  WHERE
    filterEvent = 1
    AND rowNumber = 1
    AND event = "canceled"
),
TreatedCreated AS (
  SELECT
    issuingCardId,
    origem,
    accum,
    cardType,
    status,
    eventDate,
    event,
  FROM
    PreTreated
  WHERE
    filterEvent = 1
    AND rowNumber = 1
    AND event IN("created", "unblocked")
)
SELECT
  *
FROM
  TreatedCanceled