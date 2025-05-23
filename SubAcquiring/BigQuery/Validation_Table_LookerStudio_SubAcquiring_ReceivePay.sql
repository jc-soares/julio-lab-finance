-- Essa query calcula o que entra e o que sai de cada dia, considerando as datas de pagamento do adquirente e do comerciante.
-- Ela já está preparada para analisar os números relacionados às compras, na data de criação delas. Essa parte está comentada

DECLARE DS_START_DATE STRING;
DECLARE DS_END_DATE STRING;

SET DS_START_DATE = FORMAT_DATE("%Y%m%d",DATE_SUB(CURRENT_DATE(), INTERVAL 14 MONTH));
SET DS_END_DATE = FORMAT_DATE("%Y%m%d",CURRENT_DATE());

WITH
PARAMS AS (
    SELECT
        PARSE_DATE("%Y%m%d", DS_START_DATE) AS START_DATE,
        PARSE_DATE("%Y%m%d", DS_END_DATE) AS END_DATE,
        -- PARSE_DATE("%Y%m%d", @DS_START_DATE) AS START_DATE,
        -- PARSE_DATE("%Y%m%d", @DS_END_DATE) AS END_DATE,
),
NominalDue AS (
    SELECT
        DATE(installment.due, "America/Sao_Paulo") AS nominalDue,
        DATE(installment.created, "America/Sao_Paulo") AS installmentCreated,
        installmentId
    FROM
        Acquiring.MerchantInstallmentLog
    WHERE
        type = "created"
),
Holidays AS (
    SELECT
        holidayName,
        holidayDate,
    FROM
        Finance.HolidayBR
),
HolidayDateRange AS (
    SELECT
        rawDate,
        CASE
            WHEN EXTRACT(DAYOFWEEK FROM rawDate) IN (1, 7) OR holidayDate IS NOT NULL THEN 1
            ELSE 0
        END AS isWeekend,
    FROM
        UNNEST(GENERATE_DATE_ARRAY(
            (SELECT MIN(holidayDate) FROM Holidays),
            (SELECT MAX(holidayDate) FROM Holidays),
            INTERVAL 1 DAY
        )) AS rawDate
        LEFT JOIN Holidays ON rawDate = holidayDate
),
NextWeekday AS (
    SELECT
        a.rawDate,
        MIN(b.rawDate) AS nextWeekDay
    FROM
        HolidayDateRange a
        JOIN HolidayDateRange b ON b.rawDate >= a.rawDate AND b.isWeekend = 0
    GROUP BY a.rawDate
),
DateFromAcquirerRaw AS (
    SELECT
        CASE
            WHEN MerchantPurchase.fundingType = "credit" THEN DATE_ADD(NominalDue.installmentCreated, INTERVAL 31 DAY)
            WHEN MerchantPurchase.fundingType = "debit" THEN DATE_ADD(NominalDue.installmentCreated, INTERVAL 2 DAY)
            WHEN MerchantPurchase.fundingType = "prepaid" THEN DATE_ADD(NominalDue.installmentCreated, INTERVAL 2 DAY)
            ELSE DATE_ADD(NominalDue.installmentCreated, INTERVAL 31 DAY)
        END AS dueFromAcquirer,
        NominalDue.nominalDue,
        installmentCreated,
        CAST(MerchantInstallment.__key__.id AS STRING) AS installmentId,
    FROM
        Acquiring.MerchantInstallment
        LEFT JOIN Acquiring.MerchantPurchase ON MerchantInstallment.purchaseId = CAST(MerchantPurchase.__key__.id AS STRING)
        LEFT JOIN NominalDue ON CAST(MerchantInstallment.__key__.id AS STRING) = NominalDue.installmentId
),
DateFromAcquirer AS (
    SELECT
        nominalDue,
        installmentCreated,
        NextWeekday.nextWeekDay AS dueFromAcquirer,
        installmentId,
    FROM
        DateFromAcquirerRaw
        LEFT JOIN NextWeekday ON DateFromAcquirerRaw.dueFromAcquirer = NextWeekday.rawDate 
),
Workspace AS (
    SELECT
        workspaceId,
        companyTaxId,
        companyName,
        ROW_NUMBER() OVER(PARTITION BY workspaceId ORDER BY workspaceCreated DESC) AS rowNumber,
    FROM
        General.Workspace
),
Factoring AS (
    SELECT
        CAST(__key__.id AS STRING) AS installmentId,
        TRUE AS isFactoring,
    FROM
        Acquiring.MerchantInstallment
    WHERE
        ARRAY_TO_STRING(tags, ",") like "%contract-chunk%"
),
PurchasesCompleteCreated AS (
  SELECT
      DATE(MerchantInstallment.created, "America/Sao_Paulo") AS referenceDate,
      MerchantInstallment.network,
      MerchantInstallment.workspaceId,
      MerchantInstallment.status AS installmentStatus,
      IFNULL(Factoring.isFactoring, FALSE) AS isFactoring,
     --  SUM(IF(MerchantPurchase.status = "voided", 0, AcquiringInstallment.amount)) AS acquiringAmount,
      SUM(IF(MerchantPurchase.status = "voided", 0, MerchantInstallment.amount)) AS amountCreated,
      SUM(IF(MerchantPurchase.status = "voided", 0, MerchantInstallment.fee)) AS feeCreated,
      SUM(NULL) AS factoringFeeCreated,
      SUM(NULL) AS externalFactoringFeeCreated,
      -- OBS: não temos registrado o cobrado pelo adquirente sobre nós para fazer a antecipação
    -- TODO: arrumar com if = voided
      SUM(MerchantInstallment.fee - AcquiringInstallment.externalFee) AS netResultWithoutFactoringCreated,
      SUM(AcquiringInstallment.externalFee) AS externalFeeCreated,
      SUM(IF(MerchantInstallment.fee IS NULL AND AcquiringInstallment.externalFee IS NULL, MerchantInstallment.amount, NULL)) AS amountInstallmentsWithoutFeeAndExternalFeeCreated,
      SUM(IF(MerchantInstallment.fee IS NULL AND AcquiringInstallment.externalFee IS NOT NULL, MerchantInstallment.amount, NULL)) AS amountInstallmentsWithoutFeeOnlyCreated,
      SUM(IF(MerchantInstallment.fee IS NOT NULL AND AcquiringInstallment.externalFee IS NULL, MerchantInstallment.amount, NULL)) AS amountInstallmentsWithoutExternalFeeOnlyCreated,
      AVG(DATE_DIFF(DateFromAcquirer.nominalDue, dueFromAcquirer, DAY)) AS daysBetweenPaymentsAvgCreated,
  FROM
      Acquiring.MerchantInstallment
      LEFT JOIN Acquiring.MerchantPurchase ON MerchantInstallment.purchaseId = CAST(MerchantPurchase.__key__.id AS STRING)
      LEFT JOIN Acquiring.AcquiringPurchase USING(endToEndId)
      LEFT JOIN DateFromAcquirer ON CAST(MerchantInstallment.__key__.id AS STRING) = DateFromAcquirer.installmentId
      LEFT JOIN Acquiring.AcquiringInstallment ON CAST(AcquiringPurchase.id AS STRING) = AcquiringInstallment.purchaseId AND DateFromAcquirer.nominalDue = DATE(AcquiringInstallment.due, "America/Sao_Paulo")
      LEFT JOIN Factoring ON CAST(MerchantInstallment.__key__.id AS STRING) = Factoring.installmentId
      LEFT JOIN Workspace ON MerchantInstallment.workspaceId = Workspace.workspaceId AND rowNumber = 1
  WHERE
      DATE(MerchantInstallment.created, "America/Sao_Paulo") BETWEEN (SELECT START_DATE FROM PARAMS) AND (SELECT END_DATE FROM PARAMS)
      AND (AcquiringPurchase.amount > 0 OR AcquiringPurchase.status = 'voided')
      AND AcquiringPurchase.status IN ('confirmed', 'voided')
      AND Workspace.companyTaxId NOT IN ('39.908.427/0001-28', '20.018.183/0001-80', '38.232.533/0001-71', '49.411.582/0001-98', '43.853.248/0001-08', '55.780.627/0001-02')
    GROUP BY ALL
),
PurchasesCompleteDueMerchant AS (
  SELECT
      DATE(MerchantInstallment.due, "America/Sao_Paulo") AS referenceDate,
      MerchantInstallment.network,
      MerchantInstallment.workspaceId,
      MerchantInstallment.status AS installmentStatus,
      IFNULL(Factoring.isFactoring, FALSE) AS isFactoring,
      -- Falta factoringFee (checar também como tratar compras antecipadas canceladas)
      SUM(IF(MerchantPurchase.status = "voided", 0, MerchantInstallment.amount) - IF(MerchantPurchase.status = "voided", 0, MerchantInstallment.fee) - COALESCE(NULL, 0) ) AS amountToMerchant,
      SUM(IF(MerchantInstallment.fee IS NULL AND AcquiringInstallment.externalFee IS NULL, MerchantInstallment.amount, NULL)) AS amountInstallmentsWithoutFeeAndExternalFeeToMerchant,
      SUM(IF(MerchantInstallment.fee IS NULL AND AcquiringInstallment.externalFee IS NOT NULL, MerchantInstallment.amount, NULL)) AS amountInstallmentsWithoutFeeOnlyToMerchant,
      SUM(IF(MerchantInstallment.fee IS NOT NULL AND AcquiringInstallment.externalFee IS NULL, MerchantInstallment.amount, NULL)) AS amountInstallmentsWithoutExternalFeeOnlyToMerchant,
  FROM
      Acquiring.MerchantInstallment
      LEFT JOIN Acquiring.MerchantPurchase ON MerchantInstallment.purchaseId = CAST(MerchantPurchase.__key__.id AS STRING)
      LEFT JOIN Acquiring.AcquiringPurchase USING(endToEndId)
      LEFT JOIN DateFromAcquirer ON CAST(MerchantInstallment.__key__.id AS STRING) = DateFromAcquirer.installmentId
      LEFT JOIN Acquiring.AcquiringInstallment ON CAST(AcquiringPurchase.id AS STRING) = AcquiringInstallment.purchaseId AND DateFromAcquirer.nominalDue = DATE(AcquiringInstallment.due, "America/Sao_Paulo")
      LEFT JOIN Factoring ON CAST(MerchantInstallment.__key__.id AS STRING) = Factoring.installmentId
      LEFT JOIN Workspace ON MerchantInstallment.workspaceId = Workspace.workspaceId AND rowNumber = 1
  WHERE
      DATE(MerchantInstallment.created, "America/Sao_Paulo") BETWEEN (SELECT START_DATE FROM PARAMS) AND (SELECT END_DATE FROM PARAMS)
      AND (AcquiringPurchase.amount > 0 OR AcquiringPurchase.status = 'voided')
      AND AcquiringPurchase.status IN ('confirmed', 'voided')
      AND Workspace.companyTaxId NOT IN ('39.908.427/0001-28', '20.018.183/0001-80', '38.232.533/0001-71', '49.411.582/0001-98', '43.853.248/0001-08', '55.780.627/0001-02')
    GROUP BY ALL
),
PurchasesCompleteDueAcquirer AS (
  SELECT
      IF(IFNULL(Factoring.isFactoring, FALSE) = TRUE, DATE(MerchantInstallment.due, "America/Sao_Paulo"), DateFromAcquirer.dueFromAcquirer) AS referenceDate,
      MerchantInstallment.network,
      MerchantInstallment.workspaceId,
      MerchantInstallment.status AS installmentStatus,
      IFNULL(Factoring.isFactoring, FALSE) AS isFactoring,
      -- também seria bom se tivesse um factoringExternalFee
      SUM(IF(MerchantPurchase.status = "voided", 0, MerchantInstallment.amount) - IF(MerchantPurchase.status = "voided", 0, AcquiringInstallment.externalFee)) AS amountFromAcquirer,
      -- aqui serve para calcular o que deve ser externalFee para os null
      SUM(IF(MerchantInstallment.fee IS NULL AND AcquiringInstallment.externalFee IS NULL, MerchantInstallment.amount, NULL)) AS amountInstallmentsWithoutFeeAndExternalFeeFromAcquirer,
      SUM(IF(MerchantInstallment.fee IS NULL AND AcquiringInstallment.externalFee IS NOT NULL, MerchantInstallment.amount, NULL)) AS amountInstallmentsWithoutFeeOnlyFromAcquirer,
      SUM(IF(MerchantInstallment.fee IS NOT NULL AND AcquiringInstallment.externalFee IS NULL, MerchantInstallment.amount, NULL)) AS amountInstallmentsWithoutExternalFeeOnlyFromAcquirer,
  FROM
      Acquiring.MerchantInstallment
      LEFT JOIN Acquiring.MerchantPurchase ON MerchantInstallment.purchaseId = CAST(MerchantPurchase.__key__.id AS STRING)
      LEFT JOIN Acquiring.AcquiringPurchase USING(endToEndId)
      LEFT JOIN DateFromAcquirer ON CAST(MerchantInstallment.__key__.id AS STRING) = DateFromAcquirer.installmentId
      LEFT JOIN Acquiring.AcquiringInstallment ON CAST(AcquiringPurchase.id AS STRING) = AcquiringInstallment.purchaseId AND DateFromAcquirer.nominalDue = DATE(AcquiringInstallment.due, "America/Sao_Paulo")
      LEFT JOIN Factoring ON CAST(MerchantInstallment.__key__.id AS STRING) = Factoring.installmentId
      LEFT JOIN Workspace ON MerchantInstallment.workspaceId = Workspace.workspaceId AND rowNumber = 1
  WHERE
      DATE(MerchantInstallment.created, "America/Sao_Paulo") BETWEEN (SELECT START_DATE FROM PARAMS) AND (SELECT END_DATE FROM PARAMS)
      AND (AcquiringPurchase.amount > 0 OR AcquiringPurchase.status = 'voided')
      AND AcquiringPurchase.status IN ('confirmed', 'voided')
      AND Workspace.companyTaxId NOT IN ('39.908.427/0001-28', '20.018.183/0001-80', '38.232.533/0001-71', '49.411.582/0001-98', '43.853.248/0001-08', '55.780.627/0001-02')
    GROUP BY ALL
),
DateRange AS (
  SELECT
  * 
  FROM
    UNNEST(GENERATE_DATE_ARRAY((SELECT MIN(referenceDate) FROM PurchasesCompleteCreated), (SELECT MAX(referenceDate) FROM PurchasesCompleteDueMerchant), INTERVAL 1 DAY)) referenceDate
),
DailySummary AS (
  SELECT
    referenceDate,
    Workspace.companyName,
    MerchantCreated.isFactoring,
    COALESCE(MerchantCreated.network, AcquirerDue.network, MerchantDue.network) AS network,
    COALESCE(MerchantCreated.installmentStatus, AcquirerDue.installmentStatus, MerchantDue.installmentStatus) AS installmentStatus,

    amountFromAcquirer,
    amountInstallmentsWithoutFeeAndExternalFeeFromAcquirer,
    amountInstallmentsWithoutFeeOnlyFromAcquirer,
    amountInstallmentsWithoutExternalFeeOnlyFromAcquirer,

    amountToMerchant,
    amountInstallmentsWithoutFeeAndExternalFeeToMerchant,
    amountInstallmentsWithoutFeeOnlyToMerchant,
    amountInstallmentsWithoutExternalFeeOnlyToMerchant,

    -- amountCreated,
    -- feeCreated,
    -- externalFeeCreated,
    -- factoringFeeCreated,
    -- externalFactoringFeeCreated,
    -- amountInstallmentsWithoutFeeAndExternalFeeCreated,
    -- amountInstallmentsWithoutFeeOnlyCreated,
    -- amountInstallmentsWithoutExternalFeeOnlyCreated,
    -- netResultWithoutFactoringCreated,
    -- daysBetweenPaymentsAvgCreated,
  FROM
    DateRange
    LEFT JOIN PurchasesCompleteCreated MerchantCreated USING(referenceDate)
    FULL JOIN PurchasesCompleteDueAcquirer AcquirerDue USING(referenceDate, network, installmentStatus, workspaceId)
    FULL JOIN PurchasesCompleteDueMerchant MerchantDue USING(referenceDate, network, installmentStatus, workspaceId)
    LEFT JOIN Workspace ON COALESCE(MerchantCreated.workspaceId, MerchantDue.workspaceId, AcquirerDue.workspaceId) = Workspace.workspaceId AND rowNumber = 1
)
SELECT
  *
FROM
  DailySummary
