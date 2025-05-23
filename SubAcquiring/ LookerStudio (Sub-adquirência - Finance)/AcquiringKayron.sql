WITH 
merchant AS( 
    SELECT 
        MP.endToEndId,
        MI.purchaseId AS merchantId,
        DATE_TRUNC(DATE(MI.due, "America/Sao_Paulo"), DAY) AS paymentDay, -- pagamento para o cliente    
        MI.status AS intallmentStatus,
        ROW_NUMBER() OVER (PARTITION BY MP.endToEndId ORDER BY DATE_TRUNC(DATE(MI.due, "America/Sao_Paulo"), DAY)) AS installmentNumber ,
        MI.amount AS intallmentAmount, -- em centavos   
        MI.fee AS installmentMDR -- em centavos           
    FROM 
        Acquiring.MerchantPurchase MP
        LEFT JOIN Acquiring.MerchantInstallment MI ON MI.purchaseId = CAST(MP.id AS STRING)
), 
acquirer AS(
  SELECT
    AP.endToEndId,
    AP.merchantTaxId,
    DATE_TRUNC(DATE(AI.created, "America/Sao_Paulo"), DAY) AS createdDay,
    AP.amount AS total_amount,
    AP.fundingType,
    AP.network,
    AP.status,
    AP.installmentCount,
    DATE_TRUNC(DATE(AI.due, "America/Sao_Paulo"),DAY) AS redePaymentDay,
    ROW_NUMBER() OVER (PARTITION BY AP.endToEndId ORDER BY DATE_TRUNC(DATE(AI.due, "America/Sao_Paulo"),DAY)) AS installmentNumber,
    AI.externalFee AS installmentCost, -- em centavos
  FROM 
    Acquiring.AcquiringInstallment AI 
    LEFT JOIN Acquiring.AcquiringPurchase AP ON AI.purchaseId = CAST(AP.id AS STRING)
  WHERE 
    AP.status IN (
      'confirmed',
      'voided'
    )
    -- REMOVENDO AS EMPRESAS DO GRUPO STARK
    AND AP.merchantTaxId NOT IN(
      '39.908.427/0001-28',
      '20.018.183/0001-80',
      '38.232.533/0001-71',
      '49.411.582/0001-98',
      '43.853.248/0001-08',
      '55.780.627/0001-02'
    ) 
),
merchant_acquirer AS(
  SELECT
    acquirer.endToEndId,
    createdDay,
    total_amount,
    merchantTaxId,
    w.companyName AS merchantName,
    fundingType,
    network,
    status,
    installmentCount,
    merchantId,
    paymentDay, -- pagamento para o cliente
    intallmentStatus,
    merchant.installmentNumber,
    intallmentAmount,
    installmentMDR,
    installmentCost
  FROM acquirer
    LEFT JOIN (
        SELECT DISTINCT
        companyTaxId,
        companyName
        FROM Accounting.WorkspaceExport
    ) AS w ON acquirer.merchantTaxId = w.companyTaxId
    LEFT JOIN merchant ON (acquirer.endToEndId = merchant.endToEndId AND acquirer.installmentNumber = merchant.installmentNumber)
  WHERE 
    -- REMOVENDO AS OPERAÇÕES DE ZERO DOLLAR
    (merchantId IS NOT NULL AND total_amount <> 0)
    -- REMOVENDO AS OPERAÇÕES DO DIA CORRENTE
    AND DATE(createdDay) < CURRENT_DATE() 
  ORDER BY createdDay, merchantId, paymentDay
)
SELECT 
    --acquirer.endToEndId,
    createdDay,
    --total_amount,
    merchantTaxId,
    merchantName,
    fundingType,
    network,
    status,
    installmentCount,
    --merchantId,
    paymentDay, -- pagamento para o cliente
    intallmentStatus,
    --installmentNumber,
    SUM(intallmentAmount) AS intallmentAmount,
    SUM(installmentMDR) AS installmentMDR,
    SUM(installmentCost) AS installmentCost
FROM merchant_acquirer
GROUP BY ALL
