CREATE TEMP TABLE Tbl_SubAdquirencia_Rede_Ajustes
  AS( SELECT * FROM Finance.Tbl_SubAdquirencia_Rede_Ajustes);
DROP TABLE IF EXISTS Finance.Tbl_SubAdquirencia_Rede_Ajustes;
CREATE OR REPLACE TABLE Finance.Tbl_SubAdquirencia_Rede_Ajustes
  PARTITION BY data_do_ajuste
  AS( SELECT * FROM Tbl_SubAdquirencia_Rede_Ajustes);

CREATE TEMP TABLE Tbl_SubAdquirencia_Rede_CancelamentosEContestacoes
  AS( SELECT * FROM Finance.Tbl_SubAdquirencia_Rede_CancelamentosEContestacoes);
DROP TABLE IF EXISTS Finance.Tbl_SubAdquirencia_Rede_CancelamentosEContestacoes;
CREATE OR REPLACE TABLE Finance.Tbl_SubAdquirencia_Rede_CancelamentosEContestacoes
  PARTITION BY data_original_da_venda
  AS( SELECT * FROM Tbl_SubAdquirencia_Rede_CancelamentosEContestacoes);

CREATE TEMP TABLE Tbl_SubAdquirencia_Rede_CobrancasEmAberto
  AS( SELECT * FROM Finance.Tbl_SubAdquirencia_Rede_CobrancasEmAberto);
DROP TABLE IF EXISTS Finance.Tbl_SubAdquirencia_Rede_CobrancasEmAberto;
CREATE OR REPLACE TABLE Finance.Tbl_SubAdquirencia_Rede_CobrancasEmAberto
  PARTITION BY data_de_criacao_do_debito
  AS( SELECT * FROM Tbl_SubAdquirencia_Rede_CobrancasEmAberto);

CREATE TEMP TABLE Tbl_SubAdquirencia_Rede_Pagamentos
  AS( SELECT * FROM Finance.Tbl_SubAdquirencia_Rede_Pagamentos);
DROP TABLE IF EXISTS Finance.Tbl_SubAdquirencia_Rede_Pagamentos;
CREATE OR REPLACE TABLE Finance.Tbl_SubAdquirencia_Rede_Pagamentos
  PARTITION BY data_original_da_venda
  AS( SELECT * FROM Tbl_SubAdquirencia_Rede_Pagamentos);

CREATE TEMP TABLE Tbl_SubAdquirencia_Rede_PagamentosFuturos
  AS( SELECT * FROM Finance.Tbl_SubAdquirencia_Rede_PagamentosFuturos);
DROP TABLE IF EXISTS Finance.Tbl_SubAdquirencia_Rede_PagamentosFuturos;
CREATE OR REPLACE TABLE Finance.Tbl_SubAdquirencia_Rede_PagamentosFuturos
  PARTITION BY data_original_da_venda
  AS( SELECT * FROM Tbl_SubAdquirencia_Rede_PagamentosFuturos);

CREATE TEMP TABLE Tbl_SubAdquirencia_Rede_Recebidos
  AS( SELECT * FROM Finance.Tbl_SubAdquirencia_Rede_Recebidos);
DROP TABLE IF EXISTS Finance.Tbl_SubAdquirencia_Rede_Recebidos;
CREATE OR REPLACE TABLE Finance.Tbl_SubAdquirencia_Rede_Recebidos
  PARTITION BY data
  AS( SELECT * FROM Tbl_SubAdquirencia_Rede_Recebidos);
-- Esse não tem outra opção de data