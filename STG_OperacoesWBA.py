#!/usr/bin/env python
# coding: utf-8

# # WBA

# In[ ]:


from datetime import datetime 
datetime.today().strftime('Data: %Y-%m-%d Hora: %H:%m')


# In[ ]:


import pandas as pd
import mysql.connector
import pyodbc
import urllib

# Configurações para o banco de dados SQL Server (fonte)
source_server = '172.31.5.118'
source_database = 'AGIS'
source_user = 'READ'
source_password = 'JyRHwp7U2gU7qb5aCyhE'

# Configurações para o banco de dados MySQL (destino)
dest_server = 'catalise-bi-dados-cluster.cluster-ciaao5zm9beh.sa-east-1.rds.amazonaws.com'
dest_database = 'DW_STAGING'
dest_user = 'admin'
dest_password = 'yb222KraY7PTN0jbH7P3'

# Conectar ao banco de dados MySQL e truncar a tabela
connection = mysql.connector.connect(
    host=dest_server,
    user=dest_user,
    password=dest_password,
    database=dest_database
)

cursor = connection.cursor()
# cursor.execute(f"TRUNCATE TABLE Stg_OperacoesWBA")
# connection.commit()

# Configurações para a conexão com o SQL Server
source_connection_str = (
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={source_server};'
    f'DATABASE={source_database};'
    f'UID={source_user};'
    f'PWD={source_password}'
)
source_engine = pyodbc.connect(source_connection_str)

# Executar a consulta SQL e salvar em um DataFrame
sql_query = """
-- SELECT
--     'UNITY' Empresa,
--     cgc_cliente as DocCedente,
-- 	cgc_sacado as DocSacado,
--     numero_titulo ,
--     LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--     -- numero_bordero,
--     -- valor_titulo,
--     -- valor_desconto,
--     valor_aberto,
--     data_original data_vencimento,
--     data_emissao
--     -- sequencia_bordero
-- FROM [UNITY].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
-- LEFT JOIN  [UNITY]..SIGFIDC SG
--     ON AB.nosso_numero = SG.ctrl_id
-- UNION ALL
-- SELECT
--     'BLUE ROCKET' Empresa,
--     cgc_cliente as DocCedente,
-- 	cgc_sacado as DocSacado,
--     numero_titulo ,
--     LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--     -- numero_bordero,
--     -- valor_titulo,
--     -- valor_desconto,
--     valor_aberto,
--     data_original data_vencimento,
--     data_emissao
--     -- sequencia_bordero
-- FROM [BLUEROCKET].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
-- LEFT JOIN  [BLUEROCKET]..SIGFIDC SG
--     ON AB.nosso_numero = SG.ctrl_id
-- UNION ALL
-- SELECT
--     'CREDIAL' Empresa,
--     cgc_cliente as DocCedente,
-- 	cgc_sacado as DocSacado,
--     numero_titulo ,
--     LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--     -- numero_bordero,
--     -- valor_titulo,
--     -- valor_desconto,
--     valor_aberto,
--     data_original data_vencimento,
--     data_emissao
--     -- sequencia_bordero
-- FROM [CREDIAL].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
-- LEFT JOIN  [CREDIAL]..SIGFIDC SG
--     ON AB.nosso_numero = SG.ctrl_id
-- UNION ALL
SELECT
    'F2 BANK' Empresa,
    cgc_cliente as DocCedente,
	cgc_sacado as DocSacado,
    numero_titulo ,
    LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
    -- numero_bordero,
    -- valor_titulo,
    -- valor_desconto,
    valor_aberto,
    data_original data_vencimento,
    data_emissao
    -- sequencia_bordero
FROM [F2BANK].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
LEFT JOIN  [F2BANK]..SIGFIDC SG
    ON AB.nosso_numero = SG.ctrl_id
UNION ALL
SELECT
    'KERDOS' Empresa,
    cgc_cliente as DocCedente,
	cgc_sacado as DocSacado,
    numero_titulo ,
    LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
    -- numero_bordero,
    -- valor_titulo,
    -- valor_desconto,
    valor_aberto,
    data_original data_vencimento,
    data_emissao
    -- sequencia_bordero
FROM [KerdosFIDC].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
LEFT JOIN  [KerdosFIDC]..SIGFIDC SG
    ON AB.nosso_numero = SG.ctrl_id
UNION ALL
--SELECT
--    'LOGFIN' Empresa,
--    cgc_cliente as DocCedente,
--	cgc_sacado as DocSacado,
--    numero_titulo ,
--    LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--    -- numero_bordero,
--    -- valor_titulo,
--    -- valor_desconto,
--    valor_aberto,
--    data_original data_vencimento,
--    data_emissao
--    -- sequencia_bordero
--FROM [LOGFIN].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
--LEFT JOIN  [LOGFIN]..SIGFIDC SG
--    ON AB.nosso_numero = SG.ctrl_id
--UNION ALL
SELECT
    '3G BANK' Empresa,
    cgc_cliente as DocCedente,
	cgc_sacado as DocSacado,
    numero_titulo ,
    LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
    -- numero_bordero,
    -- valor_titulo,
    -- valor_desconto,
    valor_aberto,
    data_original data_vencimento,
    data_emissao
    -- sequencia_bordero
FROM [TRESGBANK].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
LEFT JOIN  [TRESGBANK]..SIGFIDC SG
    ON AB.nosso_numero = SG.ctrl_id
UNION ALL
SELECT
    'VELSO' Empresa,
    cgc_cliente as DocCedente,
	cgc_sacado as DocSacado,
    numero_titulo ,
    LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
    -- numero_bordero,
    -- valor_titulo,
    -- valor_desconto,
    valor_aberto,
    data_original data_vencimento,
    data_emissao
    -- sequencia_bordero
FROM [VELSOFIDC].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
LEFT JOIN  [VELSOFIDC]..SIGFIDC SG
    ON AB.nosso_numero = SG.ctrl_id
UNION ALL
--SELECT
--    'VISHNU' Empresa,
--    cgc_cliente as DocCedente,
--	cgc_sacado as DocSacado,
--    numero_titulo ,
--    LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--    -- numero_bordero,
--    -- valor_titulo,
--    -- valor_desconto,
--    valor_aberto,
--    data_original data_vencimento,
--    data_emissao
--    -- sequencia_bordero
--FROM [VISHNU].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
--LEFT JOIN  [VISHNU]..SIGFIDC SG
--    ON AB.nosso_numero = SG.ctrl_id
--UNION ALL
SELECT
    'TOPCRED' Empresa,
    cgc_cliente as DocCedente,
	cgc_sacado as DocSacado,
    numero_titulo ,
    LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
    -- numero_bordero,
    -- valor_titulo,
    -- valor_desconto,
    valor_aberto,
    data_original data_vencimento,
    data_emissao
    -- sequencia_bordero
FROM [TOPCREDFIDC].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
LEFT JOIN  [TOPCREDFIDC]..SIGFIDC SG
    ON AB.nosso_numero = SG.ctrl_id
UNION ALL
SELECT
    'MF GROUP' Empresa,
    cgc_cliente as DocCedente,
	cgc_sacado as DocSacado,
    numero_titulo ,
    LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
    -- numero_bordero,
    -- valor_titulo,
    -- valor_desconto,
    valor_aberto,
    data_original data_vencimento,
    data_emissao
    -- sequencia_bordero
FROM [MFG].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
LEFT JOIN  [MFG]..SIGFIDC SG
    ON AB.nosso_numero = SG.ctrl_id
UNION ALL
SELECT
    'PAYCARGO' Empresa,
    cgc_cliente as DocCedente,
	cgc_sacado as DocSacado,
    numero_titulo ,
    LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
    -- numero_bordero,
    -- valor_titulo,
    -- valor_desconto,
    valor_aberto,
    data_original data_vencimento,
    data_emissao
    -- sequencia_bordero
FROM [PAYCARGO].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
LEFT JOIN  [PAYCARGO]..SIGFIDC SG
    ON AB.nosso_numero = sg.ctrl_id
UNION ALL
SELECT
    'SDL' Empresa,
    cgc_cliente as DocCedente,
	cgc_sacado as DocSacado,
    numero_titulo ,
    LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
    -- numero_bordero,
    -- valor_titulo,
    -- valor_desconto,
    valor_aberto,
    data_original data_vencimento,
    data_emissao
    -- sequencia_bordero
FROM [SDLFIDC].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
LEFT JOIN  [SDLFIDC]..SIGFIDC SG
    ON AB.nosso_numero = SG.ctrl_id
UNION ALL
SELECT
    '3RD' Empresa,
    cgc_cliente as DocCedente,
	cgc_sacado as DocSacado,
    numero_titulo ,
    LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
    -- numero_bordero,
    -- valor_titulo,
    -- valor_desconto,
    valor_aberto,
    data_original data_vencimento,
    data_emissao
    -- sequencia_bordero
FROM [TRESRDFIDC].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
LEFT JOIN  [TRESRDFIDC]..SIGFIDC SG
    ON AB.nosso_numero = SG.ctrl_id
UNION ALL
-- SELECT
--     'NINE' Empresa,
--     cgc_cliente as DocCedente,
-- 	cgc_sacado as DocSacado,
--     numero_titulo ,
--     LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--     -- numero_bordero,
--     -- valor_titulo,
--     -- valor_desconto,
--     valor_aberto,
--     data_original data_vencimento,
--     data_emissao
--     -- sequencia_bordero
-- FROM [NINE].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
-- LEFT JOIN  [NINE]..SIGFIDC SG
--     ON AB.nosso_numero = SG.ctrl_id
-- UNION ALL
-- SELECT
--     'BRAVOS' Empresa,
--     cgc_cliente as DocCedente,
-- 	cgc_sacado as DocSacado,
--     numero_titulo ,
--     LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--     -- numero_bordero,
--     -- valor_titulo,
--     -- valor_desconto,
--     valor_aberto,
--     data_original data_vencimento,
--     data_emissao
--     -- sequencia_bordero
-- FROM [BRAVOS].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
-- LEFT JOIN  [BRAVOS]..SIGFIDC SG
--     ON AB.nosso_numero = SG.ctrl_id
-- UNION ALL
-- SELECT
--     'AF6' Empresa,
--     cgc_cliente as DocCedente,
-- 	cgc_sacado as DocSacado,
--     numero_titulo ,
--     LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--     -- numero_bordero,
--     -- valor_titulo,
--     -- valor_desconto,
--     valor_aberto,
--     data_original data_vencimento,
--     data_emissao
--     -- sequencia_bordero
-- FROM [AF6].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
-- LEFT JOIN  [AF6]..SIGFIDC SG
--     ON AB.nosso_numero = SG.ctrl_id
-- UNION ALL
-- SELECT
--     'MASTRENN' Empresa,
--     cgc_cliente as DocCedente,
-- 	cgc_sacado as DocSacado,
--     numero_titulo ,
--     LTRIM(RTRIM(SG.ctrl_id)) + '000000001' Operacao,
--     -- numero_bordero,
--     -- valor_titulo,
--     -- valor_desconto,
--     valor_aberto,
--     data_original data_vencimento,
--     data_emissao
--     -- sequencia_bordero
-- FROM [MASTRENN].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
-- LEFT JOIN  [MASTRENN]..SIGFIDC SG
--     ON AB.nosso_numero = SG.ctrl_id
-- UNION ALL
SELECT
    'MALBEC' Empresa,
    cgc_cliente as DocCedente,
	cgc_sacado as DocSacado,
    numero_titulo,
    LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
    -- numero_bordero,
    -- valor_titulo,
    -- valor_desconto,
    valor_aberto,
    data_original data_vencimento,
    data_emissao
    -- sequencia_bordero
FROM [MALBEC].DBO.[Vw_obtemtitulosemabertofidc] (NOLOCK) AB
LEFT JOIN [MALBEC]..SIGFIDC SG
    ON AB.nosso_numero = sg.ctrl_id
UNION ALL
--SELECT
--    'BELL' Empresa,
--    cgc_cliente as DocCedente,
--	cgc_sacado as DocSacado,
--    numero_titulo ,
--    LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--    -- numero_bordero,
--    -- valor_titulo,
--    -- valor_desconto,
--    valor_aberto,
--    data_original data_vencimento,
--    data_emissao
--    -- sequencia_bordero
--FROM [BELL].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
--LEFT JOIN  [BELL]..SIGFIDC SG
--    ON AB.nosso_numero = SG.ctrl_id
--UNION ALL
--SELECT
--    'VERGINIA' Empresa,
--    cgc_cliente as DocCedente,
--	cgc_sacado as DocSacado,
--    numero_titulo ,
--    LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--    -- numero_bordero,
--    -- valor_titulo,
--    -- valor_desconto,
--    valor_aberto,
--    data_original data_vencimento,
--    data_emissao
--    -- sequencia_bordero
--FROM [VERGINIA].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
--LEFT JOIN  [VERGINIA]..SIGFIDC SG
--    ON AB.nosso_numero = SG.ctrl_id
--UNION ALL
-- SELECT
--     'VALENTE' Empresa,
--     cgc_cliente as DocCedente,
-- 	cgc_sacado as DocSacado,
--     numero_titulo ,
--     LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--     -- numero_bordero,
--     -- valor_titulo,
--     -- valor_desconto,
--     valor_aberto,
--     data_original data_vencimento,
--     data_emissao
--     -- sequencia_bordero
-- FROM [VALENTE].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
-- LEFT JOIN  [VALENTE]..SIGFIDC SG
--     ON AB.nosso_numero = SG.ctrl_id
-- UNION ALL
-- SELECT
--     'K-FINANCE' Empresa,
--     cgc_cliente as DocCedente,
-- 	cgc_sacado as DocSacado,
--     numero_titulo ,
--     LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--     -- numero_bordero,
--     -- valor_titulo,
--     -- valor_desconto,
--     valor_aberto,
--     data_original data_vencimento,
--     data_emissao
--     -- sequencia_bordero
-- FROM [Kfinance].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
-- LEFT JOIN  [Kfinance]..SIGFIDC SG
--     ON AB.nosso_numero = SG.ctrl_id
-- UNION ALL
-- SELECT
--     'ALBAREDO' Empresa,
--     cgc_cliente as DocCedente,
-- 	cgc_sacado as DocSacado,
--     numero_titulo ,
--     LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--     -- numero_bordero,
--     -- valor_titulo,
--     -- valor_desconto,
--     valor_aberto,
--     data_original data_vencimento,
--     data_emissao
--     -- sequencia_bordero
-- FROM [ALBAREDO].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
-- LEFT JOIN  [ALBAREDO]..SIGFIDC SG
--     ON AB.nosso_numero = SG.ctrl_id
-- UNION ALL
-- SELECT
--     'FIDC SC' Empresa,
--     cgc_cliente as DocCedente,
-- 	cgc_sacado as DocSacado,
--     numero_titulo ,
--     LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--     -- numero_bordero,
--     -- valor_titulo,
--     -- valor_desconto,
--     valor_aberto,
--     data_original data_vencimento,
--     data_emissao
--     -- sequencia_bordero
-- FROM [SCI].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
-- LEFT JOIN  [SCI]..SIGFIDC SG
--     ON AB.nosso_numero = SG.ctrl_id
-- UNION ALL
-- SELECT
--     'PRIME AGRO' Empresa,
--     cgc_cliente as DocCedente,
-- 	cgc_sacado as DocSacado,
--     numero_titulo ,
--     LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--     -- numero_bordero,
--     -- valor_titulo,
--     -- valor_desconto,
--     valor_aberto,
--     data_original data_vencimento,
--     data_emissao
--     -- sequencia_bordero
-- FROM [PRIMEAGRO].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
-- LEFT JOIN  [PRIMEAGRO]..SIGFIDC SG
--     ON AB.nosso_numero = SG.ctrl_id
    
-- UNION ALL
-- SELECT
--     'Z INVEST' Empresa,
--     cgc_cliente as DocCedente,
-- 	cgc_sacado as DocSacado,
--     numero_titulo ,
-- 	LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--     -- numero_bordero,
--     -- valor_titulo,
--     -- valor_desconto,
--     valor_aberto,
--     data_original data_vencimento,
--     data_emissao
--     -- sequencia_bordero
-- FROM [ZFIDC].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
-- LEFT JOIN  [ZFIDC]..SIGFIDC SG
--     ON AB.nosso_numero = SG.ctrl_id
-- UNION ALL
-- SELECT
--     'BASÃ' Empresa,
--     cgc_cliente as DocCedente,
-- 	cgc_sacado as DocSacado,
--     numero_titulo ,
-- 	LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--     -- numero_bordero,
--     -- valor_titulo,
--     -- valor_desconto,
--     valor_aberto,
--     data_original data_vencimento,
--     data_emissao
--     -- sequencia_bordero
-- FROM [BASA].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
-- LEFT JOIN  [BASA]..SIGFIDC SG
--     ON AB.nosso_numero = SG.ctrl_id
-- UNION ALL
-- SELECT
--     'GLOBAL FUTURA' Empresa,
--     cgc_cliente as DocCedente,
-- 	cgc_sacado as DocSacado,
--     numero_titulo ,
-- 	LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--     -- numero_bordero,
--     -- valor_titulo,
--     -- valor_desconto,
--     valor_aberto,
--     data_original data_vencimento,
--     data_emissao
--     -- sequencia_bordero
-- FROM [GLOBALFUTURA].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
-- LEFT JOIN  [GLOBALFUTURA]..SIGFIDC SG
--     ON AB.nosso_numero = SG.ctrl_id
    
-- UNION ALL
SELECT
    'UKF' Empresa,
    cgc_cliente as DocCedente,
	cgc_sacado as DocSacado,
    numero_titulo ,
	LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
    -- numero_bordero,
    -- valor_titulo,
    -- valor_desconto,
    valor_aberto,
    data_original data_vencimento,
    data_emissao
    -- sequencia_bordero
FROM [UKF].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
LEFT JOIN  [UKF]..SIGFIDC SG
    ON AB.nosso_numero = SG.ctrl_id
UNION ALL
-- SELECT
--     'PEROLA' Empresa,
--     cgc_cliente as DocCedente,
-- 	cgc_sacado as DocSacado,
--     numero_titulo ,
--     LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--     -- numero_bordero,
--     -- valor_titulo,
--     -- valor_desconto,
--     valor_aberto,
--     data_original data_vencimento,
--     data_emissao
--     -- sequencia_bordero
-- FROM [PEROLA].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
-- LEFT JOIN  [PEROLA]..SIGFIDC SG
--     ON AB.nosso_numero = SG.ctrl_id
-- UNION ALL
SELECT
	'BONTEMPO' Empresa,
    cgc_cliente as DocCedente,
	cgc_sacado as DocSacado,
	numero_titulo ,
	LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
	-- numero_bordero,
	-- valor_titulo,
	-- valor_desconto,
	valor_aberto,
	data_original data_vencimento,
	data_emissao
	-- sequencia_bordero
FROM [BONTEMPO].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
LEFT JOIN  [BONTEMPO]..SIGFIDC SG
	ON AB.nosso_numero = SG.ctrl_id
UNION ALL
--SELECT
--	'PINPAG' Empresa,
--    cgc_cliente as DocCedente,
--	cgc_sacado as DocSacado,
--	numero_titulo ,
--	LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
--	-- numero_bordero,
--	-- valor_titulo,
--	-- valor_desconto,
--	valor_aberto,
--	data_original data_vencimento,
--	data_emissao
--	-- sequencia_bordero
--FROM [PINPAG].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
--LEFT JOIN  [PINPAG]..SIGFIDC SG
--	ON AB.nosso_numero = SG.ctrl_id
--UNION ALL
SELECT
	'AGROFORTE' Empresa,
    cgc_cliente as DocCedente,
	cgc_sacado as DocSacado,
	numero_titulo ,
	LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
	-- numero_bordero,
	-- valor_titulo,
	-- valor_desconto,
	valor_aberto,
	data_original data_vencimento,
	data_emissao
	-- sequencia_bordero
FROM [AGROFORTE].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
LEFT JOIN  [AGROFORTE]..SIGFIDC SG
	ON AB.nosso_numero = SG.ctrl_id
UNION ALL
SELECT
	'AGIS' Empresa,
    cgc_cliente as DocCedente,
	cgc_sacado as DocSacado,
	numero_titulo ,
	LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
	-- numero_bordero,
	-- valor_titulo,
	-- valor_desconto,
	valor_aberto,
	data_original data_vencimento,
	data_emissao
	-- sequencia_bordero
FROM [AGIS].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
LEFT JOIN  [AGIS]..SIGFIDC SG
	ON AB.nosso_numero = SG.ctrl_id
UNION ALL
# --SELECT
# --	'BEFIC' Empresa,
# --    cgc_cliente as DocCedente,
# --	cgc_sacado as DocSacado,
# --	numero_titulo ,
# ----    COALESCE(
# ----  CASE 
# ----    WHEN LTRIM(RTRIM(SG.controleparticipante)) IS NULL OR LTRIM(RTRIM(SG.controleparticipante)) = '' 
# ----    THEN LTRIM(RTRIM(SG.ctrl_id)) + '000000001'
# ----    ELSE LTRIM(RTRIM(SG.controleparticipante))
# ----  END,
# ----  LTRIM(RTRIM(SG.ctrl_id)) + '000000001'
# ----) AS Operacao,
# --	LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
# --	-- numero_bordero,
# --	-- valor_titulo,
# --	-- valor_desconto,
# --	valor_aberto,
# --	data_original data_vencimento,
# --	data_emissao
# --	-- sequencia_bordero
# --FROM [BEFIC].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
# --LEFT JOIN  [BEFIC]..SIGFIDC SG
# --	ON AB.nosso_numero = SG.ctrl_id
SELECT
	'TORONTO' as Empresa,
    cgc_cliente as DocCedente,
	cgc_sacado as DocSacado,
	numero_titulo ,
	LTRIM(RTRIM(nosso_numero)) + '000000001' Operacao,
	-- numero_bordero,
	-- valor_titulo,
	-- valor_desconto,
	valor_aberto,
	data_original data_vencimento,
	data_emissao
	-- sequencia_bordero
FROM [TORONTO].DBO.[VW_ObtemTitulosEmAbertoFIDC] (NOLOCK) AB
LEFT JOIN  [TORONTO]..SIGFIDC SG
	ON AB.nosso_numero = SG.ctrl_id
"""

# Carregar os dados em chunks e inserir no MySQL
chunk_size = 100000
table_name = 'Stg_OperacoesWBA'  # Nome da tabela destino

chunks = pd.read_sql(sql_query, source_engine, chunksize=chunk_size)

for chunk_number, chunk in enumerate(chunks):
    # Convertendo o DataFrame em listas de tuplas para a inserção no MySQL
    data = [tuple(row) for row in chunk.to_numpy()]
    columns = ', '.join(chunk.columns)
    placeholders = ', '.join(['%s'] * len(chunk.columns))

    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    cursor.executemany(insert_query, data)
    connection.commit()

    # Imprimir mensagem indicando que foram inseridos registros
    print(f"Inseridos {chunk_size * (chunk_number + 1)} registros.")

# Fechar conexões
cursor.close()
connection.close()
source_engine.close()


# # MYSQL

# # SPIRIT

# In[ ]:


import pandas as pd
import mysql.connector

# Configurações para o banco de dados MySQL (origem)
orig_server = 'dbc-sp-1-instance-1-instance-1.ciaao5zm9beh.sa-east-1.rds.amazonaws.com'
orig_database = 'cataliseProd'
orig_user = 'dwuser'
orig_password = 'omcUXQpd1ZcFLwRVdPi6'

# Configurações para o banco de dados MySQL (destino)
dest_server = 'catalise-bi-dados-cluster.cluster-ciaao5zm9beh.sa-east-1.rds.amazonaws.com'
dest_database = 'DW_STAGING'
dest_user = 'admin'
dest_password = 'yb222KraY7PTN0jbH7P3'

# Conectar ao banco de dados MySQL (origem)
orig_connection = mysql.connector.connect(
    host=orig_server,
    database=orig_database,
    user=orig_user,
    password=orig_password
)

# Conectar ao banco de dados MySQL (destino)
dest_connection = mysql.connector.connect(
    host=dest_server,
    database=dest_database,
    user=dest_user,
    password=dest_password
)

if not orig_connection.is_connected():
    print('Falha na conexão ao MySQL de origem')
    exit(1)

if not dest_connection.is_connected():
    print('Falha na conexão ao MySQL de destino')
    exit(1)

# Função para inserir um pedaço do DataFrame na tabela MySQL
def insert_dataframe_to_mysql(df, connection, table_name):
    cursor = connection.cursor()
    for i, row in df.iterrows():
        sql = f"INSERT INTO {table_name} ({', '.join(row.index)}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, tuple(row))
    connection.commit()
    cursor.close()

# Executar a consulta SQL da origem e salvar em um DataFrame em chunks
sql_query = """
SELECT
    REPLACE(FUN.nome, '	', '') as Empresa,
    CED.documento as DocCedente,
    SAC.documento as DocSacado,
    EST.numeroDoDocumento as numero_titulo,
    EST.nDeControleDoParticipante as Operacao,
    EST.ValorDoTituloFace as valor_aberto,
    EST.dataDoVencimentoDoTitulo as data_vencimento,
    EST.dataDaEmissaoDoTitulo as data_emissao
FROM
    cataliseProd.estoque EST
INNER JOIN cataliseProd.fundos FUN
    ON
    EST.idFundo = FUN.id
INNER JOIN
cataliseProd.cedente_sacado CED
    ON EST.idCedente = CED.id
INNER JOIN
cataliseProd.cedente_sacado SAC
    ON EST.idSacado = SAC.id
WHERE FUN.nome IN ('CREDILOG II FUNDO DE INVESTIMENTO EM DIREITOS CREDITÓRIOS',
                    'CREDILOG - FUNDO DE INVESTIMENTO EM DIREITOS CREDITÓRIOS NÃO-PADRONIZADOS	', 
                    'GREENWOOD FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS NAO PADRONIZADOS',
                    'AGROCETE FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS',
                    'USECORP CATÁLISE FUNDO DE INVESTIMENTO EM DIREITOS CREDITÓRIOS SEGMENTO COMERCIAL DE RESP LIMITADA',
                    'DBANK FUNDO DE INVESTIMENTO EM DIREITOS CREDITÓRIOS DE RESPONSABILIDADE LIMITADA',
                    'FUTURO CAPITALFUNDO DE INVESTIMENTO EM DIREITOS  CREDITÓRIOS DE RESPONSABILIDADE LIMITADA',
                    'CAPITALIZA FUNDO DE INVESTIMENTO EM DIREITOS CREDITÓRIOS DE RESPONSABILIDADE LIMITADA',
                    'PEROLA FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS DE RESPOSABILIDADE LIMITADA',
                    'NINE CAPITAL FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS',
                    'BASÃ  FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS DE RESPONSABILIDADE LIMITADA',
                    'BLUE ROCKET FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS NAO-PADRONIZADO',
                    'AF6 FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS NAO-PADRONIZADO',
                    'NR11 FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS DE RESPONSABILIDADE LIMITADA',
                    'CREDIAL BANK PAN FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS NAO-PADRONIZADOS',
                    'GLOBAL FUTURA FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS DE RESPONSABILIDADE LIMITADA',
                    'PRIME AGRO FUNDO DE INVESTIMENTO EM DIREITOS CREDITÓRIOS DE RESPONSABILIDADE LIMITADA',
                    'Z INVEST FUNDO DE INVESTIMENTO EM DIREITOS CREDITÓRIOS  RESPONSABILIDADE ILIMITADA',
                    'ANIL FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS DE RESPONSABILIDADE LIMITADA',
                    'ANVERES FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS DE RESPONSABILIDADE LIMITADA',
                    'VITTRA FUNDO DE INVESTIMENTO EM DIREITOS CREDITÓRIOS DE RESPONSABILIDADE LIMITADA',   
                    'MASTRENN FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO MULTIMERCADO CREDITO PRIVADO',
                    'SC FUNDO DE INVESTIMENTO DIREITOS CREDITORIOS RESPONSABILIDADE ILIMITADA',
                    'RHB CRED FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS DE RESPONSABILIDADE LIMITADA',
                    'SMT AGRO FUNDO DE INVESTIMENTO EM DIREITOS CREDITÓRIOS DE RESPONSABILIDADE LIMITADA',
                    'ALBAREDO FUNDO DE INVESTIMENTO EM DIREITOS CREDITÓRIOS',
                    'VALENTE FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS',
                    'LOGFIN FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS',
                    'PINPAG FUNDO DE INVESTIMENTO EM DIREITOS CREDITÓRIOS RESPONSABILIDADE ILIMITADA',
                    'VERGINIA FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS',
                    'BEFIC FUNDO DE INVESTIMENTO DIREITOS CREDITÓRIOS DE  RESPONSABILIDADE LIMITADA',
                    'BELL FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS',
                    'VISHNU FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS',
                    'TERTON FUNDO DE INVESTIMENTO DIREITOS CREDITORIOS DE RESPONSABILIDADE LIMITADA',
                    'LEX CAPITAL FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS DE RESPONSABILIDADE LIMITADA',
                    'BRAVOS FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS',
                    'UNITY FUNDO DE INVESTIMENTO EM DIREITOS CREDITÓRIOS NÃO-PADRONIZADOS',
                    'IPE - FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS NAO-PADRONIZADOS',
                    'BOAZ FUNDO DE INVESTIMENTO EM DIREITOS CREDITORIOS DE RESPONSABILIDADE LIMITADA'
                    )
AND EST.dataDeBaixa IS NULL
AND EST.excluido IS NULL
"""

chunk_size = 10000
table_name = 'Stg_OperacoesWBA'

orig_cursor = orig_connection.cursor()
orig_cursor.execute(sql_query)

while True:
    chunk = orig_cursor.fetchmany(chunk_size)
    if not chunk:
        break

    chunk_df = pd.DataFrame(chunk, columns=[desc[0] for desc in orig_cursor.description])

    # Fazer qualquer transformação necessária nos dados do chunk

    # Enviar o chunk para o MySQL de destino
    insert_dataframe_to_mysql(chunk_df, dest_connection, table_name)

    # Imprimir mensagem indicando que foram inseridos a cada 10000
    print(f"Inseridos {len(chunk_df)} registros.")

# Fechar conexões
orig_cursor.close()
orig_connection.close()
dest_connection.close()


# In[ ]:


datetime.today().strftime('Data: %Y-%m-%d Hora: %H:%m')


# In[ ]:


# CREATE TABLE DW_STAGING.Stg_OperacoesWBA (
#     Empresa VARCHAR(255),
#     numero_titulo VARCHAR(255),
#     Operacao VARCHAR(255),
#     valor_aberto DECIMAL(10, 2),
#     data_vencimento DATE,
#     data_emissao DATE
# );


# # Stand - Desativado

# In[ ]:


# import pandas as pd
# import pyodbc
# import mysql.connector

# # Configurações para o banco de dados SQL Server (fonte)
# source_server = '172.31.6.45'
# source_database = 'POSICAO'
# source_user = 'sa'
# source_password = 'st@nd159'

# # Configurações para o banco de dados MySQL (destino)
# dest_server = 'catalise-bi-dados-cluster.cluster-ciaao5zm9beh.sa-east-1.rds.amazonaws.com'
# dest_database = 'DW_STAGING'
# dest_user = 'admin'
# dest_password = 'yb222KraY7PTN0jbH7P3'

# # Configuração da conexão com o SQL Server usando pyodbc
# sql_server_connection_str = (
#     f"DRIVER={{ODBC Driver 17 for SQL Server}};"
#     f"SERVER={source_server};"
#     f"DATABASE={source_database};"
#     f"UID={source_user};"
#     f"PWD={source_password}"
# )
# sql_server_connection = pyodbc.connect(sql_server_connection_str)
# sql_server_cursor = sql_server_connection.cursor()

# # Configuração da conexão com o MySQL usando mysql.connector
# mysql_connection = mysql.connector.connect(
#     host=dest_server,
#     database=dest_database,
#     user=dest_user,
#     password=dest_password
# )

# if not mysql_connection.is_connected():
#     print('Falha na conexão ao MySQL')
#     exit(1)

# # Função para inserir um pedaço do DataFrame na tabela MySQL
# def insert_dataframe_to_mysql(df, connection, table_name):
#     cursor = connection.cursor()
#     for i, row in df.iterrows():
#         sql = f"INSERT INTO {table_name} ({', '.join(row.index)}) VALUES ({', '.join(['%s'] * len(row))})"
#         cursor.execute(sql, tuple(row))
#     connection.commit()
#     cursor.close()

# # Executar a consulta SQL no SQL Server e salvar em um DataFrame em chunks
# sql_server_query = """
# SELECT
# 		Empresa
# 		,doc_cedente as DocCedente
# 		,doc_sacado as DocSacado
# 		,numero_titulo
# 		,numero_bordero_novo AS Operacao
# 		,valor_titulo AS valor_aberto
# 		-- ,valor_aberto
# 		,data_vencimento
# 		,CAST(REPLACE(data_emissao, '1921', '2021') AS date) as data_emissao
# FROM POSICAO..vw_connect3_abertos (NOLOCK)
# where Empresa NOT IN ( 'UNITY','PAYCARGO', 'DISAM', 'GREENWOOD', 'AGROFORTE', 'AGIS')
# """

# chunk_size = 10000
# table_name = 'Stg_OperacoesWBA'  # Nome da tabela no MySQL

# sql_server_cursor.execute(sql_server_query)

# while True:
#     chunk = sql_server_cursor.fetchmany(chunk_size)
#     if not chunk:
#         break

#     chunk_df = pd.DataFrame.from_records(chunk, columns=[desc[0] for desc in sql_server_cursor.description])

#     # Fazer qualquer transformação necessária nos dados do chunk

#     # Enviar o chunk para o MySQL
#     insert_dataframe_to_mysql(chunk_df, mysql_connection, table_name)

#     # Imprimir mensagem indicando que foram inseridos a cada 10000
#     print(f"Inseridos {len(chunk_df)} registros.")

# # Fechar conexões
# sql_server_cursor.close()
# sql_server_connection.close()
# mysql_connection.close()


# In[2]:


get_ipython().system('jupyter nbconvert --to script STG_OperacoesWBA 1.ipynb')


# In[ ]:




