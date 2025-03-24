Query:


SELECT
	COUNT(DISTINCT DocFundo),
	DtPosicao
from
	DW_CORPORATIVO.Ft_Patrimonio fp
group by
	DtPosicao
order by
	DtPosicao desc



---------------------------------------------------------





SELECT
	MAX(DtReferencia)
FROM
	DW_CORPORATIVO.Ft_Despesas fd

---------------------------------------------------------


SELECT
	DISTINCT NmFundo
from
	DW_CORPORATIVO.Ft_Operacoes fo
WHERE DtReferencia = '2024-10-03'
AND NmFundo LIKE '%3G%'


---------------------------------------------------------

SELECT
	DISTINCT nmfundo
from
	DW_CORPORATIVO.Ft_Patrimonio fp
WHERE
	NmFundo like '%SMT%'



---------------------------------------------------------



SELECT
	*
FROM
	DW_CORPORATIVO.Ft_Patrimonio fp
WHERE
	NmFundo LIKE '%ANTARES FIC%'
	AND DtPosicao > '2024-09-30'


---------------------------------------------------------



SELECT
    fp1.DocFundo,
    fp1.nmfundo
FROM
    DW_CORPORATIVO.Ft_Patrimonio fp1
LEFT JOIN
    DW_CORPORATIVO.Ft_Patrimonio fp2 ON fp1.DocFundo = fp2.DocFundo
    AND fp2.DtPosicao = '2024-10-08'
WHERE
    fp1.DtPosicao = '2024-10-07'
    AND fp2.DocFundo IS NULL







QUERY'S DE CHECK NO APOS CARGA:

ESTOQUE:

SELECT DISTINCT DtReferencia FROM DW_CORPORATIVO.Ft_Estoque fe


SELECT
	COUNT(DISTINCT DOCFUNDO),
	DocFundo
FROM
	DW_CORPORATIVO.vw_Estoque ve
GROUP BY
	DocFundo




SELECT DISTINCT
    fe.DocFundo,
    df.NmFundo,
    fe.DtReferencia
FROM
    DW_CORPORATIVO.Ft_Estoque fe
LEFT JOIN
    DW_CORPORATIVO.Dm_Fundo df
ON
    fe.DocFundo = df.DocFundo
WHERE
    fe.DtReferencia = '2024-11-12'

 


PATRIMONIO:

SELECT DISTINCT NmFundo FROM DW_CORPORATIVO.Ft_Patrimonio fp 

SELECT DISTINCT DtPosicao FROM DW_CORPORATIVO.Ft_Patrimonio fp 



lIQUIDADOS:

SELECT NmFundo, DataETL, DtMovimento FROM DW_STAGING.Stg_LiquidadosBaixados slb 

OPERACOES:

SELECT DtReferencia, DataETL, NmFundo FROM DW_CORPORATIVO.Ft_Operacoes fo 

SELECT DISTINCT NmFundo FROM DW_CORPORATIVO.Ft_Operacoes fo 




Criação da Carteira Diária:



CREATE TABLE DW_STAGING.Ft_CarteiraDiaria (
    NmFundo VARCHAR(255) NOT NULL,       
    Tp_Fundo VARCHAR(255) NOT NULL,
    Classificacao VARCHAR(255) NOT NULL,
    DtPosicao DATE NOT NULL,              
    Nome VARCHAR(255) NOT NULL,           
    Espec VARCHAR(255),                   
    Qnt DECIMAL(15, 4),                   
    VlrMercado DECIMAL(20, 2),            
    DataVenc DATE DEFAULT NULL,          
    DataAplic DATE DEFAULT NULL,          
    Descricao VARCHAR(255),               
    Cod INT NOT NULL,                     
    Grupo VARCHAR(255) NOT NULL           
)



VERIFICAÇÂO DE DUPLICADOS:


SELECT DtPosicao, NmFundo, VlrAtivo, COUNT(*) AS QuantidadeDuplicadas 
FROM DW_CORPORATIVO.Dm_OutrosAtivos doa
WHERE doa.Ativo = 'PDD/MTM'
GROUP BY DtPosicao, NmFundo
HAVING COUNT(*) > 1
ORDER BY DtPosicao

SELECT DISTINCT
    fe.DocFundo,
    df.NmFundo,
    fe.DtReferencia
FROM
    DW_CORPORATIVO.Ft_Estoque fe
LEFT JOIN
    DW_CORPORATIVO.Dm_Fundo df
ON
    fe.DocFundo = df.DocFundo
WHERE
    fe.DtReferencia = '2024-12-19'
    
  
    
  SELECT DISTINCT
    fe.DocFundo,
    df.NmFundo,
    fe.DtReferencia
FROM
    DW_CORPORATIVO.Ft_Estoque fe
LEFT JOIN
    DW_CORPORATIVO.Dm_Fundo df
ON
    fe.DocFundo = df.DocFundo
WHERE
    fe.DtReferencia = '2024-12-20'
    
    
    SELECT DISTINCT
    fp.DocFundo,
    df.NmFundo,
    fp.DtPosicao 
FROM
    DW_CORPORATIVO.Ft_Patrimonio fp 
LEFT JOIN
    DW_CORPORATIVO.Dm_Fundo df
ON
    fp.DocFundo = df.DocFundo
WHERE
    fp.DtPosicao = '2024-12-20'


 SELECT * FROM DW_CORPORATIVO.Ft_Estoque fe 
 WHERE DocFundo = '51.660.048/0001-67' AND DtReferencia = '2024-12-19'

PATRIMONIO:

SELECT DISTINCT NmFundo, DtPosicao, DataETL FROM DW_CORPORATIVO.Ft_Patrimonio fp 
WHERE NmFundo lIKE '%BINVE%'

SELECT DISTINCT NmFundo FROM DW_CORPORATIVO.Ft_Patrimonio fp 


SELECT DISTINCT DtReferencia FROM DW_STAGING.Stg_Estoque se 


lIQUIDADOS:

SELECT DISTINCT NmFundo FROM DW_STAGING.Stg_LiquidadosBaixados slb 

OPERACOES:

SELECT DtReferencia, DataETL, NmFundo FROM DW_CORPORATIVO.Ft_Operacoes fo 

SELECT DISTINCT NmFundo FROM DW_CORPORATIVO.Ft_Operacoes fo 



SELECT DISTINCT Nmfundo, DocFundo
FROM DW_CORPORATIVO.Ft_Patrimonio fp
WHERE Nmfundo LIKE '%3G BANK%'
   OR Nmfundo LIKE '%BETTA%'
   OR Nmfundo LIKE '%BINVEST%'
   OR Nmfundo LIKE '%FERPAR%'
   OR Nmfundo LIKE '%LOGFIN%'
   OR Nmfundo LIKE '%P2%'
   OR Nmfundo LIKE '%PEROLA%'
   OR Nmfundo LIKE '%TOPCRED%'
   OR Nmfundo LIKE '%TRACTOR%'
   OR Nmfundo LIKE '%UNITY%'
   OR Nmfundo LIKE '%VALENTE%'
   OR Nmfundo LIKE '%FINTRUST%'
   OR Nmfundo LIKE '%BRAVOS%'
   OR Nmfundo LIKE '%DFC%'
   OR Nmfundo LIKE '%ARIS%'
   OR Nmfundo LIKE '%BR8%'
   OR Nmfundo LIKE '%PRIMER%';



WITH id AS (
	SELECT IDControle FROM(
		SELECT
			*,
			RANK() OVER (PARTITION BY DtPosicao, NmFundo ORDER BY DataETL DESC) rk
		
		FROM DW_CORPORATIVO.Ft_Patrimonio fp 
			) v2
	WHERE rk > 1
	)

 DELETE
FROM DW_CORPORATIVO.Ft_Patrimonio fp 
WHERE IDControle in (SELECT * FROM id)









WITH DUPLICADOS AS (

	SELECT * FROM (
	
		SELECT
			IDControle, 
			ChaveUnica,
			RANK() OVER (PARTITION BY ChaveUnica ORDER BY IdControle DESC) rk
		FROM
			DW_CORPORATIVO.Ft_Estoque
		WHERE 1=1
-- 		AND DocFundo = '41.520.452/0001-81'
 		AND DtReferencia = "2024-12-19"

	) v2
	where rk > 1 
)

DELETE FROM DW_CORPORATIVO.Ft_Estoque WHERE IDControle in (SELECT IDControle from DUPLICADOS )






WITH TEMP AS(
SELECT 
	IDControle,
	NmFundo ,
	DtReferencia,
	REPLACE( REPLACE( REPLACE( DocCedente, '.', ''),'-',''),'/','') DocCedente,
	NuDocumento	,
	Operacao
FROM DW_CORPORATIVO.Ft_Operacoes fo 
),

DUPLICADOS AS(
	SELECT 
		IDControle,
		NmFundo ,
		DtReferencia,
		DocCedente,
		NuDocumento,
		Operacao,
		RANK() OVER (PARTITION BY NmFundo, DtReferencia,  DocCedente, NuDocumento, Operacao ORDER BY IdControle DESC) rk
	FROM TEMP
	ORDER BY rk DESC 
	)

DELETE FROM DW_CORPORATIVO.Ft_Operacoes WHERE IDControle in (SELECT IDControle from DUPLICADOS WHERE rk > 1 )


SELECT DISTINCT Nmfundo, DocFundo
FROM DW_CORPORATIVO.Ft_Patrimonio fp
WHERE NmFundo LIKE '%USE%'

SELECT
	DISTINCT NmFundo, DocFundo
from
	DW_CORPORATIVO.Ft_Patrimonio fp
WHERE
	NmFundo LIKE '%PROSPER%'
	OR NmFundo LIKE '%VED%'
	
SELECT DISTINCT NmFundo FROM DW_CORPORATIVO.Ft_Patrimonio fp 

	
	
UPDATE DW_CORPORATIVO.Ft_Patrimonio 
SET NmFundo = CASE 
    WHEN NmFundo = 'ABX9 FIM CP' THEN 'FIM ABX9'
    WHEN NmFundo = 'AF FC FIM CP RESP LT' THEN 'FICFIM AF'
    WHEN NmFundo = 'APG FIDC - MEZANINO 1' THEN 'FIDC APG MZ1'
    WHEN NmFundo = 'APG FIDC - MEZANINO 2' THEN 'FIDC APG MZ2'
    WHEN NmFundo = 'APG FIDC - SENIOR 1' THEN 'FIDC APG SR1'
    WHEN NmFundo = 'APG FIDC - SENIOR 2' THEN 'FIDC APG SR2'
    WHEN NmFundo = 'AROEIRA FIC FIM CP' THEN 'FICFIM AROEIRA'
    WHEN NmFundo = 'ATICO FC FIM CP' THEN 'FICFIM ATICO'
    WHEN NmFundo = 'ATLANTICO FIC FIM' THEN 'FICFIM ATLANTICO'
    WHEN NmFundo = 'BAUNKER II FIM CP' THEN 'FIM BAUNKER II'
    WHEN NmFundo = 'CAMPOS GERAIS FC FIM' THEN 'FICFIM CAMPOS GERAIS'
    WHEN NmFundo = 'CARGO HOLD FIC FIM' THEN 'FICFIM CARGO HOLD'
    WHEN NmFundo = 'CWB CATALISE FC FIM' THEN 'FICFIM CWB CATALISE'
    WHEN NmFundo = 'DANIELI FIC FIM CP' THEN 'FICFIM DANIELI'
    WHEN NmFundo = 'DANIELI II FC FIM CP' THEN 'FICFIM DANIELI II'
    WHEN NmFundo = 'E11EVEN FIC FIM CP' THEN 'FICFIM E11EVEN'
    WHEN NmFundo = 'EARNIER MAPS FIM CP' THEN 'FIM EARNIER'
    WHEN NmFundo = 'F2 BANK FC FIM CP' THEN 'FICFIM F2 BANK'
    WHEN NmFundo = 'FBH FIC FIM CP' THEN 'FICFIM FBH'
    WHEN NmFundo = 'FBJ77 FC FIM CP' THEN 'FICFIM FBJ77'
    WHEN NmFundo = 'FFBANK FC FIM' THEN 'FICFIM FFBANK'
    WHEN NmFundo = 'FIDC 3G BANK NP' THEN 'FIDC 3G BANK'
    WHEN NmFundo = 'FIDC BETTA MEZ4' THEN 'FIDC BETTA MZ4'
    WHEN NmFundo = 'FIDC BETTA SR 2' THEN 'FIDC BETTA SR2'
    WHEN NmFundo = 'FIDC BETTA SUB' THEN 'FIDC BETTA'
    WHEN NmFundo = 'FIDC BR8 MEZ' THEN 'FIDC BR8 MZ'
    WHEN NmFundo = 'FIDC BR8 SEN' THEN 'FIDC BR8 SR'
    WHEN NmFundo = 'FIDC BR8 SUB' THEN 'FIDC BR8'
    WHEN NmFundo = 'FIDC CRDLOG 2SR' THEN 'FIDC CREDILOG 2 SR'
    WHEN NmFundo = 'FIDC FERPAR NP' THEN 'FIDC FERPAR'
    WHEN NmFundo = 'FIDC FINANZE SB' THEN 'FIDC FINANZE'
    WHEN NmFundo = 'FIDC FINTRUST M' THEN 'FIDC FINTRUST MZ'
    WHEN NmFundo = 'FIDC NINE' THEN 'FIDC NINE CAPITAL'
    WHEN NmFundo = 'FIDC PAYCARGO S' THEN 'FIDC PAY CARGO SR'
    WHEN NmFundo = 'FIDC TOPCRED MA' THEN 'FIDC TOPCRED MZ1'
    WHEN NmFundo = 'FIDC TOPCRED MB' THEN 'FIDC TOPCRED MZ2'
    WHEN NmFundo = 'FIDC TRACTOR MA' THEN 'FIDC TRACTOR MZ1'
    WHEN NmFundo = 'FIDC UNITY NP' THEN 'FIDC UNITY'
    WHEN NmFundo = 'FIGUEIREDO FIC FIM' THEN 'FICFIM FIGUEIREDO'
    WHEN NmFundo = 'GANESHA FIC FIM CP' THEN 'FICFIM GANESHA'
    WHEN NmFundo = 'GLOBAL FIC FIM CP' THEN 'FICFIM GLOBAL'
    WHEN NmFundo = 'GMOT FC FIM' THEN 'FICFIM GMOT'
    WHEN NmFundo = 'GO TOGETHER FIC FIM' THEN 'FICFIM GO TOGETHER'
    WHEN NmFundo = 'GPW INV FIC FIM CP' THEN 'FICFIM GPW'
    WHEN NmFundo = 'GRUPO CORES FIC FIM' THEN 'FICFIM GRUPO CORES'
    WHEN NmFundo = 'JB FIM CP' THEN 'FIM JB'
    WHEN NmFundo = 'LIBRA FC FIM CP' THEN 'FICFIM LIBRA'
    WHEN NmFundo = 'LION FC FIDC' THEN 'FICFIDC LION'
    WHEN NmFundo = 'MASTRENN FIC FIM CP' THEN 'FICFIM MASTRENN'
    WHEN NmFundo = 'NOVA GLOBAL FIC FIM' THEN 'FICFIM NOVA GLOBAL'
    WHEN NmFundo = 'OIKOS FC FIM CP' THEN 'FICFIM OIKOS'
    WHEN NmFundo = 'PAN FC FIM CP' THEN 'FICFIM PAN'
    WHEN NmFundo = 'PROSPER FIM CP' THEN 'FIM PROSPER'
    WHEN NmFundo = 'RHB INVEST FIC FIM' THEN 'FICFIM RHB'
    WHEN NmFundo = 'RINCAO FIC FIM' THEN 'FICFIM RINCAO'
    WHEN NmFundo = 'SCI SAO CR FC FIM CP' THEN 'FICFIM SCI'
    WHEN NmFundo = 'SECULO FIC FIM CP' THEN 'FICFIM SECULO'
    WHEN NmFundo = 'UKF FIC FIM' THEN 'FICFIM UKF'
    WHEN NmFundo = 'VEDELAGO FIC FIM' THEN 'FICFIM VEDELAGO'
    WHEN NmFundo = 'FIC ABIATAR' THEN 'FICFIM ABIATAR'
    WHEN NmFundo = 'FIC ARCTURUS' THEN 'FIM ARCTURUS'
    WHEN NmFundo = 'FIC ARTANIS' THEN 'FIM ARTANIS'
    WHEN NmFundo = 'FIC GOLIATH' THEN 'FIM GOLIATH'
    WHEN NmFundo = 'FICFIM PROSPER' THEN 'FIM PROSPER'
    WHEN NmFundo = 'FICFIM VEDALAGO' THEN 'FICFIM VEDELAGO'
    WHEN NmFundo = 'Z CORP FIC FIM' THEN 'FICFIM Z CORP'
    
    
    WHEN NmFundo = 'FIDC 3G BANK NP' THEN 'FIDC 3G BANK'
    WHEN NmFundo = 'FIDC FERPAR NP' THEN 'FIDC FERPAR'
    WHEN NmFundo = 'FIDC FINTRUST M' THEN 'FIDC FINTRUST'
    WHEN NmFundo = 'FIDC TOPCRED MA' THEN 'FIDC TOPCRED MZ1'
    WHEN NmFundo = 'FIDC TRACTOR MA' THEN 'FIDC TRACTOR MZ1'
    WHEN NmFundo = 'FIDC UNITY NP' THEN 'FIDC UNITY' 
    ELSE NmFundo
END;
 
SELECT
	*
from
	DW_STAGING.Ft_CarteiraDiaria fcd
WHERE
	NmFundo = 'FIM EARNIER'
	AND DtPosicao < '2024-12-15'
 



UPDATE DW_CORPORATIVO.Ft_Patrimonio 
SET NmFundo = CASE 
    WHEN NmFundo = 'FIDC USECORP CATÁLISE' THEN 'FIDC USECORP CATALISE'
    ELSE NmFundo
END;



SELECT
	fp.NmFundo,
	MinData.MinDtPosicao,
	fp.VlrCota,
	fp.DataETL
FROM
	DW_CORPORATIVO.Ft_Patrimonio fp
INNER JOIN
    (
	SELECT
		NmFundo,
		MIN(DtPosicao) AS MinDtPosicao
	FROM
		DW_CORPORATIVO.Ft_Patrimonio
	GROUP BY
		NmFundo) AS MinData
ON
	fp.NmFundo = MinData.NmFundo
	AND fp.DtPosicao = MinData.MinDtPosicao
ORDER BY
	MinData.MinDtPosicao DESC
	
	
	
	SELECT DISTINCT NmFundo, DtPosicao FROM DW_CORPORATIVO.Ft_Patrimonio fp 
	WHERE DtPosicao = '2024-12-19'

	
SELECT
	COUNT(DISTINCT DocFundo),
	DtPosicao, NmFundo 
from
	DW_CORPORATIVO.Ft_Patrimonio fp
group by
	DtPosicao
order by
	DtPosicao desc




DELETE FROM DW_CORPORATIVO.Ft_Operacoes fo
WHERE DtReferencia BETWEEN '2024-11-01' AND '2024-11-29'
  
 
DELETE FROM DW_STAGING.Stg_LiquidadosBaixados slb
WHERE DtMovimento BETWEEN '2024-11-01' AND '2024-11-29'








ERY'S DE CHECK NO APOS CARGA:

ESTOQUE:

SELECT DISTINCT DtReferencia FROM DW_CORPORATIVO.Ft_Estoque fe

select DISTINCT NmFundo from DW_STAGING.Stg_Estoque

SELECT
	COUNT(DISTINCT DOCFUNDO),
	DocFundo
FROM
	DW_CORPORATIVO.vw_Estoque ve
GROUP BY
	DocFundo





SELECT DISTINCT
    fe.DocFundo,
    df.NmFundo,
    fe.DtReferencia
FROM
    DW_CORPORATIVO.Ft_Estoque fe
LEFT JOIN
    DW_CORPORATIVO.Dm_Fundo df
ON
    fe.DocFundo = df.DocFundo
WHERE
    fe.DtReferencia = '2025-01-07'

 TRUNCATE TABLE DW_STAGING.Ft_CarteiraDiaria

select DISTINCT DtPosicao from DW_STAGING.Ft_CarteiraDiaria


PATRIMONIO:

SELECT DISTINCT NmFundo FROM DW_CORPORATIVO.Ft_Patrimonio fp 
WHERE NmFundo LIKE '%FIDC LIBRA%'

SELECT DISTINCT DtPosicao FROM DW_CORPORATIVO.Ft_Patrimonio fp 



lIQUIDADOS:

SELECT NmFundo, DataETL, DtMovimento FROM DW_STAGING.Stg_LiquidadosBaixados slb 

SELECT DISTINCT NmFundo FROM DW_STAGING.Stg_LiquidadosBaixados slb 

OPERACOES:

SELECT DtReferencia, DataETL, NmFundo FROM DW_CORPORATIVO.Ft_Operacoes fo 

SELECT DISTINCT NmFundo FROM DW_CORPORATIVO.Ft_Operacoes fo 

truncate table DW_STAGING.Stg_Estoque

select DISTINCT DtReferencia, NmFundo from DW_STAGING.Stg_Estoque


Criação da Carteira Diária:



CREATE TABLE DW_STAGING.Ft_CarteiraDiaria (
    NmFundo VARCHAR(255) NOT NULL,       
    Tp_Fundo VARCHAR(255) NOT NULL,
    Classificacao VARCHAR(255) NOT NULL,
    DtPosicao DATE NOT NULL,              
    Nome VARCHAR(255) NOT NULL,           
    Espec VARCHAR(255),                   
    Qnt DECIMAL(15, 4),                   
    VlrMercado DECIMAL(20, 2),            
    DataVenc DATE DEFAULT NULL,          
    DataAplic DATE DEFAULT NULL,          
    Descricao VARCHAR(255),               
    Cod INT NOT NULL,                     
    Grupo VARCHAR(255) NOT NULL           
)



VERIFICAÇÂO DE DUPLICADOS:


SELECT DtPosicao, NmFundo, VlrAtivo, COUNT(*) AS QuantidadeDuplicadas 
FROM DW_CORPORATIVO.Dm_OutrosAtivos doa
WHERE doa.Ativo = 'PDD/MTM'
GROUP BY DtPosicao, NmFundo
HAVING COUNT(*) > 1
ORDER BY DtPosicao

SELECT DISTINCT
    fe.DocFundo,
    df.NmFundo,
    fe.DtReferencia
FROM
    DW_CORPORATIVO.Ft_Estoque fe
LEFT JOIN
    DW_CORPORATIVO.Dm_Fundo df
ON
    fe.DocFundo = df.DocFundo
WHERE
    fe.DtReferencia = '2025-01-07'
    
  
    
    
    
    
  SELECT DISTINCT
    fe.DocFundo,
    df.NmFundo,
    fe.DtReferencia
FROM
    DW_CORPORATIVO.Ft_Estoque fe
LEFT JOIN
    DW_CORPORATIVO.Dm_Fundo df
ON
    fe.DocFundo = df.DocFundo
WHERE
    fe.DtReferencia = '2025-01-08'
    
    
    SELECT DISTINCT
    fp.DocFundo,
    df.NmFundo,
    fp.DtPosicao 
FROM
    DW_CORPORATIVO.Ft_Patrimonio fp 
LEFT JOIN
    DW_CORPORATIVO.Dm_Fundo df
ON
    fp.DocFundo = df.DocFundo
WHERE
    fp.DtPosicao = '2025-01-08'


 SELECT * FROM DW_CORPORATIVO.Ft_Estoque fe 
 WHERE DocFundo = '51.660.048/0001-67' AND DtReferencia = '2024-12-19'

PATRIMONIO:

SELECT DISTINCT NmFundo, DtPosicao, DataETL FROM DW_CORPORATIVO.Ft_Patrimonio fp 
WHERE NmFundo lIKE '%BINVE%'

SELECT DISTINCT NmFundo, DtPosicao FROM DW_CORPORATIVO.Ft_Patrimonio fp 
WHERE NmFundo LIKE '%PRIME AGRO%'


SELECT * FROM DW_STAGING.Stg_Estoque se 
WHERE DtReferencia = '2025-01-06' AND NmFundo LIKE '%APG%'


lIQUIDADOS:

SELECT DISTINCT NmFundo FROM DW_STAGING.Stg_LiquidadosBaixados slb


OPERACOES:

SELECT DtReferencia, DataETL, NmFundo FROM DW_CORPORATIVO.Ft_Operacoes fo 

SELECT DISTINCT NmFundo FROM DW_CORPORATIVO.Ft_Operacoes fo 


SELECT *
FROM DW_STAGING.Stg_LiquidadosBaixados
WHERE DtMovimento BETWEEN '2024-12-01' AND '2024-12-31'
  AND NmFundo = 'FIDC APG';



SELECT *
FROM DW_STAGING.Stg_LiquidadosBaixados
WHERE DtMovimento BETWEEN '2024-12-01' AND '2024-12-31'
  AND NmFundo = 'FIDC AGROFORTE';







SELECT DISTINCT Nmfundo, DocFundo
FROM DW_CORPORATIVO.Ft_Patrimonio fp
WHERE Nmfundo LIKE '%3G BANK%'
   OR Nmfundo LIKE '%BETTA%'
   OR Nmfundo LIKE '%BINVEST%'
   OR Nmfundo LIKE '%FERPAR%'
   OR Nmfundo LIKE '%LOGFIN%'
   OR Nmfundo LIKE '%P2%'
   OR Nmfundo LIKE '%PEROLA%'
   OR Nmfundo LIKE '%TOPCRED%'
   OR Nmfundo LIKE '%TRACTOR%'
   OR Nmfundo LIKE '%UNITY%'
   OR Nmfundo LIKE '%VALENTE%'
   OR Nmfundo LIKE '%FINTRUST%'
   OR Nmfundo LIKE '%BRAVOS%'
   OR Nmfundo LIKE '%DFC%'
   OR Nmfundo LIKE '%ARIS%'
   OR Nmfundo LIKE '%BR8%'
   OR Nmfundo LIKE '%PRIMER%';



WITH id AS (
	SELECT IDControle FROM(
		SELECT
			*,
			RANK() OVER (PARTITION BY DtPosicao, NmFundo ORDER BY DataETL DESC) rk
		
		FROM DW_CORPORATIVO.Ft_Patrimonio fp 
			) v2
	WHERE rk > 1
	)

 DELETE
FROM DW_CORPORATIVO.Ft_Patrimonio fp 
WHERE IDControle in (SELECT * FROM id)





WITH DUPLICADOS AS (

	SELECT * FROM (
	
		SELECT
			IDControle, 
			ChaveUnica,
			RANK() OVER (PARTITION BY ChaveUnica ORDER BY IdControle DESC) rk
		FROM
			DW_CORPORATIVO.Ft_Estoque
		WHERE 1=1
-- 		AND DocFundo = '41.520.452/0001-81'
 		AND DtReferencia = "2025-01-08"

	) v2
	where rk > 1 
)

DELETE FROM DW_CORPORATIVO.Ft_Estoque WHERE IDControle in (SELECT IDControle from DUPLICADOS )


select * from DW_STAGING
WHERE NmFundo = 'FIDC BINVEST'



select * from DW_CORPORATIVO.Dm_CaixaLancamentos




WITH TEMP AS(
SELECT 
	IDControle,
	NmFundo ,
	DtReferencia,
	REPLACE( REPLACE( REPLACE( DocCedente, '.', ''),'-',''),'/','') DocCedente,
	NuDocumento	,
	Operacao
FROM DW_CORPORATIVO.Ft_Operacoes fo 
),

DUPLICADOS AS(
	SELECT 
		IDControle,
		NmFundo ,
		DtReferencia,
		DocCedente,
		NuDocumento,
		Operacao,
		RANK() OVER (PARTITION BY NmFundo, DtReferencia,  DocCedente, NuDocumento, Operacao ORDER BY IdControle DESC) rk
	FROM TEMP
	ORDER BY rk DESC 
	)

DELETE FROM DW_CORPORATIVO.Ft_Operacoes WHERE IDControle in (SELECT IDControle from DUPLICADOS WHERE rk > 1 )


SELECT * FROM DW_CORPORATIVO.Ft_Patrimonio
WHERE NmFundo LIKE '%APG%'
  


SELECT
	DISTINCT NmFundo, DocFundo
from
	DW_CORPORATIVO.Ft_Patrimonio fp
WHERE
	NmFundo LIKE '%PROSPER%'
	OR NmFundo LIKE '%VED%'
	
SELECT DISTINCT NmFundo FROM DW_CORPORATIVO.Ft_Patrimonio fp 



SELECT
	*
FROM
	DW_STAGING.Stg_LiquidadosBaixados slb
WHERE
	NmFundo = 'FIDC BEFIC'
	AND DATE_FORMAT(DtMovimento, '%Y-%m') = '2024-04'




SELECT DISTINCT Ocorrencia, TpRecebivel 
FROM DW_STAGING.Stg_LiquidadosBaixados
WHERE DtMovimento BETWEEN '2024-12-01' AND '2024-12-29'
AND NmFundo = 'FIDC LIBRA';


SELECT
	*
FROM
	DW_STAGING.Stg_LiquidadosBaixados slb
WHERE
	NmFundo = 'FIDC LIBRA'
	AND DATE_FORMAT(DtMovimento, '%Y-%m') = '2024-12'


ALTER TABLE DW_STAGING.Ft_CarteiraDiaria
ADD IDControle INT AUTO_INCREMENT PRIMARY KEY,
ADD Origem VARCHAR(30),
ADD DataETL TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

select * from DW_STAGING.Ft_CarteiraDiaria
WHERE Tp_Fundo = 'FICFIM'

	
	
UPDATE DW_CORPORATIVO.Ft_Patrimonio 
SET NmFundo = CASE 
    WHEN NmFundo = 'ABX9 FIM CP' THEN 'FIM ABX9'
    WHEN NmFundo = 'AF FC FIM CP RESP LT' THEN 'FICFIM AF'
    WHEN NmFundo = 'APG FIDC - MEZANINO 1' THEN 'FIDC APG MZ1'
    WHEN NmFundo = 'APG FIDC - MEZANINO 2' THEN 'FIDC APG MZ2'
    WHEN NmFundo = 'APG FIDC - SENIOR 1' THEN 'FIDC APG SR1'
    WHEN NmFundo = 'APG FIDC - SENIOR 2' THEN 'FIDC APG SR2'
    WHEN NmFundo = 'AROEIRA FIC FIM CP' THEN 'FICFIM AROEIRA'
    WHEN NmFundo = 'ATICO FC FIM CP' THEN 'FICFIM ATICO'
    WHEN NmFundo = 'ATLANTICO FIC FIM' THEN 'FICFIM ATLANTICO'
    WHEN NmFundo = 'BAUNKER II FIM CP' THEN 'FIM BAUNKER II'
    WHEN NmFundo = 'CAMPOS GERAIS FC FIM' THEN 'FICFIM CAMPOS GERAIS'
    WHEN NmFundo = 'CARGO HOLD FIC FIM' THEN 'FICFIM CARGO HOLD'
    WHEN NmFundo = 'CWB CATALISE FC FIM' THEN 'FICFIM CWB CATALISE'
    WHEN NmFundo = 'DANIELI FIC FIM CP' THEN 'FICFIM DANIELI'
    WHEN NmFundo = 'DANIELI II FC FIM CP' THEN 'FICFIM DANIELI II'
    WHEN NmFundo = 'E11EVEN FIC FIM CP' THEN 'FICFIM E11EVEN'
    WHEN NmFundo = 'EARNIER MAPS FIM CP' THEN 'FIM EARNIER'
    WHEN NmFundo = 'F2 BANK FC FIM CP' THEN 'FICFIM F2 BANK'
    WHEN NmFundo = 'FBH FIC FIM CP' THEN 'FICFIM FBH'
    WHEN NmFundo = 'FBJ77 FC FIM CP' THEN 'FICFIM FBJ77'
    WHEN NmFundo = 'FFBANK FC FIM' THEN 'FICFIM FFBANK'
    WHEN NmFundo = 'FIDC 3G BANK NP' THEN 'FIDC 3G BANK'
    WHEN NmFundo = 'FIDC BETTA MEZ4' THEN 'FIDC BETTA MZ4'
    WHEN NmFundo = 'FIDC BETTA SR 2' THEN 'FIDC BETTA SR2'
    WHEN NmFundo = 'FIDC BETTA SUB' THEN 'FIDC BETTA'
    WHEN NmFundo = 'FIDC BR8 MEZ' THEN 'FIDC BR8 MZ'
    WHEN NmFundo = 'FIDC BR8 SEN' THEN 'FIDC BR8 SR'
    WHEN NmFundo = 'FIDC BR8 SUB' THEN 'FIDC BR8'
    WHEN NmFundo = 'FIDC CRDLOG 2SR' THEN 'FIDC CREDILOG 2 SR'
    WHEN NmFundo = 'FIDC FERPAR NP' THEN 'FIDC FERPAR'
    WHEN NmFundo = 'FIDC FINANZE SB' THEN 'FIDC FINANZE'
    WHEN NmFundo = 'FIDC FINTRUST M' THEN 'FIDC FINTRUST MZ'
    WHEN NmFundo = 'FIDC NINE' THEN 'FIDC NINE CAPITAL'
    WHEN NmFundo = 'FIDC PAYCARGO S' THEN 'FIDC PAY CARGO SR'
    WHEN NmFundo = 'FIDC TOPCRED MA' THEN 'FIDC TOPCRED MZ1'
    WHEN NmFundo = 'FIDC TOPCRED MB' THEN 'FIDC TOPCRED MZ2'
    WHEN NmFundo = 'FIDC TRACTOR MA' THEN 'FIDC TRACTOR MZ1'
    WHEN NmFundo = 'FIDC UNITY NP' THEN 'FIDC UNITY'
    WHEN NmFundo = 'FIGUEIREDO FIC FIM' THEN 'FICFIM FIGUEIREDO'
    WHEN NmFundo = 'GANESHA FIC FIM CP' THEN 'FICFIM GANESHA'
    WHEN NmFundo = 'GLOBAL FIC FIM CP' THEN 'FICFIM GLOBAL'
    WHEN NmFundo = 'GMOT FC FIM' THEN 'FICFIM GMOT'
    WHEN NmFundo = 'GO TOGETHER FIC FIM' THEN 'FICFIM GO TOGETHER'
    WHEN NmFundo = 'GPW INV FIC FIM CP' THEN 'FICFIM GPW'
    WHEN NmFundo = 'GRUPO CORES FIC FIM' THEN 'FICFIM GRUPO CORES'
    WHEN NmFundo = 'JB FIM CP' THEN 'FIM JB'
    WHEN NmFundo = 'LIBRA FC FIM CP' THEN 'FICFIM LIBRA'
    WHEN NmFundo = 'LION FC FIDC' THEN 'FICFIDC LION'
    WHEN NmFundo = 'MASTRENN FIC FIM CP' THEN 'FICFIM MASTRENN'
    WHEN NmFundo = 'NOVA GLOBAL FIC FIM' THEN 'FICFIM NOVA GLOBAL'
    WHEN NmFundo = 'OIKOS FC FIM CP' THEN 'FICFIM OIKOS'
    WHEN NmFundo = 'PAN FC FIM CP' THEN 'FICFIM PAN'
    WHEN NmFundo = 'PROSPER FIM CP' THEN 'FIM PROSPER'
    WHEN NmFundo = 'RHB INVEST FIC FIM' THEN 'FICFIM RHB'
    WHEN NmFundo = 'RINCAO FIC FIM' THEN 'FICFIM RINCAO'
    WHEN NmFundo = 'SCI SAO CR FC FIM CP' THEN 'FICFIM SCI'
    WHEN NmFundo = 'SECULO FIC FIM CP' THEN 'FICFIM SECULO'
    WHEN NmFundo = 'UKF FIC FIM' THEN 'FICFIM UKF'
    WHEN NmFundo = 'VEDELAGO FIC FIM' THEN 'FICFIM VEDELAGO'
    WHEN NmFundo = 'FIC ABIATAR' THEN 'FICFIM ABIATAR'
    WHEN NmFundo = 'FIC ARCTURUS' THEN 'FIM ARCTURUS'
    WHEN NmFundo = 'FIC ARTANIS' THEN 'FIM ARTANIS'
    WHEN NmFundo = 'FIC GOLIATH' THEN 'FIM GOLIATH'
    WHEN NmFundo = 'FICFIM PROSPER' THEN 'FIM PROSPER'
    WHEN NmFundo = 'FICFIM VEDALAGO' THEN 'FICFIM VEDELAGO'
    WHEN NmFundo = 'Z CORP FIC FIM' THEN 'FICFIM Z CORP'
    
    
    WHEN NmFundo = 'FIDC 3G BANK NP' THEN 'FIDC 3G BANK'
    WHEN NmFundo = 'FIDC FERPAR NP' THEN 'FIDC FERPAR'
    WHEN NmFundo = 'FIDC FINTRUST M' THEN 'FIDC FINTRUST'
    WHEN NmFundo = 'FIDC TOPCRED MA' THEN 'FIDC TOPCRED MZ1'
    WHEN NmFundo = 'FIDC TRACTOR MA' THEN 'FIDC TRACTOR MZ1'
    WHEN NmFundo = 'FIDC UNITY NP' THEN 'FIDC UNITY' 
    ELSE NmFundo
END;
 
SELECT
	 DISTINCT DtPosicao
from
	DW_STAGING.Ft_CarteiraDiaria fcd

select * from DW_STAGING.Ft_CarteiraDiaria

SELECT * FROM DW_STAGING.Ft_CarteiraDiaria
WHERE NmFundo IN (
    'FIDC 3G BANK NP',
    'FIDC 3RD',
    'FIDC BRAVOS',
    'FIDC CREDILOG 2',
    'FIDC DFC',
    'FIDC MASTRENN',
    'FIDC FERPAR NP',
    'FIDC NINE',
    'FIDC MF GROUP',
    'FIDC LOGFIN',
    'FIDC PEROLA',
    'FIDC MALBEC',
    'FIDC F2 BANK',
    'FIDC P2',
    'FIDC TOPCRED',
    'FIDC TRACTOR',
    'FIDC VALENTE',
    'FIDC FINTRUST',
    'FIDC BINVEST',
    'FIDC BETTA SUB',
    'FIDC GREENWOOD',
    'FIDC PAY CARGO',
    'FIDC AGROCETE',
    'FIDC VELSO - NP',
    'FIDC UKF',
    'FIDC CREDILOG',
    'FIDC IPE NP',
    'FIDC K FINANCE',
    'FIDC CREDIAL',
    'FIDC KERDOS',
    'FIDC VERGINIA',
    'FIDC VISHNU',
    'FIDC AF6 NP',
    'FIDC B ROCKET',
    'FIDC BELL',
    'FICFIM MF HOLDI',
    'FIDC FINANZE SB',
    'FIDC SDL',
    'FIDC MFD',
    'FIDC AGROFORTE',
    'FIDC BR8 SUB',
    'FIDC PRIMER',
    'FIDC E3 NP',
    'FIDC DISAM',
    'FIDC UNITY NP'
);




UPDATE DW_CORPORATIVO.Ft_Patrimonio 
SET NmFundo = CASE 
    WHEN NmFundo = 'FIDC USECORP CATÁLISE' THEN 'FIDC USECORP CATALISE'
    ELSE NmFundo
END;


SELECT
	fp.NmFundo,
	MinData.MinDtPosicao,
	fp.VlrCota,
	fp.DataETL
FROM
	DW_CORPORATIVO.Ft_Patrimonio fp
INNER JOIN
    (
	SELECT
		NmFundo,
		MIN(DtPosicao) AS MinDtPosicao
	FROM
		DW_CORPORATIVO.Ft_Patrimonio
	GROUP BY
		NmFundo) AS MinData
ON
	fp.NmFundo = MinData.NmFundo
	AND fp.DtPosicao = MinData.MinDtPosicao
ORDER BY
	MinData.MinDtPosicao DESC
	
	
	
	SELECT DISTINCT NmFundo FROM DW_CORPORATIVO.Ft_Patrimonio fp 

select distinct DtReferencia from DW_STAGING.Stg_Estoque
	

	
SELECT
	COUNT(DISTINCT DocFundo),
	DtPosicao, NmFundo 
from
	DW_CORPORATIVO.Ft_Patrimonio fp
group by
	DtPosicao
order by
	DtPosicao desc


WITH DUPLICADOS AS (
    SELECT 
        VlrNominal,
        COUNT(*) AS RegistroCount
    FROM 
        DW_STAGING.Stg_Estoque se

    GROUP BY 
        VlrNominal
    HAVING 
        COUNT(*) > 1
)
SELECT *
FROM DUPLICADOS;


select DISTINCT DtReferencia from DW_STAGING.Stg_Estoque

select * from DW_CORPORATIVO.Ft_Despesas

SELECT COUNT(*) AS TotalRegistros
FROM DW_STAGING.Stg_Estoque se;

select Count(*) from DW_CORPORATIVO.vw_Estoque
WHERE DocFundo = '29.109.316/0001-06'


TRUNCATE TABLE DW_STAGING.Ft_CarteiraDiaria

select * from DW_STAGING.Ft_CarteiraDiaria
WHERE NmFundo LIKE '%EARNIER%'







