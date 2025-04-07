import pandas as pd
import openpyxl
import mysql.connector
import os
import numpy as np
from dotenv import load_dotenv
from mysql.connector import Error

# Carregar variáveis de ambiente
load_dotenv()

host = os.getenv("HOST")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
database = os.getenv("DATABASE")


# Dicionário de nomes padronizados específicos
nome_padronizado = {
    'FIM ABX9 CP': 'FIM ABX9',
    'FIC FIM AF': 'FICFIM AF',
    'FIC FIM ARABAN CP': 'FICFIM ARABAN',
    'FIC FIM CP AROEIRA': 'FICFIM AROEIRA',
    'FIC FIM ÁTICO CP': 'FICFIM ATICO',
    'FIC FIM ATLÂNTICO': 'FICFIM ATLANTICO',
    'FIM BAUNKER II CP': 'FIM BAUNKER II',
    'FIC FIM CAMPOS GERAIS': 'FICFIM CAMPOS GERAIS',
    'FIC FIM CARGO HOLD': 'FICFIM CARGO HOLD',
    'FIC FIM CWB CATALISE': 'FICFIM CWB CATALISE',
    'FIC FIM DANIELI II CP': 'FICFIM DANIELI II',
    'FIC FIM E11EVEN': 'FICFIM E11EVEN',
    'FIM CP EARNIER MAPS': 'FIM EARNIER MAPS',
    'FIC FIM F2 BANK CP': 'FICFIM F2 BANK',
    'FIC FIM FBH': 'FICFIM FBH',
    'FIC FIM FBJ77 CP': 'FICFIM FBJ77',
    'FIC FIM FFBANK CP': 'FICFIM FFBANK',
    'FIC FIM FIGUEIREDO': 'FICFIM FIGUEIREDO',
    'FIC FIM GANESHA CP': 'FICFIM GANESHA',
    'FIC FIM GLOBAL CP': 'FICFIM GLOBAL',
    'FIC FIM GMOT': 'FICFIM GMOT',
    'FIC FIM GO TOGETHER': 'FICFIM GO TOGETHER',
    'FIC FIM IRIS': 'FICFIM IRIS',
    'FIC FIM GPW INV CP': 'FICFIM GPW INV',
    'FIC FIM GRUPO CORES': 'FICFIM GRUPO CORES',
    'FIM JB CP': 'FIM JB',
    'FIC FIM LIBRA CP': 'FICFIM LIBRA',
    'FIC FIDC LION': 'FICFIDC LION',
    'FIC FIM MASTRENN': 'FICFIM MASTRENN',
    'FIC FIM NOVA GLOBAL': 'FICFIM NOVA GLOBAL',
    'FIC FIM OIKOS CP': 'FICFIM OIKOS',
    'FIC FIM PAN CP': 'FICFIM PAN',
    'FIM PROSPER CP': 'FIM PROSPER',
    'FIC FIDC CATALISE': 'FICFIDC CATALISE',
    'FIC MACANAN': 'FIC MACANAN',
    'FIDC 3G BANK NP': 'FIDC 3G BANK',
    'FIDC 3RD': 'FIDC 3RD',
    'FIDC AF6': 'FIDC AF6',
    'FIC FIM ABIATAR': 'FICFIM ABIATAR',
    'FIC FIM BELLAGIO': 'FICFIM BELLAGIO',
    'FIDC AGIS NP': 'FIDC AGIS',
    'FIC FIM GRUPO PRIME AGRO CP': 'FICFIM GRUPO PRIME AGRO',
    'FIDC AGROCETE': 'FIDC AGROCETE',
    'FIDC AGROFORTE SUB': 'FIDC AGROFORTE',
    'FIDC ALBAREDO': 'FIDC ALBAREDO',
    'FIC FIM BELLIN': 'FICFIM BELLIN',
    'FIDC ANIL': 'FIDC ANIL',
    'FIDC ANVERES': 'FIDC ANVERES',
    'FIDC APG': 'FIDC APG',
    'FIDC ARIS CAPITAL': 'FIDC ARIS CAPITAL',
    'FIDC BASÃ': 'FIDC BASA',
    'FIDC BEFIC': 'FIDC BEFIC',
    'FIDC BELL': 'FIDC BELL',
    'FIM ARCTURUS CP IE': 'FIM ARCTURUS IE',
    'FIM ARTANIS CP IE': 'FIM ARTANIS IE',
    'FIM GOLIATH CP IE': 'FIM GOLIATH IE',
    'FIDC BETTA': 'FIDC BETTA',
    'FIDC BINVEST SUB': 'FIDC BINVEST',
    'FIDC BLUE ROCKET SUB': 'FIDC BLUE ROCKET',
    'FIDC BOAZ': 'FIDC BOAZ',
    'FIDC BONTEMPO': 'FIDC BONTEMPO',
    'FIC FIM AVANT FINTECH': 'FICFIM AVANT FINTECH',
    'FIDC BR8': 'FIDC BR8',
    'FIDC BR8 SEN': 'FIDC BR8 SR',
    'FIDC BR8 MEZ': 'FIDC BR8 MEZ',
    'FIDC BRAVOS': 'FIDC BRAVOS',
    'FIDC  K-FINANCE NP SR': 'FIDC K-FINANCE SR',
    'FIC FIM KINVEST CP': 'FICFIM KINVEST',
    'FIDC CAPITALIZA': 'FIDC CAPITALIZA',
    'FIDC CONDOBEM': 'FIDC CONDOBEM',
    'FIDC CONDOBEM SR': 'FIDC CONDOBEM SR',
    'FIDC CREDIAL': 'FIDC CREDIAL',
    'FIDC CREDILOG': 'FIDC CREDILOG',
    'FIC FIM BINVEST CP': 'FICFIM BINVEST',
    'FIDC CREDILOG II': 'FIDC CREDILOG II',
    'FIDC DBANK': 'FIDC DBANK',
    'FIDC DFC': 'FIDC DFC',
    'FIDC F2 BANK SUB': 'FIDC F2 BANK',
    'FIDC FERPAR NP': 'FIDC FERPAR',
    'FIDC FINTRUST': 'FIDC FINTRUST',
    'FIDC FUTURO CAPITAL': 'FIDC FUTURO CAPITAL',
    'FIDC GLOBAL FUTURA': 'FIDC GLOBAL FUTURA',
    'FIDC GREENWOOD': 'FIDC GREENWOOD',
    'FIDC IMOVEIS APOLAR - A DEFINIR': 'FIDC IMOVEIS APOLAR',
    'FIC FIM NINE CAPITAL CP': 'FICFIM NINE CAPITAL',
    'FIDC IPE NP': 'FIDC IPE',
    'FIDC K FINANCE SUB': 'FIDC K FINANCE',
    'FIDC KERDOS': 'FIDC KERDOS',
    'FIDC LEX CAPITAL': 'FIDC LEX CAPITAL',
    'FIC FIM MF HOLDI': 'FICFIM MF HOLDI',
    'FIDC LIBRA CONSIGNADO': 'FIDC LIBRA CONSIGNADO',
    'FIDC LOGFIN': 'FIDC LOGFIN',
    'FIP FERGUS MULT': 'FIP FERGUS',
    'FIDC MALBEC': 'FIDC MALBEC',
    'FIDC MASTRENN': 'FIDC MASTRENN',
    'FIC FIM AXXIA': 'FICFIM AXXIA',
    'FIDC MF GROUP': 'FIDC MF GROUP',
    'FIDC NB': 'FIDC NB',
    'FIC RHB': 'FIC RHB',
    'FIDC NIMOFAST': 'FIDC NIMOFAST',
    'FIDC NINE': 'FIDC NINE CAPITAL',
    'FIC FIM RINCAO': 'FICFIM RINCAO',
    'FIC FIM SCI': 'FICFIM SCI',
    'FIC FIM B5 CP': 'FICFIM B5',
    'FIC FIM SECULO CP': 'FICFIM SECULO',
    'FIDC BLUE ROCKET SR': 'FIDC BLUE ROCKET SR',
    'FIDC AGROFORTE MZ1': 'FIDC AGROFORTE MZ1',
    'FIDC AGROFORTE MZ2': 'FIDC AGROFORTE MZ2',
    'FIDC AGROFORTE SR 1': 'FIDC AGROFORTE SR1',
    'FIDC AGROFORTE SR 2': 'FIDC AGROFORTE SR2',
    'FIDC APG MZ1': 'FIDC APG MZ1',
    'FIDC APG MZ2': 'FIDC APG MZ2',
    'FIDC APG SR1': 'FIDC APG SR1',
    'FIDC BETTA SR2': 'FIDC BETTA SR2',
    'FIDC BETTA MZ4': 'FIDC BETTA MZ4',
    'FIDC BINVEST MZ': 'FIDC BINVEST MZ',
    'FIDC BINVEST SR': 'FIDC BINVEST SR',
    'FIDC CREDILOG 2 SR': 'FIDC CREDILOG 2 SR',
    'FIDC F2 BANK MZ': 'FIDC F2 BANK MZ',
    'FIDC PAY CARGO SUB': 'FIDC PAY CARGO',
    'FIDC PAY CARGO SR': 'FIDC PAY CARGO SR',
    'FIDC TOPCRED SUB': 'FIDC TOPCRED',
    'FIDC TOPCRED MZ A': 'FIDC TOPCRED MZA',
    'FIDC TOPCRED MZ B': 'FIDC TOPCRED MZB',
    'FIDC TOPCRED SR': 'FIDC TOPCRED SR',
    'FIDC TRACTOR SUB': 'FIDC TRACTOR',
    'FIDC TRACTOR MZ A': 'FIDC TRACTOR MZA',
    'FIDC TRACTOR SR': 'FIDC TRACTOR SR',
    'FIDC NR11': 'FIDC NR11',
    'FIDC P2': 'FIDC P2 INVEST',
    'FIC FIM ADCBANK': 'FICFIM ADCBANK',
    'FIDC PEROLA': 'FIDC PEROLA',
    'FIC FIM CAP4': 'FICFIM CAP4',
    'FIC FIM CAP6': 'FICFIM CAP6',
    'FIDC PINPAG': 'FIDC PINPAG',
    'FIDC PINPAG SR': 'FIDC PINPAG SR',
    'FIC FIM PETKOW CAP': 'FICFIM PETKOW CAP',
    'FIDC PRIME AGRO': 'FIDC PRIME AGRO',
    'FIDC PRIME AGRO SR': 'FIDC PRIME AGRO SR',
    'FIDC PRIMER': 'FIDC PRIMER',
    'FIDC PRIMER MZ': 'FIDC PRIMER MZ',
    'FIDC PRIMER SR': 'FIDC PRIMER SR',
    'FIDC PROMETHEUS': 'FIDC PROMETHEUS',
    'FIC FIM NB': 'FICFIM NB',
    'FIDC RECEBA SEGURO': 'FIDC RECEBA SEGURO',
    'FIDC RHB': 'FIDC RHB',
    'FIDC B5': 'FIDC B5',
    'FIDC SC': 'FIDC SC',
    'FIDC SDL': 'FIDC SDL',
    'FIC FIM MAUI​': 'FICFIM MAUI',
    'FIDC SMT AGRO': 'FIDC SMT AGRO',
    'FIDC FINTRUST MZ': 'FIDC FINTRUST MZ',
    'FIC FIM STAND BY': 'FICFIM STAND BY',
    'FIDC TERTON': 'FIDC TERTON',
    'FIC FIM UKF': 'FICFIM UKF',
    'FIDC TORONTO': 'FIDC TORONTO',
    'FIDC TORONTO SR': 'FIDC TORONTO SR',
    'FIDC TORONTO MEZ': 'FIDC TORONTO MEZ',
    'FIC FIM LC AZUL': 'FICFIM LC AZUL',
    'FIDC UKF': 'FIDC UKF',
    'FIC FIM ANTARES': 'FICFIM ANTARES',
    'FIDC UNIMED - A DEFINIR': 'FIDC UNIMED',
    'FIDC UNITY NP': 'FIDC UNITY',
    'FIC FIM VEDELAGO': 'FICFIM VEDELAGO',
    'FIC FIM SMT AGRO HOLDING': 'FICFIM SMT AGRO',
    'FIDC USECORP CATÁLISE': 'FIDC USECORP CATALISE',
    'FIC FIM IMOVEIS APOLAR - A DEFINIR': 'FICFIM IMOVEIS APOLAR',
    'FIDC USECORP CATÁLISE SR': 'FIDC USECORP CATALISE SR',
    'FIDC USECORP CATÁLISE MZ': 'FIDC USECORP CATALISE MZ',
    'FIC FIM NIMOFAST': 'FICFIM NIMOFAST',
    'FIDC VALENTE': 'FIDC VALENTE',
    'FIC JAQUIM': 'FIC JAQUIM',
    'FIDC VELSO NP': 'FIDC VELSO',
    'FIDC VERGINIA': 'FIDC VERGINIA',
    'FIC FIM Z CORP': 'FICFIM Z CORP',
    'FIDC SYRAH': 'FIDC SYRAH',
    'FIDC DBANK SR': 'FIDC DBANK SR',
    'FIDC LIBRA CONSIGNADO SR': 'FIDC LIBRA CONSIGNADO SR',
    'FIDC DFC SR': 'FIDC DFC SR',
    'FIDC DFC MZ': 'FIDC DFC MZ',
    'FIDC APG SR2': 'FIDC APG SR2',
    'FIDC VISHNU': 'FIDC VISHNU',
    'FIDC VITTRA': 'FIDC VITTRA',
    'FIDC Z INVEST': 'FIDC Z INVEST',
    'FIP SHIFT AGRO': 'FIP SHIFT AGRO',
    'FIA IE DAYTONA': 'FIA IE DAYTONA',
    'FIC FIM KINVEST CP': 'FICFIM KINVEST',
    'MJC FIC FIM CP': 'MJC FICFIM',
    'FIC FIM 3B CP': 'FICFIM 3B',
    'FIDC NP BGR': 'FIDC BGR',
    'FIC FIM ELS CP': 'FICFIM ELS',
    'FIC FIM ITC CP': 'FICFIM ITC',
    'FIDC FINANZE SB': 'FIDC FINANZE SB',
    'FIDC ITC': 'FIDC ITC',
    'FIC FIM ÔMEGA CP': 'FICFIM OMEGA',
    'FIDC NP PROFIT MAPS': 'FIDC PROFIT MAPS',
    'FIC FIM PROSPERARE CP': 'FICFIM PROSPERARE',
    'FIDC K FINANCE SUB': 'FIDC K FINANCE',
    'FIDC STAL': 'FIDC STAL',
    'FIM CR FALCON': 'FIM CR FALCON',
    'FIDC MFD': 'FIDC MFD',
    'FIDC TRINU': 'FIDC TRINU',
    'FIC FIM ZW INVEST CP': 'FICFIM ZW INVEST',
    'FIC FIM DANIELI CP': 'FICFIM DANIELI',
    'FIM E3 IE CP': 'FIM E3 IE',
    'FIDC  K-FINANCE NP SR': 'FIDC K-FINANCE SR',
    'FIDC YELLOW': 'FIDC YELLOW',
    'FIDC YELLOW SR': 'FIDC YELLOW SR',
    'FIDC EJM': 'FIDC EJM',
    'FIDC BRISTOL MULT SUB': 'FIDC BRISTOL',
    'FIDC BRISTOL MULT MEZ CLA': 'FIDC BRISTOL CLA MZ1',
    'FIDC BRISTOL MULT MEZ CLC': 'FIDC BRISTOL CLC MZ2',
    'FIDC BRISTOL MULT MEZ HY': 'FIDC BRISTOL HY MZ4',
    'FIDC BRISTOL MULT SR 5': 'FIDC BRISTOL SR5',
    'FIDC FIRENZI CAPITAL': 'FIDC FIRENZI CAPITAL',
    'FIDC SAGEL BANK': 'FIDC SAGEL BANK',
    'FIC FIM MENEGOTTI': 'FICFIM MENEGOTTI',
    'FIDC ZAB LEGACY': 'FIDC ZAB LEGACY',
    'FIDC ÁGUAS DO FUTURO': 'FIDC AGUAS DO FUTURO',
    'FIA ASUL': 'FIA ASUL',
    'FIDC HB - BREITKOPF': 'FIDC HB BREITKOPF',
    'FIDC LINK CAPITA': 'FIDC LINK CAPITA',
    'FIC MTZ CAPITAL': 'FIC MTZ CAPITAL',
    'FIDC ÁPICE CREDIT': 'FIDC APICE CREDIT',
    'FIC FIM GRUPO LOPES': 'FICFIM GRUPO LOPES',
    'FIC SOARES CAPITAL': 'FIC SOARES CAPITAL',
    'FIDC LUPUS': 'FIDC LUPUS',
    'FIC HB': 'FIC HB',
    'FIDC PRUNA': 'FIDC PRUNA',
    'FIC FIM ANIMA': 'FICFIM ANIMA',
    'FIC FIM SAGEL ASSESSORIA': 'FICFIM SAGEL',
    'Fundos Novos': 'Fundos Novos'
}

# Carregar a planilha Excel
planilha = pd.read_excel(r'F:\CADFUN.xlsx', sheet_name='Base', header=3)

# Selecionar colunas desejadas
colunas_desejadas = [
    "CNPJ", "NOME FUNDO", "TIPO FUNDO", "ADMINISTRADOR ATUAL", "CUSTODIANTE", "GESTOR",
    "DATA INÍCIO", "DATA DE CONSTITUIÇÃO", "DATA REGISTRO CVM", "CÓDIGO ISIN", "CÓDIGO GIIN",
    "STATUS", "GRUPO ECONÔMICO", "TIPO ESTRUTURA", "CÓDIGO CVM", "CLASSE"
]
planilha = planilha[colunas_desejadas]

# Renomear colunas
planilha = planilha.rename(columns={
    'CNPJ': 'DocFundo',
    'NOME FUNDO': 'NmFundo',
    'TIPO FUNDO': 'Tipo',
    'ADMINISTRADOR ATUAL': 'Administrador',
    'CUSTODIANTE': 'Custodiante',
    'GESTOR': 'Gestor',
    'DATA INÍCIO': 'DtInicio',
    'DATA DE CONSTITUIÇÃO': 'DtConstituicao',
    'DATA REGISTRO CVM': 'DtRegistroCVM',
    'CÓDIGO ISIN': 'ISIN',
    'CÓDIGO GIIN': 'GIIN',
    'STATUS': 'Status',
    'GRUPO ECONÔMICO': 'GrupoEconomico',
    'TIPO ESTRUTURA': 'Estrutura',
    'CÓDIGO CVM': 'CVM',
    'CLASSE': 'Classe'
})

# Aplicar apenas as substituições específicas do dicionário
for nome_original, nome_novo in nome_padronizado.items():
    planilha.loc[planilha['NmFundo'] == nome_original, 'NmFundo'] = nome_novo


# Filtrar fundos ativos
planilha = planilha[planilha['Status'] != 'Encerrado']

# Conectar ao banco de dados
try:
    conexao = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    
    if conexao.is_connected():
        print("Conectado ao MySQL!")
        cursor = conexao.cursor()

        # Contar fundos existentes no banco
        cursor.execute("SELECT COUNT(*) FROM DW_DESENV.CADFUN")
        fundos_no_banco = cursor.fetchone()[0]
        fundos_na_planilha = len(planilha)
        
        print(f"Fundos no banco: {fundos_no_banco}")
        print(f"Fundos na planilha: {fundos_na_planilha}")
        
        if fundos_na_planilha > fundos_no_banco:
            print("Novos fundos identificados! Inserindo no banco...")
            
            insert_query = """
            INSERT INTO DW_DESENV.CADFUN 
            (DocFundo, NmFundo, Tipo, Administrador, Custodiante, Gestor, 
            DtInicio, DtConstituicao, DtRegistroCVM, ISIN, GIIN, Status, 
            GrupoEconomico, Estrutura, CVM, Classe, DataETL)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """

            for _, row in planilha.iterrows():
                valores = tuple(None if pd.isna(v) else v for v in [
                    row['DocFundo'], row['NmFundo'], row['Tipo'], row['Administrador'], row['Custodiante'],
                    row['Gestor'], row['DtInicio'], row['DtConstituicao'], row['DtRegistroCVM'],
                    row['ISIN'], row['GIIN'], row['Status'], row['GrupoEconomico'], row['Estrutura'],
                    row['CVM'], row['Classe']
                ])
                cursor.execute(insert_query, valores)

            conexao.commit()

            # Chamada da procedure para atualizar os nomes dos fundos
            cursor.callproc("DW_DESENV.Nomes_CADFUN")
            print("Dados inseridos com sucesso!")
        else:
            print("Nenhum novo fundo encontrado. Nenhuma inserção realizada.")

except Error as e:
    print(f"Erro ao conectar ao MySQL: {e}")

finally:
    if conexao.is_connected():
        cursor.close()
        conexao.close()
        print("Conexão fechada.")
