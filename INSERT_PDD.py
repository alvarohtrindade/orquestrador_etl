import pandas as pd
import openpyxl
import mysql.connector
import os
from datetime import datetime

from dotenv import load_dotenv
from mysql.connector import Error

# Carregar variáveis de ambiente
load_dotenv()

host = os.getenv("HOST")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
database = os.getenv("DATABASE")

# Ler a planilha
planilha = pd.read_excel(open(r'F:\\18. Divulgação de Cotas\\Divulgação cotas.xlsm', 'rb'), sheet_name='PDD') 
planilha = planilha.drop(columns=['CONCATENAR', 'NOME'])

# Renomear colunas para o padrão do banco
planilha = planilha.rename(columns={
    'CNPJ': 'CNPJ',
    'NOME FUNDO': 'NmFundo',
    'DATA': 'DtPosicao',
    'PATRIMONIO LIQUIDO': 'Patrimonio',
    'PDD': 'VlrAtivo'
})

# Adicionar a coluna Tipo com valor fixo 'PDD/MTM'
planilha['Ativo'] = 'PDD/MTM'

# Tratar valores
def parse_valor(valor):
    if isinstance(valor, str):
        valor = valor.replace('R$', '').replace('.', '').replace(',', '.').strip()
        return float(valor) if valor else 0
    return float(valor)

planilha['VlrAtivo'] = planilha['VlrAtivo'].apply(parse_valor)

# Remover linhas com valores nulos
print(planilha.isnull().sum())
planilha.dropna(inplace=True)

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

        # Buscar as datas únicas já existentes na tabela
        check_query = "SELECT DISTINCT DtPosicao FROM DW_CORPORATIVO.Dm_OutrosAtivos"
        cursor.execute(check_query)
        datas_existentes = [row[0] for row in cursor.fetchall()]
        
        # Converter para o mesmo formato para comparação
        datas_existentes = [data.date() if isinstance(data, datetime) else data for data in datas_existentes]
        
        # Filtrar o DataFrame para incluir apenas registros com datas que não estão no banco
        datas_planilha = planilha['DtPosicao'].dt.date.unique()
        datas_novas = [data for data in datas_planilha if data not in datas_existentes]
        
        if not datas_novas:
            print("Não há novas datas para inserir. Todos os dados já estão atualizados!")
        else:
            print(f"Inserindo dados para as seguintes datas: {datas_novas}")
            
            # Filtrar apenas os registros com datas novas
            planilha_filtrada = planilha[planilha['DtPosicao'].dt.date.isin(datas_novas)]
            
            # Criar comando INSERT (agora incluindo a coluna Tipo)
            insert_query = """
            INSERT INTO DW_CORPORATIVO.Dm_OutrosAtivos 
            (CNPJ, NmFundo, DtPosicao, Patrimonio, VlrAtivo, Ativo, DataETL)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """

            # Inserir os dados no MySQL
            registros_inseridos = 0
            for _, row in planilha_filtrada.iterrows():
                valores = (
                    row['CNPJ'],
                    row['NmFundo'],
                    row['DtPosicao'],
                    row['Patrimonio'],
                    row['VlrAtivo'],
                    row['Ativo']
                )
                cursor.execute(insert_query, valores)
                registros_inseridos += 1

            conexao.commit()
            print(f"Dados inseridos com sucesso! {registros_inseridos} registros inseridos para {len(datas_novas)} nova(s) data(s).")

except Error as e:
    print(f"Erro ao conectar ao MySQL: {e}")

finally:
    if 'conexao' in locals() and conexao.is_connected():
        cursor.close()
        conexao.close()
        print("Conexão fechada.")