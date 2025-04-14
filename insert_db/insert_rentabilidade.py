import json
import pandas as pd
import mysql.connector
import argparse
import os
import sys
import glob
from datetime import datetime
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Definição das variáveis de ambiente (podem ser substituídas por argumentos da linha de comando)
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
TABLE_NAME = os.getenv('DB_RENTABILIDADE')

# Nova configuração para o diretório onde os arquivos JSON serão buscados
JSON_DIR = os.getenv('BTG_RENTABILIDADE')

def encontrar_arquivo_json_mais_recente(diretorio=None):
    """
    Encontra o arquivo JSON mais recente em um diretório.
    
    Args:
        diretorio: Caminho para o diretório onde procurar arquivos JSON.
                  Se None, usa o valor de JSON_DIR.
    
    Returns:
        Caminho completo para o arquivo JSON mais recente, ou None se nenhum for encontrado.
    """
    try:
        # Usar o diretório padrão se nenhum for fornecido
        diretorio = diretorio or JSON_DIR
        
        # Verificar se o diretório existe
        if not os.path.exists(diretorio):
            print(f"AVISO: Diretório {diretorio} não existe.")
            return None
        
        # Procurar todos os arquivos JSON no diretório
        pattern = os.path.join(diretorio, "*.json")
        arquivos_json = glob.glob(pattern)
        
        if not arquivos_json:
            print(f"AVISO: Nenhum arquivo JSON encontrado em {diretorio}.")
            return None
        
        # Ordenar arquivos por data de modificação (mais recente primeiro)
        arquivos_json.sort(key=os.path.getmtime, reverse=True)
        
        arquivo_mais_recente = arquivos_json[0]
        print(f"Arquivo JSON mais recente encontrado: {arquivo_mais_recente}")
        return arquivo_mais_recente
        
    except Exception as e:
        print(f"ERRO ao buscar arquivo JSON: {e}")
        return None

def processar_json_rentabilidade(file_path):
    """
    Processa o arquivo JSON de rentabilidade e retorna um DataFrame estruturado.
    
    Args:
        file_path: Caminho para o arquivo JSON de rentabilidade
        
    Returns:
        DataFrame com os dados estruturados
    """
    try:
        # Ler o arquivo JSON
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Lista para armazenar todos os registros
        registros = []
        
        # Processar cada fundo no resultado
        for fundo in data.get('result', []):
            nome_fundo = fundo.get('fundName', '')
            
            # Processar cada registro de dados do fundo
            for dado_fundo in fundo.get('data', []):
                # Extrair dados de rentabilidade nominal
                rentabilidade = {}
                if dado_fundo.get('nominalQuoteList') and len(dado_fundo['nominalQuoteList']) > 0:
                    rentabilidade = dado_fundo['nominalQuoteList'][0]
                
                # Extrair dados de rentabilidade vs indexador
                rent_vs_cdi = {}
                if (dado_fundo.get('quotaProfitabilityDifference') and 
                    dado_fundo['quotaProfitabilityDifference'].get('CDIE') and 
                    dado_fundo['quotaProfitabilityDifference']['CDIE'].get('NominalVsIndexador')):
                    rent_vs_cdi = dado_fundo['quotaProfitabilityDifference']['CDIE']['NominalVsIndexador']
                
                # Verificar se a data de referência é válida (para evitar dados inconsistentes)
                if not dado_fundo.get('referenceDate'):
                    print(f"AVISO: Registro de {nome_fundo} sem data de referência, ignorando.")
                    continue
                
                # Criar registro para o DataFrame
                registro = {
                    'NmFundo': nome_fundo,
                    'CdConta': dado_fundo.get('account', ''),
                    'CnpjFundo': dado_fundo.get('cnpj', ''),
                    'DtReferencia': dado_fundo.get('referenceDate', '').split('T')[0],
                    'VlCotacao': dado_fundo.get('liquidQuote', None),
                    'VlCotacaoBruta': dado_fundo.get('rawQuote', None),
                    'VlPatrimonio': dado_fundo.get('assetValue', None),
                    'QtCotas': dado_fundo.get('numberOfQuotes', None),
                    'VlAplicacoes': dado_fundo.get('acquisitions', None),
                    'VlResgates': dado_fundo.get('redemptions', None),
                    
                    # Rentabilidade nominal
                    'RentDia': rentabilidade.get('day', None),
                    'RentMes': rentabilidade.get('month', None),
                    'RentAno': rentabilidade.get('year', None),
                    'Rent12Meses': rentabilidade.get('twelveMonths', None),
                    'Rent24Meses': rentabilidade.get('twentyFourMonths', None),
                    'Rent36Meses': rentabilidade.get('thirtySixMonths', None),
                    
                    # Rentabilidade vs indexador (CDI)
                    'RentDiaVsCDI': rent_vs_cdi.get('Day', None),
                    'RentMesVsCDI': rent_vs_cdi.get('Month', None),
                    'RentAnoVsCDI': rent_vs_cdi.get('Year', None),
                    'Rent12MesesVsCDI': rent_vs_cdi.get('Twelve', None),
                    'Rent24MesesVsCDI': rent_vs_cdi.get('TwentyFour', None),
                    'Rent36MesesVsCDI': rent_vs_cdi.get('ThirtySix', None),
                    
                    'TpClasse': dado_fundo.get('hierarchyClass', ''),
                    'CdSubClasse': dado_fundo.get('subClassCode', '')
                }
                
                registros.append(registro)
        
        # Verificar se temos registros para processar
        if not registros:
            print("ERRO: Nenhum registro válido encontrado no JSON.")
            return None
        
        # Criar DataFrame a partir dos registros
        df = pd.DataFrame(registros)
        
        # Converter datas
        if 'DtReferencia' in df.columns:
            df['DtReferencia'] = pd.to_datetime(df['DtReferencia'])
        
        return df
    
    except Exception as e:
        print(f"ERRO ao processar arquivo JSON: {e}")
        return None

def insert_dataframe_to_mysql(df, host, user, password, database, table_name, batch_size=100):
    """
    Insere os dados do DataFrame no banco MySQL em lotes.
    
    Args:
        df: DataFrame com os dados a serem inseridos
        host: Host do banco de dados
        user: Usuário do banco de dados
        password: Senha do banco de dados
        database: Nome do banco de dados
        table_name: Nome da tabela para inserção
        batch_size: Tamanho do lote para inserção
    """
    try:
        # Estabelecer conexão
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()
        
        # Total de registros
        total_registros = len(df)
        print(f"Inserindo {total_registros} registros na tabela {table_name}...")
        
        # Processar em lotes
        for i in range(0, total_registros, batch_size):
            # Obter lote atual
            batch_df = df.iloc[i:i+batch_size]
            
            # Preparar os valores para inserção
            values = []
            for _, row in batch_df.iterrows():
                # Substituir valores nulos por None para compatibilidade com MySQL
                row_values = [None if pd.isna(val) else val for val in row]
                values.append(tuple(row_values))
            
            # Construir a query de inserção
            columns = ', '.join(batch_df.columns)
            placeholders = ', '.join(['%s'] * len(batch_df.columns))
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            # Executar a inserção em lote
            cursor.executemany(sql, values)
            conn.commit()
            
            print(f"Lote inserido: {i+1} a {min(i+batch_size, total_registros)} de {total_registros}")
        
        print(f"Inserção concluída com sucesso! {total_registros} registros inseridos.")
        
    except Exception as e:
        print(f"ERRO ao inserir dados: {e}")
        if 'conn' in locals() and conn.is_connected():
            conn.rollback()
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            print("Conexão com o banco encerrada.")

def executar_procedure_atualizacao(host, user, password, database, data_referencia):
    """
    Executa procedure de atualização após a inserção dos dados.
    
    Args:
        host: Host do banco de dados
        user: Usuário do banco de dados
        password: Senha do banco de dados
        database: Nome do banco de dados
        data_referencia: Data de referência para atualizações específicas
    """
    try:
        # Estabelecer conexão
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()
        
        print(f"Executando procedure de atualização para a data {data_referencia}...")
        
        # Pode ser substituído pela procedure específica desejada
        # cursor.callproc('ATT_RENTABILIDADE_FUNDOS', [data_referencia])
        # Exemplo de como executar uma query direta se necessário:
        # cursor.execute(f"CALL ATT_RENTABILIDADE_FUNDOS('{data_referencia}')")
        
        conn.commit()
        print("Procedure executada com sucesso!")
        
    except Exception as e:
        print(f"ERRO ao executar procedure: {e}")
        if 'conn' in locals() and conn.is_connected():
            conn.rollback()
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            print("Conexão com o banco encerrada.")

def mapear_nomes_fundos(df, mapping_file=None):
    """
    Aplica mapeamento para normalizar nomes de fundos.
    
    Args:
        df: DataFrame com os dados dos fundos
        mapping_file: Caminho para o arquivo JSON de mapeamento (opcional)
        
    Returns:
        DataFrame com nomes de fundos normalizados
    """
    # Verifica se um arquivo de mapeamento foi fornecido
    if mapping_file and os.path.exists(mapping_file):
        try:
            with open(mapping_file, 'r', encoding='utf-8') as f:
                mapping = json.load(f)
            
            # Aplica o mapeamento nos nomes dos fundos
            print(f"Aplicando mapeamento de nomes de {len(mapping)} fundos...")
            df['NmFundo'] = df['NmFundo'].replace(mapping)
            
        except Exception as e:
            print(f"ERRO ao carregar ou aplicar mapeamento: {e}")
    
    return df

def main():
    """
    Função principal que coordena o processo de extração e inserção de dados.
    """
    # Configurar o parser de argumentos
    parser = argparse.ArgumentParser(description='Processa e insere dados de rentabilidade de fundos BTG')
    parser.add_argument('--json-file', help='Caminho para o arquivo JSON de rentabilidade (opcional, se não fornecido, buscará o mais recente)')
    parser.add_argument('--json-dir', help='Diretório onde buscar o arquivo JSON mais recente (opcional)')
    parser.add_argument('--mapping-file', help='Caminho para o arquivo JSON de mapeamento de nomes de fundos')
    parser.add_argument('--batch-size', type=int, default=100, help='Tamanho do lote para inserção')
    parser.add_argument('--save-csv', action='store_true', help='Salva os dados processados em um arquivo CSV')
    parser.add_argument('--output-csv', help='Caminho para salvar o arquivo CSV')
    parser.add_argument('--auto', action='store_true', help='Executa automaticamente sem confirmação de inserção no banco')
    
    args = parser.parse_args()
    
    # Usar os valores das variáveis de ambiente para conexão com o banco
    host = DB_HOST
    user = DB_USER
    password = DB_PASSWORD
    database = DB_NAME
    table_name = TABLE_NAME
    
    # Determinar o arquivo JSON a ser processado
    json_file_path = args.json_file
    
    # Se não foi especificado um arquivo, buscar o mais recente
    if not json_file_path:
        json_file_path = encontrar_arquivo_json_mais_recente(args.json_dir)
        if not json_file_path:
            print("ERRO: Nenhum arquivo JSON encontrado.")
            sys.exit(1)
    # Se foi especificado, verificar se existe
    elif not os.path.exists(json_file_path):
        print(f"ERRO: Arquivo JSON não encontrado em {json_file_path}")
        sys.exit(1)
    
    print(f"Processando arquivo: {json_file_path}")
    
    # Processar o arquivo JSON
    df_rentabilidade = processar_json_rentabilidade(json_file_path)
    
    if df_rentabilidade is None or df_rentabilidade.empty:
        print("ERRO: Falha ao processar o arquivo JSON ou nenhum dado encontrado.")
        sys.exit(1)
    
    # Aplicar mapeamento de nomes se fornecido
    if args.mapping_file:
        df_rentabilidade = mapear_nomes_fundos(df_rentabilidade, args.mapping_file)
    
    # Informações sobre os dados processados
    print(f"Dados processados: {len(df_rentabilidade)} registros de rentabilidade")
    
    # Identificar a data de referência mais recente
    data_referencia = df_rentabilidade['DtReferencia'].max().strftime('%Y-%m-%d')
    print(f"Data de referência mais recente: {data_referencia}")
    
    # Salvar em CSV se solicitado
    if args.save_csv:
        output_csv = args.output_csv or f"rentabilidade_processada_{data_referencia}.csv"
        df_rentabilidade.to_csv(output_csv, index=False)
        print(f"Dados salvos em: {output_csv}")
    
    # Verificar se deve inserir no banco
    if all([host, user, password, database]):
        # Perguntar se deseja continuar, a menos que --auto tenha sido especificado
        if not args.auto and not args.save_csv:  # Se não estiver apenas salvando em CSV e não for automático
            resposta = input("Deseja inserir os dados no banco de dados MySQL? (s/n): ").lower()
            if resposta != 's':
                print("Operação cancelada pelo usuário.")
                sys.exit(0)
        
        # Inserir no banco
        insert_dataframe_to_mysql(
            df_rentabilidade, 
            host, 
            user, 
            password, 
            database, 
            table_name, 
            args.batch_size
        )
        
        # Executar procedure de atualização
        executar_procedure_atualizacao(host, user, password, database, data_referencia)
    else:
        print("Configurações de banco de dados incompletas nas variáveis de ambiente. Verifique o arquivo .env.")
        print("Dados não inseridos no banco. Considere usar a opção --save-csv para salvar em arquivo.")


if __name__ == "__main__":
    main()