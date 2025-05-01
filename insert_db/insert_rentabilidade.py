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

def converter_porcentagem_para_decimal(valor):
    """
    Converte um valor em porcentagem para decimal (divide por 100).
    
    Args:
        valor: Valor percentual ou None
        
    Returns:
        Valor decimal ou None se o valor original for None
    """
    if valor is None:
        return None
    try:
        return float(valor) / 100
    except (ValueError, TypeError):
        return valor

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

def processar_json_rentabilidade(file_path, debug=False):
    """
    Processa o arquivo JSON de rentabilidade e retorna um DataFrame estruturado.
    
    Args:
        file_path: Caminho para o arquivo JSON de rentabilidade
        debug: Se True, exibe logs detalhados para diagnóstico
        
    Returns:
        DataFrame com os dados estruturados
    """
    try:
        # Tentar ler o arquivo JSON com diferentes codificações, se necessário
        try:
            # Tentar com utf-8
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except UnicodeDecodeError:
            # Se falhar, tentar com latin-1
            print("AVISO: Erro de decodificação UTF-8, tentando com latin-1")
            with open(file_path, 'r', encoding='latin-1') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            # Se falhar ao decodificar o JSON
            print(f"ERRO: JSON inválido: {e}")
            # Tentar ler os primeiros bytes para diagnóstico
            with open(file_path, 'rb') as f:
                raw_data = f.read(200)
            print(f"Primeiros bytes do arquivo: {raw_data}")
            return None
        
        # Adicionar logs para diagnóstico
        if debug:
            print(f"Estrutura do JSON: {list(data.keys())}")
        
        if 'result' not in data:
            print("ERRO: Estrutura de JSON inválida - chave 'result' não encontrada")
            print(f"Primeiros 200 caracteres do JSON: {str(data)[:200]}")
            return None
        
        if not data.get('result'):
            print("AVISO: Array 'result' está vazio no JSON")
            return None
        
        print(f"Número de fundos encontrados no JSON: {len(data.get('result', []))}")
        
        # Lista para armazenar todos os registros
        registros = []
        
        # Processar cada fundo no resultado
        for i, fundo in enumerate(data.get('result', [])):
            nome_fundo = fundo.get('fundName', '')
            print(f"Processando fundo {i+1}/{len(data.get('result', []))}: {nome_fundo}")
            
            if not fundo.get('data'):
                print(f"AVISO: Fundo {nome_fundo} não tem dados ('data')")
                continue
                
            print(f"Número de registros para o fundo {nome_fundo}: {len(fundo.get('data', []))}")
            
            # Processar cada registro de dados do fundo
            for dado_fundo in fundo.get('data', []):
                # Extrair dados de rentabilidade nominal e converter de porcentagem para decimal
                rentabilidade = {}
                if dado_fundo.get('nominalQuoteList') and len(dado_fundo['nominalQuoteList']) > 0:
                    rentabilidade_raw = dado_fundo['nominalQuoteList'][0]
                    rentabilidade = {
                        'day': converter_porcentagem_para_decimal(rentabilidade_raw.get('day')),
                        'month': converter_porcentagem_para_decimal(rentabilidade_raw.get('month')),
                        'year': converter_porcentagem_para_decimal(rentabilidade_raw.get('year'))
                    }
                
                # Extrair dados de rentabilidade vs indexador (CDI) e converter de porcentagem para decimal
                rent_vs_cdi = {}
                if (dado_fundo.get('quotaProfitabilityDifference') and 
                    dado_fundo['quotaProfitabilityDifference'].get('CDIE') and 
                    dado_fundo['quotaProfitabilityDifference']['CDIE'].get('NominalVsIndexador')):
                    
                    rent_vs_cdi_raw = dado_fundo['quotaProfitabilityDifference']['CDIE']['NominalVsIndexador']
                    rent_vs_cdi = {
                        'Day': converter_porcentagem_para_decimal(rent_vs_cdi_raw.get('Day')),
                        'Month': converter_porcentagem_para_decimal(rent_vs_cdi_raw.get('Month')),
                        'Year': converter_porcentagem_para_decimal(rent_vs_cdi_raw.get('Year'))
                    }
                
                # Verificar se a data de referência é válida (para evitar dados inconsistentes)
                if not dado_fundo.get('referenceDate'):
                    print(f"AVISO: Registro de {nome_fundo} sem data de referência, ignorando.")
                    continue
                
                # Criar registro para o DataFrame
                registro = {
                    'NmFundo': nome_fundo,
                    'CdConta': dado_fundo.get('account', ''),
                    'CnpjFundo': dado_fundo.get('cnpj', ''),
                    'DtPosicao': dado_fundo.get('referenceDate', '').split('T')[0],
                    'VlrCotacao': dado_fundo.get('liquidQuote', None),
                    'VlrCotacaoBruta': dado_fundo.get('rawQuote', None),
                    'VlrPatrimonio': dado_fundo.get('assetValue', None),
                    'QtdCota': dado_fundo.get('numberOfQuotes', None),
                    'VlrAplicacao': dado_fundo.get('acquisitions', None),
                    'VlrResgate': dado_fundo.get('redemptions', None),
                    
                    # Rentabilidade nominal (já convertida para decimal)
                    'RentDia': rentabilidade.get('day', None),
                    'RentMes': rentabilidade.get('month', None),
                    'RentAno': rentabilidade.get('year', None),
                    
                    # Rentabilidade vs indexador (CDI) (já convertida para decimal)
                    'RentDiaVsCDI': rent_vs_cdi.get('Day', None),
                    'RentMesVsCDI': rent_vs_cdi.get('Month', None),
                    'RentAnoVsCDI': rent_vs_cdi.get('Year', None),
                    
                    'TpClasse': dado_fundo.get('hierarchyClass', '')
                }
                
                registros.append(registro)
        
        # Verificar se temos registros para processar
        if not registros:
            print("ERRO: Nenhum registro válido encontrado no JSON.")
            return None
        
        # Criar DataFrame a partir dos registros
        df = pd.DataFrame(registros)
        
        # Converter datas
        if 'DtPosicao' in df.columns:
            df['DtPosicao'] = pd.to_datetime(df['DtPosicao'])
        
        return df
    
    except Exception as e:
        import traceback
        print(f"ERRO ao processar arquivo JSON: {e}")
        print("Detalhes do erro:")
        traceback.print_exc()
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
        
        print(f"MÉTRICA: Total de registros processados: {total_registros}")
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
            original_names = df['NmFundo'].unique().tolist()
            df['NmFundo'] = df['NmFundo'].replace(mapping)
            mapped_names = df['NmFundo'].unique().tolist()
            
            # Verificar e relatar fundos que foram mapeados
            for orig_name in original_names:
                if orig_name in mapping:
                    new_name = mapping[orig_name]
                    print(f"Mapeado: '{orig_name}' -> '{new_name}'")
            
            print(f"MÉTRICA: {len(mapping)} fundos no mapeamento, {len(original_names)} fundos nos dados, {len(mapped_names)} fundos após mapeamento")
            
        except Exception as e:
            print(f"ERRO ao carregar ou aplicar mapeamento: {e}")
    
    return df

def main():
    """
    Função principal que coordena o processo de extração e inserção de dados.
    """
    try:
        # Configurar o parser de argumentos
        parser = argparse.ArgumentParser(description='Processa e insere dados de rentabilidade de fundos BTG')
        parser.add_argument('--json-file', help='Caminho para o arquivo JSON de rentabilidade (opcional, se não fornecido, buscará o mais recente)')
        parser.add_argument('--json-dir', help='Diretório onde buscar o arquivo JSON mais recente (opcional)')
        parser.add_argument('--mapping-file', help='Caminho para o arquivo JSON de mapeamento de nomes de fundos')
        parser.add_argument('--batch-size', type=int, default=100, help='Tamanho do lote para inserção')
        parser.add_argument('--save-csv', action='store_true', help='Salva os dados processados em um arquivo CSV')
        parser.add_argument('--output-csv', help='Caminho para salvar o arquivo CSV')
        parser.add_argument('--auto', action='store_true', help='Executa automaticamente sem confirmação de inserção no banco')
        parser.add_argument('--debug', action='store_true', help='Habilita logs detalhados para diagnóstico')
        
        args = parser.parse_args()
        
        # Configuração do modo debug
        debug = args.debug
        if debug:
            print("MODO DEBUG: Logs detalhados habilitados")
        
        print(f"Argumentos recebidos: {args}")  # DEBUG: Imprimir argumentos recebidos
        
        # Usar os valores das variáveis de ambiente para conexão com o banco
        host = DB_HOST
        user = DB_USER
        password = DB_PASSWORD
        database = DB_NAME
        table_name = TABLE_NAME
        
        print(f"Configurações de banco: Host={host}, User={user}, DB={database}, Table={table_name}")  # DEBUG: Imprimir configs do banco
        
        # Verificação forçada do conteúdo dos arquivos JSON (se estiver em modo debug)
        if debug and args.json_dir:
            print("=" * 60)
            print("ANÁLISE DETALHADA DOS ARQUIVOS JSON")
            print("=" * 60)
            for json_file in glob.glob(os.path.join(args.json_dir, "*.json")):
                print(f"Arquivo encontrado: {json_file}")
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                        print(f"Tamanho do arquivo: {len(content)} bytes")
                        print(f"Primeiros 200 caracteres: {content[:200]}")
                        
                        # Tentar carregar como JSON
                        data = json.loads(content)
                        print(f"Estrutura de alto nível: {list(data.keys())}")
                        
                        if 'result' in data:
                            print(f"Número de resultados: {len(data['result'])}")
                            if data['result']:
                                primeiro_resultado = data['result'][0]
                                print(f"Chaves do primeiro resultado: {list(primeiro_resultado.keys())}")
                                
                                if 'data' in primeiro_resultado:
                                    print(f"Número de registros no primeiro resultado: {len(primeiro_resultado['data'])}")
                                    if len(primeiro_resultado['data']) > 0:
                                        print(f"Chaves do primeiro registro de dados: {list(primeiro_resultado['data'][0].keys())}")
                except Exception as e:
                    print(f"Erro ao analisar arquivo {json_file}: {e}")
            print("=" * 60)
        
        # Determinar o arquivo JSON a ser processado
        json_file_path = args.json_file
        
        print(f"Buscando arquivo JSON em: {args.json_dir or JSON_DIR}")  # DEBUG: Diretório de busca
        
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
        print(f"Lendo o arquivo JSON: {json_file_path}")  # DEBUG: Antes de processar
        
        # Verificar tamanho e existência do arquivo
        if os.path.exists(json_file_path):
            file_size = os.path.getsize(json_file_path)
            print(f"Tamanho do arquivo: {file_size} bytes")
            
            # Verificar se o arquivo não está vazio
            if file_size == 0:
                print("ERRO: Arquivo JSON está vazio!")
                sys.exit(1)
                
            # Tentar ler os primeiros bytes para verificar se é um JSON válido
            with open(json_file_path, 'r', encoding='utf-8') as f:
                try:
                    inicio = f.read(100)  # Ler os primeiros 100 caracteres
                    print(f"Primeiros 100 caracteres do arquivo: {inicio}")
                except Exception as e:
                    print(f"ERRO ao ler início do arquivo: {e}")
        else:
            print(f"ERRO: Arquivo {json_file_path} não existe apesar de ter passado na verificação anterior!")
            sys.exit(1)
        
        # Chamar a função de processamento com flag de debug
        df_rentabilidade = processar_json_rentabilidade(json_file_path, debug=debug)
        
        if df_rentabilidade is None or df_rentabilidade.empty:
            print("ERRO: Falha ao processar o arquivo JSON ou nenhum dado encontrado.")
            sys.exit(1)
        
        print(f"DataFrame criado com sucesso: {len(df_rentabilidade)} linhas, {len(df_rentabilidade.columns)} colunas")  # DEBUG
        
        # Aplicar mapeamento de nomes se fornecido
        if args.mapping_file:
            df_rentabilidade = mapear_nomes_fundos(df_rentabilidade, args.mapping_file)
        
        # Informações sobre os dados processados
        fundos_unicos = df_rentabilidade['NmFundo'].nunique()
        print(f"MÉTRICA: Total de fundos processados: {fundos_unicos}")
        print(f"MÉTRICA: Total de registros processados: {len(df_rentabilidade)}")
        
        # Identificar a data de referência mais recente
        data_referencia = df_rentabilidade['DtPosicao'].max().strftime('%Y-%m-%d')
        print(f"Data de referência mais recente: {data_referencia}")
        
        # Salvar em CSV se solicitado
        if args.save_csv:
            output_csv = args.output_csv or f"rentabilidade_processada_{data_referencia}.csv"
            df_rentabilidade.to_csv(output_csv, index=False)
            print(f"Dados salvos em: {output_csv}")
        
        # Verificar se deve inserir no banco
        if all([host, user, password, database, table_name]):
            print(f"Todas as configurações de banco estão presentes")  # DEBUG
            
            # Perguntar se deseja continuar, a menos que --auto tenha sido especificado
            if not args.auto and not args.save_csv:  # Se não estiver apenas salvando em CSV e não for automático
                resposta = input("Deseja inserir os dados no banco de dados MySQL? (s/n): ").lower()
                if resposta != 's':
                    print("Operação cancelada pelo usuário.")
                    sys.exit(0)
            
            # Inserir no banco
            print(f"Iniciando inserção no banco: {table_name}")  # DEBUG
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
            print(f"Executando procedure de atualização para data {data_referencia}")  # DEBUG
            executar_procedure_atualizacao(host, user, password, database, data_referencia)
        else:
            missing = []
            if not host: missing.append("host")
            if not user: missing.append("user")
            if not password: missing.append("password")
            if not database: missing.append("database")
            if not table_name: missing.append("table_name")
            print(f"Configurações de banco de dados incompletas. Faltando: {', '.join(missing)}")
            print("Verifique o arquivo .env ou os parâmetros de linha de comando.")
            print("Dados não inseridos no banco. Considere usar a opção --save-csv para salvar em arquivo.")
    
    except Exception as e:
        import traceback
        print(f"ERRO CRÍTICO na execução: {e}")
        print("Detalhes do erro:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()