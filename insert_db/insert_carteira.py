import os
import re
import pandas as pd
import mysql.connector
import sys
import json
import argparse
import time
from datetime import datetime
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Variáveis do banco de dados
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_TABLE = os.getenv('DB_TABLE', 'Ft_CarteiraDiaria')  # Tabela padrão se não estiver definida

# Função para carregar mappings de arquivos JSON
def load_json_mapping(file_path):
    """
    Carrega mapeamentos de arquivos JSON
    
    Args:
        file_path: Caminho para o arquivo JSON
        
    Returns:
        Dicionário com o mapeamento carregado
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Erro ao carregar o arquivo de mapeamento {file_path}: {e}")
        return {}

# Obter diretório do script atual
script_dir = os.path.dirname(os.path.abspath(__file__))
# Diretório mappings está um nível acima no projeto reorganizado
mappings_dir = os.path.join(os.path.dirname(script_dir), 'mappings')

# Carregar mapeamentos
fund_mapping_path = os.path.join(mappings_dir, 'map_nmfundos.json')
description_mapping_path = os.path.join(mappings_dir, 'map_description.json')

print(f"Carregando mapeamentos de: {mappings_dir}")
mapping = load_json_mapping(fund_mapping_path)
descricao_dict = load_json_mapping(description_mapping_path)

# Dicionário grupo_map
grupo_map = {
    'PORTFOLIO INVESTIDO': 15804,
    'RENDA FIXA': 15805,
    'DESPESAS': 'OUTROS',
    'SALDO DE CAIXA': 10108,
    'CAIXA': 10108,
    'ACOES': 'OUTROS',
    'PDD': 11902
}

def ajustar_quantidade(valor):
    """
    Formata o valor da quantidade com separador de milhares e 4 casas decimais
    
    Args:
        valor: Valor numérico ou string a ser formatado
        
    Returns:
        Valor formatado como string
    """
    try:
        # Verifica se o valor é None ou vazio
        if valor is None or valor == "":
            return valor 

        # Converte o valor para número (float)
        valor_num = float(valor)

        # Formata o valor com separador de milhares (ponto) e até 4 casas decimais
        valor_formatado = f"{valor_num:,.4f}".replace('.', 'X').replace(',', '.').replace('X', ',')
        return valor_formatado

    except Exception as e:
        print(f"Erro ao ajustar quantidade '{valor}': {e}")
        return valor  # Retorna o valor original em caso de erro

# Função para aplicar o mapeamento de nomes no DF
def mapear_nomes_fic(df, mapping, apply_mapping=True):
    """
    Aplica mapeamento para normalizar nomes de fundos
    
    Args:
        df: DataFrame com coluna NmFundo
        mapping: Dicionário de mapeamento {nome_original: nome_normalizado}
        apply_mapping: Flag para ativar/desativar o mapeamento
        
    Returns:
        DataFrame com nomes normalizados se apply_mapping=True, 
        ou o DataFrame original se apply_mapping=False
    """
    # Remove espaços extras que podem interferir na comparação
    df['NmFundo'] = df['NmFundo'].str.strip()
    
    # Substitui os nomes de acordo com o dicionário de mapeamento apenas se apply_mapping=True
    if apply_mapping:
        print("Aplicando mapeamento de nomes de fundos...")
        df['NmFundo'] = df['NmFundo'].replace(mapping)
    else:
        print("Mapeamento de nomes de fundos desativado.")

    return df

def process_portfolio_investido(df, nome_fundo, data):
    """
    Processa os dados de portfolio investido do fundo
    
    Args:
        df: DataFrame com os dados brutos
        nome_fundo: Nome do fundo
        data: Data da posição
        
    Returns:
        DataFrame processado e nomes das colunas
    """
    try:
        start_index = df[df[df.columns[0]] == 'Portfolio_Investido'].index[0]
        end_index = df[df[df.columns[0]] == 'DESPESAS'].index[0]
        portfolio_df = df.loc[start_index:end_index].iloc[1:-3].reset_index(drop=True)

        new_column_names = portfolio_df.iloc[0].tolist()
        portfolio_df.columns = new_column_names
        portfolio_df = portfolio_df[1:].reset_index(drop=True)
        portfolio_df.insert(0, 'Nome Fundo', nome_fundo)
        portfolio_df.insert(1, 'Data', data)
        portfolio_df = portfolio_df.iloc[:, :9].drop(columns=['ISIN', 'CNPJ', '% P.L.'])
        portfolio_df['Classificacao'] = 'PORTFOLIO INVESTIDO'
        
        # Adicionando a coluna Tp_Fundo com valor None
        portfolio_df['Tp_Fundo'] = None
        # Não definimos 'Descricao' aqui, pois será o valor da coluna 'Portfólio Inv.'
        portfolio_df['Cod'] = grupo_map.get('PORTFOLIO INVESTIDO', None)
        
        return portfolio_df, new_column_names
    except Exception as e:
        print(f"Erro ao processar Portfolio Investido do fundo '{nome_fundo}': {e}")
        return None, None


def process_titulos_publicos(df, nome_fundo, data, new_column_names):
    """
    Processa os dados de títulos públicos do fundo
    
    Args:
        df: DataFrame com os dados brutos
        nome_fundo: Nome do fundo
        data: Data da posição
        new_column_names: Nomes das colunas
        
    Returns:
        DataFrame processado
    """
    try:
        start_index = df[df[df.columns[0]] == 'Titulos_Publicos'].index[0]
        end_index = df[df[df.columns[0]].isna() & (df.index > start_index)].index[0]
        titulos_df = df.loc[start_index:end_index-1].iloc[1:].reset_index(drop=True)
        titulos_df.columns = new_column_names
        titulos_df['CNPJ'] = ''
        titulos_df['Quantidade'] = ''
        titulos_df['Quota'] = ''
        titulos_df['Portfólio Inv.'] = titulos_df['Financeiro']
        titulos_df['Financeiro'] = titulos_df['Var.Diária']
        titulos_df.iloc[:, titulos_df.columns.get_loc('% P.L.')] = titulos_df.iloc[:, -2]
        titulos_df = titulos_df.drop(columns=['ISIN']).iloc[:, :6].drop(index=0)
        titulos_df.insert(0, 'Nome Fundo', nome_fundo)
        titulos_df.insert(1, 'Data', data)
        titulos_df['Classificacao'] = 'RENDA FIXA'
        
        # Adicionando a coluna Tp_Fundo com valor None
        titulos_df['Tp_Fundo'] = None
        # Não definimos 'Descricao' aqui, pois será o valor da coluna 'Portfólio Inv.'
        titulos_df['Cod'] = grupo_map.get('RENDA FIXA', None)
        
        return titulos_df
    except Exception as e:
        print(f"Erro ao processar Títulos Públicos do fundo '{nome_fundo}': {e}")
        return None


def process_acoes(df, nome_fundo, data, new_column_names):
    """
    Processa os dados de ações do fundo
    
    Args:
        df: DataFrame com os dados brutos
        nome_fundo: Nome do fundo
        data: Data da posição
        new_column_names: Nomes das colunas
        
    Returns:
        DataFrame processado
    """
    try:
        start_index = df[df[df.columns[0]] == 'Acoes'].index[0]
        end_index = df[df[df.columns[0]].isna() & (df.index > start_index)].index[0]
        acoes_df = df.loc[start_index:end_index-1].iloc[1:].reset_index(drop=True)
        acoes_df.columns = new_column_names
        acoes_df['Portfólio Inv.'] = acoes_df['Quantidade']
        acoes_df['Quantidade'] = acoes_df['Quota']
        acoes_df['Quota'] = acoes_df['Financeiro']
        acoes_df['Financeiro'] = acoes_df['% P.L.']
        acoes_df = acoes_df.drop(columns=['ISIN', 'CNPJ', '% P.L.']).iloc[:, :4].drop(index=0)
        acoes_df.insert(0, 'Nome Fundo', nome_fundo)
        acoes_df.insert(1, 'Data', data)
        acoes_df['Classificacao'] = 'ACOES'
        
        # Adicionando a coluna Tp_Fundo com valor None
        acoes_df['Tp_Fundo'] = None
        # Não definimos 'Descricao' aqui, pois será o valor da coluna 'Portfólio Inv.'
        acoes_df['Cod'] = grupo_map.get('OUTROS', None)
        
        return acoes_df
    except Exception as e:
        print(f"Erro ao processar Ações do fundo '{nome_fundo}': {e}")
        return None


def process_despesas(df, nome_fundo, data):
    """
    Processa os dados de despesas do fundo
    
    Args:
        df: DataFrame com os dados brutos
        nome_fundo: Nome do fundo
        data: Data da posição
        
    Returns:
        DataFrame processado
    """
    try:
        start_index = df[df[df.columns[0]] == 'DESPESAS'].index[0]
        despesas_df = df.loc[start_index:].iloc[1:, :4]
        new_column_names = despesas_df.iloc[0].tolist()
        despesas_df.columns = new_column_names
        despesas_df = despesas_df[1:].reset_index(drop=True)
        despesas_df = despesas_df.rename(columns={'Nome': 'Portfólio Inv.', 'Valor': 'Financeiro'})
        despesas_df = despesas_df.drop(columns=['Data Início Vigência', 'Data Fim Vigência'])
        despesas_df.insert(0, 'Nome Fundo', nome_fundo)
        despesas_df.insert(1, 'Data', data)
        despesas_df['Classificacao'] = 'DESPESAS'
        
        # Adicionando a coluna Tp_Fundo com valor None
        despesas_df['Tp_Fundo'] = None
        # Não definimos 'Descricao' aqui, pois será o valor da coluna 'Portfólio Inv.'
        despesas_df['Cod'] = grupo_map.get('DESPESAS', None)
        
        return despesas_df
    except Exception as e:
        print(f"Erro ao processar Despesas do fundo '{nome_fundo}': {e}")
        return None


def process_caixa(df, nome_fundo, data):
    """
    Processa os dados de caixa do fundo
    
    Args:
        df: DataFrame com os dados brutos
        nome_fundo: Nome do fundo
        data: Data da posição
        
    Returns:
        DataFrame processado
    """
    try:
        caixa_row = df[df[df.columns[0]] == 'C/C SALDO FUNDO'].index[0]
        financeiro_value = df.iloc[caixa_row, 1]
        caixa_df = pd.DataFrame({
            'Nome Fundo': [nome_fundo],
            'Data': [data],
            'Portfólio Inv.': ['C/C SALDO FUNDO'],
            'Financeiro': [financeiro_value],
            'Classificacao': ['CAIXA'],
            'Tp_Fundo': [None],
            # Não definimos 'Descricao' aqui, pois será o valor da coluna 'Portfólio Inv.'
            'Cod': grupo_map.get('SALDO DE CAIXA', None)
        })
        return caixa_df
    except Exception as e:
        print(f"Erro ao processar Caixa do fundo '{nome_fundo}': {e}")
        return None


def process_titulos_privados(df, nome_fundo, data):
    """
    Processa os dados de títulos privados do fundo
    
    Args:
        df: DataFrame com os dados brutos
        nome_fundo: Nome do fundo
        data: Data da posição
        
    Returns:
        DataFrame processado
    """
    try:
        # Localizar índices de início e fim da seção
        start_index = df[df[df.columns[0]] == 'Titulos_Privados'].index[0]
        end_index = df[df[df.columns[0]].isna() & (df.index > start_index)].index[0]

        # Selecionar e ajustar os dados
        titulos_privados_df = df.loc[start_index:end_index-1].iloc[1:].reset_index(drop=True)
        titulos_privados_df.columns = titulos_privados_df.iloc[0]  # Usar a primeira linha como header
        titulos_privados_df = titulos_privados_df[1:]  # Excluir linha do header

        # Selecionar colunas específicas
        titulos_privados_df = titulos_privados_df[['Data', 'Vencimento', 'Quantidade', 'Título', 'Financeiro']]
        
        # Renomear colunas
        titulos_privados_df = titulos_privados_df.rename(columns={'Data': 'DataAplicacao', 'Título': 'Portfólio Inv.'})
        
        # Adicionar informações adicionais
        titulos_privados_df.insert(0, 'Nome Fundo', nome_fundo)
        titulos_privados_df.insert(1, 'Data', data)
        titulos_privados_df['Classificacao'] = 'RENDA FIXA'

        # Ajustar formato das datas
        titulos_privados_df['DataAplicacao'] = pd.to_datetime(titulos_privados_df['DataAplicacao'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
        titulos_privados_df['Vencimento'] = pd.to_datetime(titulos_privados_df['Vencimento'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')

        # Tratar valores ausentes
        titulos_privados_df = titulos_privados_df.fillna('')
        
        # Adicionando a coluna Tp_Fundo com valor None
        titulos_privados_df['Tp_Fundo'] = None
        titulos_privados_df['Descricao'] = None
        titulos_privados_df['Cod'] = grupo_map.get('RENDA FIXA', None)  # Preencher com o código correto
        #titulos_privados_df['Portfólio Inv.'] = titulos_privados_df.apply(atualizar_grupo, axis=1)

        return titulos_privados_df
    except Exception as e:
        print(f"Erro ao processar Títulos Privados do fundo '{nome_fundo}': {e}")
        return None


def titulos_publicos(df, nome_fundo, data):
    """
    Processa os dados de títulos públicos do fundo
    
    Args:
        df: DataFrame com os dados brutos
        nome_fundo: Nome do fundo
        data: Data da posição
        
    Returns:
        DataFrame processado
    """
    try:
        # Localizar índices de início e fim da seção
        start_index = df[df[df.columns[0]] == 'Titulos_Publicos'].index[0]
        end_index = df[df[df.columns[0]].isna() & (df.index > start_index)].index[0]
        
        # Selecionar e ajustar os dados
        publicos_df = df.loc[start_index:end_index-1].iloc[1:].reset_index(drop=True)
        publicos_df.columns = publicos_df.iloc[0]  # Usar a primeira linha como header
        publicos_df = publicos_df[1:]  # Excluir linha do header
        
        # Selecionar colunas específicas
        publicos_df = publicos_df[['Data', 'Vencimento', 'Quantidade', 'Título', 'Financeiro']]
        
        # Renomear colunas
        publicos_df = publicos_df.rename(columns={'Data': 'DataAplicacao', 'Título': 'Portfólio Inv.'})
        
        # Adicionar informações adicionais
        publicos_df.insert(0, 'Nome Fundo', nome_fundo)
        publicos_df.insert(1, 'Data', data)
        publicos_df['Classificacao'] = 'RENDA FIXA'

        # Ajustar formato das datas
        publicos_df['DataAplicacao'] = pd.to_datetime(publicos_df['DataAplicacao'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
        publicos_df['Vencimento'] = pd.to_datetime(publicos_df['Vencimento'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')

        # Tratar valores ausentes
        publicos_df = publicos_df.fillna('')
        
        # Adicionando a coluna Tp_Fundo com valor None
        publicos_df['Tp_Fundo'] = None
        publicos_df['Descricao'] = None
        publicos_df['Cod'] = grupo_map.get('RENDA FIXA', None)  # Preencher com o código correto
        #publicos_df['Portfólio Inv.'] = publicos_df.apply(atualizar_grupo, axis=1)
        
        return publicos_df
    except Exception as e:
        print(f"Erro ao processar Títulos Públicos do fundo '{nome_fundo}': {e}")
        return None


def extract_and_format_data(df):
    """
    Extrai e formata os dados do fundo a partir do DataFrame bruto
    
    Args:
        df: DataFrame com os dados brutos
        
    Returns:
        DataFrame processado
    """
    try:
        nome_fundo = df.iloc[5, 0].replace('_', ' ')
        data = df.iloc[6, 1]
        
        print(f"Processando fundo: {nome_fundo} | Data: {data}")
        
        portfolio_df, new_column_names = process_portfolio_investido(df, nome_fundo, data)
        
        if portfolio_df is None:
            return None
        
        titulos_df = process_titulos_publicos(df, nome_fundo, data, new_column_names) if 'Titulos_Publicos' in df[df.columns[0]].values else None
        acoes_df = process_acoes(df, nome_fundo, data, new_column_names) if 'Acoes' in df[df.columns[0]].values else None
        despesas_df = process_despesas(df, nome_fundo, data) if 'DESPESAS' in df[df.columns[0]].values else None
        caixa_df = process_caixa(df, nome_fundo, data) if 'C/C SALDO FUNDO' in df[df.columns[0]].values else None
        publicos_df = titulos_publicos(df, nome_fundo, data) if 'Titulos_Publicos' in df[df.columns[0]].values else None
        titulos_privados_df = process_titulos_privados(df, nome_fundo, data) if 'Titulos_Privados' in df[df.columns[0]].values else None

        final_df = pd.concat([portfolio_df, titulos_df, acoes_df, despesas_df, caixa_df, publicos_df, titulos_privados_df], ignore_index=True)
        final_df = final_df.replace('nan', '').fillna('')
        
        return final_df
    except Exception as e:
        print(f"Erro ao extrair e formatar dados do fundo: {e}")
        return None


def read_excel_file(file_path):
    """
    Lê um arquivo Excel e o converte para CSV temporário para garantir melhor compatibilidade
    
    Args:
        file_path: Caminho para o arquivo Excel
        
    Returns:
        DataFrame com os dados do arquivo Excel
    """
    try:
        print(f"Lendo arquivo: {file_path}")
        df = pd.read_excel(file_path)
        temp_csv_path = os.path.join(os.path.dirname(file_path), f"temp_{os.path.basename(file_path)}.csv")
        df.to_csv(temp_csv_path, index=False, header=True)
        df = pd.read_csv(temp_csv_path)
        os.remove(temp_csv_path)
        return df
    except Exception as e:
        print(f"Erro ao ler arquivo Excel {file_path}: {e}")
        return None


def process_files(input_directory):
    """
    Processa todos os arquivos Excel em um diretório
    
    Args:
        input_directory: Diretório com os arquivos Excel
        
    Returns:
        DataFrame com todos os dados processados
    """
    start_time = time.time()
    
    # Verificar se o diretório existe
    if not os.path.exists(input_directory):
        print(f"ERRO: Diretório {input_directory} não encontrado.")
        return pd.DataFrame()
    
    # Listar arquivos XLSX
    files = [f for f in os.listdir(input_directory) if f.endswith('.xlsx')]
    
    if not files:
        print(f"AVISO: Nenhum arquivo XLSX encontrado no diretório {input_directory}")
        return pd.DataFrame()
    
    print(f"MÉTRICA: Encontrados {len(files)} arquivos XLSX para processamento")
    
    all_dataframes = []
    fundos_processados = []
    
    for file in files:
        file_path = os.path.join(input_directory, file)
        print(f"Processando arquivo: {file}")
        
        df = read_excel_file(file_path)
        
        if df is None:
            continue
            
        final_df = extract_and_format_data(df)
        
        if final_df is not None:
            all_dataframes.append(final_df)
            
            # Adicionar o nome do fundo à lista de fundos processados (se não estiver já)
            nome_fundo = final_df['Nome Fundo'].iloc[0] if not final_df.empty else "Desconhecido"
            if nome_fundo not in fundos_processados:
                fundos_processados.append(nome_fundo)
    
    if all_dataframes:
        result_df = pd.concat(all_dataframes, ignore_index=True)
        
        # Registrar métricas
        elapsed_time = time.time() - start_time
        print(f"MÉTRICA: Tempo de execução: {elapsed_time:.2f} segundos")
        print(f"MÉTRICA: Total de fundos processados: {len(fundos_processados)}")
        print(f"MÉTRICA: Total de registros processados: {len(result_df)}")
        
        return result_df
    else:
        print("Nenhum dado processado.")
        return pd.DataFrame()


def padronizar_descricao(descricao):
    """
    Padroniza a descrição de ajustes de cota
    
    Args:
        descricao: Descrição a ser padronizada
        
    Returns:
        Descrição padronizada
    """
    if pd.notna(descricao) and "AJUSTE" in str(descricao).upper() and "COTA" in str(descricao).upper():
        return "AJUSTE DE COTA"
    return descricao


def extrair_tipo_fundo(nome_fundo):
    """
    Extrai o tipo de fundo a partir do nome do fundo
    
    Args:
        nome_fundo: Nome do fundo
        
    Returns:
        Tipo do fundo
    """
    # Procurar padrões comuns de tipos de fundos
    tipos = ['FIM', 'FIC', 'FIDC', 'FI', 'FICFIDC', 'FICFIM']
    
    # Verificar cada tipo no nome do fundo
    for tipo in tipos:
        if tipo in nome_fundo:
            return tipo
    
    # Se não encontrou nenhum tipo conhecido, retorna o primeiro token como fallback
    return nome_fundo.split()[0] if nome_fundo else None


def prepare_dataframe(df, apply_mapping=True):
    """
    Prepara o DataFrame para inserção, incluindo renomeação, formatação e limpeza dos dados
    
    Args:
        df: DataFrame bruto
        apply_mapping: Flag para ativar/desativar o mapeamento de nomes de fundos
        
    Returns:
        DataFrame preparado para inserção
    """
    # Renomear colunas
    df = df.rename(columns={
        'Nome Fundo': 'NmFundo',
        'Data': 'DtPosicao',
        'Portfólio Inv.': 'Descricao',
        'Quantidade': 'Qnt',
        'Financeiro': 'VlrMercado',
        'Classificacao': 'Grupo'
    })
    
    # Remover coluna 'Quota' se existir
    if 'Quota' in df.columns:
        df = df.drop(columns=['Quota'])
    
    # Incluir a Coluna "Custodiante"
    df['Custodiante'] = 'BTG'
    
    # Incluir coluna DocFundo vazia (será preenchida pela procedure MySQL)
    df['DocFundo'] = ''
    
    # Ajustar a coluna Qnt
    df['Qnt'] = df['Qnt'].apply(ajustar_quantidade)
    
    # Extrair o tipo de fundo
    df['Tp_Fundo'] = df['NmFundo'].apply(extrair_tipo_fundo)
    
    # Ajustar o formato da data
    df['DtPosicao'] = pd.to_datetime(df['DtPosicao'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
    
    # Adicionar colunas de datas se não existirem
    if 'DataVenc' not in df.columns:
        df["DataVenc"] = None
    if 'DataAplic' not in df.columns:
        df["DataAplic"] = None
    
    # Adicionar a coluna Categoria com o valor do grupo
    df['Categoria'] = df['Grupo']
    
    # Padronizar descrições
    df['Descricao'] = df['Descricao'].apply(padronizar_descricao)
    
    # Ajuste nos nomes dos fundos (opcional)
    df = mapear_nomes_fic(df, mapping, apply_mapping)
    
    # Definir a nova ordem das colunas
    colunas_ordem = ['NmFundo', 'DocFundo', 'Tp_Fundo', 'DtPosicao', 'Descricao', 'Qnt', 
                     'VlrMercado', 'DataVenc', 'DataAplic', 'Categoria', 
                     'Cod', 'Grupo', 'Custodiante']
    
    # Garantir que não há valores NaN
    df = df.fillna("")
    
    return df[colunas_ordem]


def insert_batch_to_mysql(df, connection, table_name):
    """
    Insere um lote do DataFrame no banco de dados
    
    Args:
        df: DataFrame com o lote a ser inserido
        connection: Conexão com o banco de dados
        table_name: Nome da tabela
    """
    cursor = connection.cursor()
    
    try:
        for i, row in df.iterrows():
            columns = ', '.join(row.index)
            placeholders = ', '.join(['%s'] * len(row))
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(row))
        
        connection.commit()
    except Exception as e:
        print(f"Erro ao inserir lote: {e}")
        connection.rollback()
    finally:
        cursor.close()


def insert_dataframe_in_batches(df, connection, table_name, batch_size=10000):
    """
    Insere um DataFrame em lotes no banco de dados
    
    Args:
        df: DataFrame a ser inserido
        connection: Conexão com o banco de dados
        table_name: Nome da tabela
        batch_size: Tamanho de cada lote
    """
    total_registros = len(df)
    start_time = time.time()
    
    for i in range(0, total_registros, batch_size):
        pedaco_df = df.iloc[i:i+batch_size]
        insert_batch_to_mysql(pedaco_df, connection, table_name)
        registros_processados = min(i + batch_size, total_registros)
        print(f"MÉTRICA: Registros inseridos: {registros_processados} / {total_registros} ({(registros_processados/total_registros)*100:.1f}%)")
    
    elapsed_time = time.time() - start_time
    print(f"MÉTRICA: Tempo de inserção no banco: {elapsed_time:.2f} segundos")


def insert_to_mysql(final_dataframe, host, database, user, password):
    """
    Insere os dados no banco MySQL
    
    Args:
        final_dataframe: DataFrame com os dados a serem inseridos
        host: Host do banco de dados
        database: Nome do banco de dados
        user: Usuário do banco de dados
        password: Senha do banco de dados
    """
    # Verificar se o DataFrame está vazio
    if final_dataframe.empty:
        print("ERRO: Nenhum dado para inserir no banco.")
        return False
    
    # Conectar ao banco de dados MySQL
    try:
        print("Conectando ao banco de dados MySQL...")
        connection = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        
        if connection.is_connected():
            print('Conectado ao MySQL com sucesso')
            
            # Inserir dados em lotes
            print(f"Iniciando inserção de {len(final_dataframe)} registros na tabela {DB_TABLE}")
            insert_dataframe_in_batches(final_dataframe, connection, DB_TABLE)
            
            # Executar a procedure para atualizar os CNPJs
            cursor = connection.cursor()
            data_posicao = final_dataframe['DtPosicao'].iloc[0]  # Assume que todos os registros têm a mesma data
            
            print(f"Executando procedure para atualizar CNPJs para a data {data_posicao}...")
            cursor.callproc('ATT_CNPJ_CARTEIRA_DIARIA', [data_posicao])
            connection.commit()
            
            print('Inserção e atualização de CNPJs concluídas com sucesso')
            cursor.close()
            
            return True
            
    except Exception as e:
        print(f"ERRO ao inserir dados no banco: {e}")
        return False
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print('Conexão com o MySQL encerrada')


def main():
    """
    Função principal que coordena o processamento de arquivos e inserção no banco de dados
    """
    print("=" * 80)
    print("INICIANDO PROCESSAMENTO DE CARTEIRA DIÁRIA BTG")
    print("=" * 80)
    
    start_time = time.time()
    
    # Configurar o parser de argumentos
    parser = argparse.ArgumentParser(description='Processa arquivos de carteira diária BTG')
    parser.add_argument('input_directory', help='Diretório contendo os arquivos a serem processados')
    parser.add_argument('--no-mapping', action='store_true', help='Desativa o mapeamento de nomes de fundos')
    parser.add_argument('--save-csv', action='store_true', help='Salva os dados em CSV sem inserir no banco')
    parser.add_argument('--output-csv', help='Caminho para salvar o arquivo CSV (usado com --save-csv)')
    parser.add_argument('--auto', action='store_true', help='Executa automaticamente sem solicitar confirmação')
    
    args = parser.parse_args()
    
    input_directory = args.input_directory
    apply_mapping = not args.no_mapping  # Inverter a flag (--no-mapping significa apply_mapping=False)
    
    print(f"Diretório de entrada: {input_directory}")
    print(f"Mapeamento de nomes de fundos: {'DESATIVADO' if args.no_mapping else 'ATIVADO'}")
    print(f"Modo: {'Automático' if args.auto else 'Interativo'}")
    
    # Processar os arquivos
    final_dataframe = process_files(input_directory)
    
    if final_dataframe.empty:
        print("ERRO: Nenhum dado para processar.")
        print("=" * 80)
        print("PROCESSAMENTO FINALIZADO COM FALHA")
        print("=" * 80)
        return 1
    
    # Preparar o DataFrame (passando a flag de mapeamento)
    final_dataframe = prepare_dataframe(final_dataframe, apply_mapping)
    
    print(f"MÉTRICA: Total de registros processados: {len(final_dataframe)}")
    
    # Salvar em CSV se solicitado
    if args.save_csv:
        output_csv = args.output_csv or os.path.join(input_directory, f"processado_carteira_diaria_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        final_dataframe.to_csv(output_csv, index=False)
        print(f"Dados salvos em: {output_csv}")
        
        total_time = time.time() - start_time
        print(f"MÉTRICA: Tempo total de execução: {total_time:.2f} segundos")
        print("=" * 80)
        print("PROCESSAMENTO FINALIZADO COM SUCESSO (MODO CSV)")
        print("=" * 80)
        return 0
    
    # Verificar credenciais do banco
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        print("ERRO: Credenciais de banco de dados incompletas no arquivo .env")
        print("Verifique se as variáveis DB_HOST, DB_NAME, DB_USER e DB_PASSWORD estão definidas.")
        print("=" * 80)
        print("PROCESSAMENTO FINALIZADO COM FALHA")
        print("=" * 80)
        return 1
    
    # Se --auto foi especificado, não perguntar por confirmação
    if args.auto:
        sucesso = insert_to_mysql(final_dataframe, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)
    else:
        # Pergunte se deseja inserir os dados no banco de dados
        resposta = input("Deseja inserir os dados no banco de dados MySQL? (s/n): ")
        if resposta.lower() == 's':
            sucesso = insert_to_mysql(final_dataframe, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)
        else:
            print("Operação cancelada pelo usuário.")
            sucesso = False
    
    total_time = time.time() - start_time
    print(f"MÉTRICA: Tempo total de execução: {total_time:.2f} segundos")
    print("=" * 80)
    
    if sucesso:
        print("PROCESSAMENTO FINALIZADO COM SUCESSO")
    else:
        print("PROCESSAMENTO FINALIZADO COM FALHA")
    
    print("=" * 80)
    return 0 if sucesso else 1

if __name__ == "__main__":
    exit(main())