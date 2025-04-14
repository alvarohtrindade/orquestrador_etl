#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Orquestrador BTG ETL

Este script orquestra o fluxo completo de ETL para dados BTG,
incluindo extração da API, processamento e carga no banco de dados.

Autor: Álvaro - Equipe Data Analytics - Catalise Investimentos
"""

import os
import argparse
import subprocess
import datetime
import logging
import glob
import json
from pathlib import Path
from dotenv import load_dotenv

# Caminhos para os diretórios do projeto
PROJECT_ROOT = Path(__file__).parent.absolute()
API_DIR = PROJECT_ROOT / "apis"
INSERT_DIR = PROJECT_ROOT / "insert_db"
LOGS_DIR = PROJECT_ROOT / "logs"
MAPPING_DIR = PROJECT_ROOT / "mappings"

# Carregar variáveis de ambiente
load_dotenv()

# Configurar logging
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / f"orquestrador_btg_{datetime.datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("OrquestradorBTG")

# Diretórios padrão a partir do .env
BTG_REPORT_PATH = os.getenv("BTG_REPORT_PATH")
BTG_RENTABILIDADE = os.getenv("BTG_RENTABILIDADE")

def executar_comando(comando, mensagem_erro, capturar_metricas=False):
    """
    Executa um comando de shell e verifica o status de saída
    
    Args:
        comando: Lista com o comando e seus argumentos
        mensagem_erro: Mensagem em caso de erro
        capturar_metricas: Se True, analisa a saída para extrair métricas
        
    Returns:
        tuple: (sucesso, metricas) onde:
            - sucesso é um bool indicando se o comando foi executado com sucesso
            - metricas é um dict com informações extraídas da saída
    """
    logger.info(f"Executando: {' '.join(comando)}")
    
    resultado = subprocess.run(comando, capture_output=True, text=True)
    
    # Inicializar dicionário de métricas
    metricas = {
        "total_fundos": 0,
        "total_registros": 0,
        "fundos_processados": [],
        "tempo_execucao": 0
    }
    
    if resultado.returncode != 0:
        logger.error(f"{mensagem_erro}: {resultado.stderr}")
        return False, metricas
    
    logger.info("Comando executado com sucesso")
    
    # Se solicitado, extrair métricas da saída
    if capturar_metricas:
        # Analisar stdout para encontrar informações relevantes
        output_lines = resultado.stdout.splitlines()
        
        # Procurar por padrões comuns nos logs
        for linha in output_lines:
            # Capturar número total de registros processados
            if "registros processados" in linha.lower() or "MÉTRICA: Total de registros processados" in linha:
                try:
                    metricas["total_registros"] = int(''.join(filter(str.isdigit, linha.split(":")[-1])))
                    logger.info(f"MÉTRICA: Total de registros processados: {metricas['total_registros']}")
                except:
                    pass
            
            # Capturar número de fundos encontrados
            elif "fundos encontrados" in linha.lower() or "fundos processados" in linha.lower() or "arquivos de fundos extraídos" in linha.lower() or "MÉTRICA: Total de fundos" in linha:
                try:
                    metricas["total_fundos"] = int(''.join(filter(str.isdigit, linha.split(":")[-1])))
                    logger.info(f"MÉTRICA: Total de fundos processados: {metricas['total_fundos']}")
                except:
                    pass
            
            # Capturar nomes de fundos processados
            elif "processando fundo:" in linha.lower():
                nome_fundo = linha.lower().split("processando fundo:")[-1].strip()
                if nome_fundo:
                    metricas["fundos_processados"].append(nome_fundo)
            
            # Capturar tempo de execução
            elif "tempo de execução:" in linha.lower() or "MÉTRICA: Tempo" in linha:
                try:
                    tempo_str = linha.lower().split(":")[-1].strip()
                    # Tentar extrair segundos
                    if "segundo" in tempo_str:
                        metricas["tempo_execucao"] = float(''.join(filter(lambda c: c.isdigit() or c == '.', tempo_str)))
                        logger.info(f"MÉTRICA: Tempo de execução: {metricas['tempo_execucao']} segundos")
                except:
                    pass
        
        # Também examinar o stderr para possíveis mensagens informativas
        error_lines = resultado.stderr.splitlines()
        for linha in error_lines:
            if "aviso:" in linha.lower() and "fundo" in linha.lower():
                logger.warning(f"AVISO: {linha}")
    
    return True, metricas

def processar_carteira(data=None, dias_atras=1, diretorio_saida=None):
    """
    Processa o fluxo de carteira diária
    
    Args:
        data: Data específica no formato YYYY-MM-DD
        dias_atras: Número de dias úteis atrás para processamento
        diretorio_saida: Diretório personalizado para saída
        
    Returns:
        tuple: (sucesso, metricas) onde:
            - sucesso é um bool indicando se o processamento foi concluído com sucesso
            - metricas é um dict com informações extraídas da saída
    """
    logger.info("=" * 50)
    logger.info("INICIANDO PROCESSAMENTO DE CARTEIRA DIÁRIA")
    logger.info("=" * 50)
    
    # Inicializar métricas
    metricas_carteira = {
        "tipo": "carteira",
        "data_processamento": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "data_referencia": data if data else f"{dias_atras} dias atrás",
        "extracao": {
            "total_fundos": 0,
            "arquivos_extraidos": 0,
            "tempo_execucao": 0
        },
        "processamento": {
            "total_registros": 0,
            "total_fundos_processados": 0,
            "tempo_execucao": 0
        }
    }
    
    # Etapa 1: Extração de dados
    cmd_extracao = ["python", str(API_DIR / "api_faas_portfolio.py")]
    
    if data:
        cmd_extracao.extend(["--date", data])
    else:
        cmd_extracao.extend(["--n-days", str(dias_atras)])
        
    if diretorio_saida:
        cmd_extracao.extend(["--output-dir", diretorio_saida])
    
    sucesso_extracao, metricas_extracao = executar_comando(
        cmd_extracao, 
        "Falha na extração de dados de carteira",
        capturar_metricas=True
    )
    
    if not sucesso_extracao:
        return False, metricas_carteira
    
    # Atualizar métricas de extração
    metricas_carteira["extracao"].update(metricas_extracao)
    
    # Determinar o diretório onde os dados foram salvos
    if not data:
        # Se não foi fornecida data, usar dias atrás para obtenção da pasta
        hoje = datetime.date.today()
        data_ref = hoje
        
        # Simplificação: tenta subtrair dias até encontrar a pasta
        for _ in range(10):  # Limitar a 10 tentativas
            data_ref = data_ref - datetime.timedelta(days=1)
            data_pasta = data_ref.strftime("%d.%m")
            
            # Construir caminho completo da pasta
            pasta_dados = diretorio_saida or BTG_REPORT_PATH
            caminho_pasta = os.path.join(pasta_dados, data_pasta)
            
            if os.path.exists(caminho_pasta):
                # Atualizar data de referência nas métricas com a data real encontrada
                metricas_carteira["data_referencia"] = data_ref.strftime('%Y-%m-%d')
                break
    else:
        # Converter data YYYY-MM-DD para o formato de pasta DD.MM
        data_pasta = f"{data[8:10]}.{data[5:7]}"
        
        # Construir caminho completo da pasta
        pasta_dados = diretorio_saida or BTG_REPORT_PATH
        caminho_pasta = os.path.join(pasta_dados, data_pasta)
        
        # Atualizar data de referência nas métricas
        metricas_carteira["data_referencia"] = data
    
    if not os.path.exists(caminho_pasta):
        logger.error(f"Diretório de dados não encontrado: {caminho_pasta}")
        return False, metricas_carteira
    
    logger.info(f"Utilizando diretório de dados: {caminho_pasta}")
    
    # Contar arquivos XLSX no diretório para métricas
    xlsx_files = glob.glob(os.path.join(caminho_pasta, "*.xlsx"))
    metricas_carteira["extracao"]["arquivos_extraidos"] = len(xlsx_files)
    logger.info(f"MÉTRICA: Encontrados {len(xlsx_files)} arquivos XLSX para processamento")
    
    # Etapa 2: Processamento e carga - não passar credenciais via CLI
    cmd_processamento = [
        "python", 
        str(INSERT_DIR / "insert_carteira.py"), 
        caminho_pasta,
        "--auto"  # Usar modo automático para não solicitar confirmação
    ]
    
    sucesso_processamento, metricas_processamento = executar_comando(
        cmd_processamento, 
        "Falha no processamento dos dados de carteira",
        capturar_metricas=True
    )
    
    # Atualizar métricas de processamento
    metricas_carteira["processamento"].update(metricas_processamento)
    
    if not sucesso_processamento:
        return False, metricas_carteira
    
    logger.info("=" * 50)
    logger.info("PROCESSAMENTO DE CARTEIRA CONCLUÍDO COM SUCESSO")
    logger.info(f"- Data de Referência: {metricas_carteira['data_referencia']}")
    logger.info(f"- Total de Fundos: {metricas_carteira['extracao']['total_fundos']}")
    logger.info(f"- Total de Registros Processados: {metricas_carteira['processamento']['total_registros']}")
    logger.info("=" * 50)
    
    return True, metricas_carteira

def processar_rentabilidade(data=None, dias_atras=1, diretorio_saida=None):
    """
    Processa o fluxo de rentabilidade
    
    Args:
        data: Data específica no formato YYYY-MM-DD
        dias_atras: Número de dias úteis atrás para processamento
        diretorio_saida: Diretório personalizado para saída
        
    Returns:
        tuple: (sucesso, metricas) onde:
            - sucesso é um bool indicando se o processamento foi concluído com sucesso
            - metricas é um dict com informações extraídas da saída
    """
    logger.info("=" * 50)
    logger.info("INICIANDO PROCESSAMENTO DE RENTABILIDADE")
    logger.info("=" * 50)
    
    # Inicializar métricas
    metricas_rentabilidade = {
        "tipo": "rentabilidade",
        "data_processamento": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "data_referencia": data if data else f"{dias_atras} dias atrás",
        "extracao": {
            "total_fundos": 0,
            "tempo_execucao": 0
        },
        "processamento": {
            "total_registros": 0,
            "total_fundos_processados": 0,
            "tempo_execucao": 0
        }
    }
    
    # Etapa 1: Extração de dados
    cmd_extracao = ["python", str(API_DIR / "api_faas_rentabilidade.py")]
    
    if data:
        cmd_extracao.extend(["--date", data])
    else:
        cmd_extracao.extend(["--n-days", str(dias_atras)])
        
    if diretorio_saida:
        cmd_extracao.extend(["--output-dir", diretorio_saida])
    
    sucesso_extracao, metricas_extracao = executar_comando(
        cmd_extracao, 
        "Falha na extração de dados de rentabilidade",
        capturar_metricas=True
    )
    
    if not sucesso_extracao:
        return False, metricas_rentabilidade
    
    # Atualizar métricas de extração
    metricas_rentabilidade["extracao"].update(metricas_extracao)
    
    # Determinar o diretório onde os dados foram salvos
    if not data:
        # Se não foi fornecida data, usar dias atrás para obtenção da pasta
        hoje = datetime.date.today()
        data_ref = hoje
        
        # Simplificação: tenta subtrair dias até encontrar a pasta
        for _ in range(10):  # Limitar a 10 tentativas
            data_ref = data_ref - datetime.timedelta(days=1)
            data_pasta = data_ref.strftime("%d.%m")
            
            # Construir caminho completo da pasta
            pasta_dados = diretorio_saida or BTG_RENTABILIDADE
            caminho_pasta = os.path.join(pasta_dados, data_pasta)
            
            if os.path.exists(caminho_pasta):
                # Atualizar data de referência nas métricas com a data real encontrada
                metricas_rentabilidade["data_referencia"] = data_ref.strftime('%Y-%m-%d')
                break
    else:
        # Converter data YYYY-MM-DD para o formato de pasta DD.MM
        data_pasta = f"{data[8:10]}.{data[5:7]}"
        
        # Construir caminho completo da pasta
        pasta_dados = diretorio_saida or BTG_RENTABILIDADE
        caminho_pasta = os.path.join(pasta_dados, data_pasta)
        
        # Atualizar data de referência nas métricas
        metricas_rentabilidade["data_referencia"] = data
    
    if not os.path.exists(caminho_pasta):
        logger.error(f"Diretório de dados não encontrado: {caminho_pasta}")
        return False, metricas_rentabilidade
    
    logger.info(f"Utilizando diretório de dados: {caminho_pasta}")
    
    # Contar arquivos JSON no diretório para métricas
    json_files = glob.glob(os.path.join(caminho_pasta, "*.json"))
    metricas_rentabilidade["extracao"]["arquivos_extraidos"] = len(json_files)
    logger.info(f"MÉTRICA: Encontrado {len(json_files)} arquivo(s) JSON para processamento")
    
    # Etapa 2: Processamento e carga
    cmd_processamento = [
        "python", 
        str(INSERT_DIR / "insert_rentabilidade.py"), 
        "--json-dir", 
        caminho_pasta, 
        "--auto"
    ]
    
    sucesso_processamento, metricas_processamento = executar_comando(
        cmd_processamento, 
        "Falha no processamento dos dados de rentabilidade",
        capturar_metricas=True
    )
    
    # Atualizar métricas de processamento
    metricas_rentabilidade["processamento"].update(metricas_processamento)
    
    if not sucesso_processamento:
        return False, metricas_rentabilidade
    
    logger.info("=" * 50)
    logger.info("PROCESSAMENTO DE RENTABILIDADE CONCLUÍDO COM SUCESSO")
    logger.info(f"- Data de Referência: {metricas_rentabilidade['data_referencia']}")
    logger.info(f"- Total de Fundos: {metricas_rentabilidade['extracao']['total_fundos']}")
    logger.info(f"- Total de Registros Processados: {metricas_rentabilidade['processamento']['total_registros']}")
    logger.info("=" * 50)
    
    return True, metricas_rentabilidade

def main():
    """Função principal que coordena a orquestração"""
    parser = argparse.ArgumentParser(description="Orquestrador de ETL para dados BTG")
    parser.add_argument("--tipo", choices=["carteira", "rentabilidade", "ambos"], default="ambos",
                       help="Tipo de dado a processar")
    parser.add_argument("--data", help="Data específica no formato YYYY-MM-DD")
    parser.add_argument("--dias-atras", type=int, default=1, help="Número de dias úteis no passado")
    parser.add_argument("--dir-carteira", help="Diretório personalizado para carteira")
    parser.add_argument("--dir-rentabilidade", help="Diretório personalizado para rentabilidade")
    parser.add_argument("--salvar-metricas", action="store_true", help="Salvar métricas em arquivo JSON")
    
    args = parser.parse_args()
    
    # Cabeçalho do log
    logger.info("=" * 80)
    logger.info("INICIANDO ORQUESTRADOR BTG ETL")
    logger.info(f"Tipo: {args.tipo}")
    logger.info(f"Data: {args.data if args.data else f'{args.dias_atras} dias atrás'}")
    logger.info("=" * 80)
    
    # Verificar diretórios de saída existem no .env
    if not BTG_REPORT_PATH and not args.dir_carteira and args.tipo in ["carteira", "ambos"]:
        logger.error("Variável BTG_REPORT_PATH não definida no arquivo .env e --dir-carteira não especificado")
        return 1
        
    if not BTG_RENTABILIDADE and not args.dir_rentabilidade and args.tipo in ["rentabilidade", "ambos"]:
        logger.error("Variável BTG_RENTABILIDADE não definida no arquivo .env e --dir-rentabilidade não especificado")
        return 1
    
    sucesso = True
    todas_metricas = []
    
    if args.tipo in ["carteira", "ambos"]:
        sucesso_carteira, metricas_carteira = processar_carteira(args.data, args.dias_atras, args.dir_carteira)
        sucesso = sucesso_carteira and sucesso
        todas_metricas.append(metricas_carteira)
        
    if args.tipo in ["rentabilidade", "ambos"]:
        sucesso_rentabilidade, metricas_rentabilidade = processar_rentabilidade(args.data, args.dias_atras, args.dir_rentabilidade)
        sucesso = sucesso_rentabilidade and sucesso
        todas_metricas.append(metricas_rentabilidade)
    
    # Salvar métricas em arquivo JSON se solicitado
    if args.salvar_metricas and todas_metricas:
        try:
            agora = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            metricas_file = LOGS_DIR / f"metricas_etl_{agora}.json"
            
            with open(metricas_file, 'w', encoding='utf-8') as f:
                json.dump(todas_metricas, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Métricas salvas em: {metricas_file}")
        except Exception as e:
            logger.error(f"Erro ao salvar métricas: {e}")
    
    # Exibir resumo de métricas importantes
    logger.info("=" * 80)
    logger.info("RESUMO DE MÉTRICAS DO PROCESSAMENTO")
    logger.info("=" * 80)
    
    for metrica in todas_metricas:
        logger.info(f"Tipo: {metrica['tipo'].upper()}")
        logger.info(f"Data de Referência: {metrica['data_referencia']}")
        logger.info(f"Extração - Total de Fundos: {metrica['extracao'].get('total_fundos', 0)}")
        logger.info(f"Processamento - Total de Registros: {metrica['processamento'].get('total_registros', 0)}")
        logger.info("-" * 40)
    
    # Rodapé do log
    logger.info("=" * 80)
    logger.info(f"ORQUESTRADOR BTG ETL FINALIZADO - Status: {'SUCESSO' if sucesso else 'FALHA'}")
    logger.info("=" * 80)
    
    # Código de saída para integração com sistemas de agendamento
    return 0 if sucesso else 1

if __name__ == "__main__":
    exit(main())