import requests
import datetime
import os
import zipfile
import time
import argparse
import mysql.connector
import traceback
import glob
import json
import shutil

from dotenv import load_dotenv

load_dotenv()

# Configurações da API
AUTH_URL = os.getenv("AUTH_URL")
# URL específica para relatório de rentabilidade (NAVPerformance)
NAV_PERFORMANCE_URL = os.getenv("NAV_PERFORMANCE_URL")
TICKET_URL = os.getenv("TICKET_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SCOPE = os.getenv("SCOPE_PATRIMONIO")

# Caminho padrão onde os arquivos JSON serão armazenados
DEFAULT_DOWNLOAD_PATH = os.getenv("BTG_RENTABILIDADE")

# Configurações do banco de dados
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

def get_token() -> str:
    """Obtém o token de autenticação da API BTG."""
    headers = {
        "accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": SCOPE
    }

    response = requests.post(AUTH_URL, headers=headers, data=data)

    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        raise Exception(f"Erro na autenticação: {response.status_code} - {response.text}")

def get_business_day(reference_date, n_days_back=1):
    """
    Obtém a data útil correspondente a n_days_back dias úteis atrás a partir da data de referência.
    
    Args:
        reference_date (datetime.date): Data de referência
        n_days_back (int): Número de dias úteis a retroceder
        
    Returns:
        datetime.date: A data útil correspondente
    """
    try:
        # Conectar ao banco de dados
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        
        cursor = conn.cursor()
        
        # Query para encontrar a data útil correspondente
        query = """
        SELECT DtReferencia 
          FROM vw_calendario 
        WHERE DtReferencia <= %s 
          AND Feriado = 0 
          AND FimSemana = 0 
        ORDER BY DtReferencia DESC 
        LIMIT %s, 1
        """
        
        # Usando n_days_back - 1 para o LIMIT, pois queremos pular n_days_back - 1 dias 
        # úteis para chegar no n-ésimo dia útil atrás
        cursor.execute(query, (reference_date, n_days_back - 1))
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if result:
            return result[0]  # Retorna a data encontrada
        else:
            raise Exception(f"Não foi possível encontrar o {n_days_back}º dia útil anterior a {reference_date}")
            
    except mysql.connector.Error as e:
        raise Exception(f"Erro ao consultar o banco de dados: {str(e)}")

def request_rentabilidade_ticket(token: str, data_ref: datetime.date) -> str:
    """
    Solicita o ticket para download do relatório de rentabilidade.
    """
    start_date = datetime.datetime.combine(data_ref, datetime.time.min)
    end_date = datetime.datetime.combine(data_ref, datetime.time.max)
    
    headers = {
        "accept": "text/plain",
        "X-SecureConnect-Token": token,
        "Content-Type": "application/json-patch+json"
    }
    
    payload = {
        "contract": {
            "startDate": start_date.isoformat() + "Z",
            "endDate": end_date.isoformat() + "Z",
            "indexers": [
                "CDIE"
            ],
            "fundName": ""
        },
        "pageSize": 100,
        "webhookEndpoint": ""
    }
    
    print("Payload para requisição de rentabilidade:", payload)
    
    response = requests.post(NAV_PERFORMANCE_URL, headers=headers, json=payload)
    
    if response.status_code == 200:
        print(f"Resposta: {response.text}")
        return response.json().get("ticket")
    else:
        raise Exception(f"Erro ao solicitar ticket de rentabilidade: {response.status_code} - {response.text}")

def get_formatted_date_directory(data_ref: datetime.date) -> str:
    """
    Retorna o nome do diretório formatado com base na data de referência.
    
    Args:
        data_ref: Data de referência
        
    Returns:
        str: Nome do diretório no formato 'dd.mm'
    """
    return data_ref.strftime("%d.%m")

def clean_directory(directory: str) -> None:
    """Limpa todos os arquivos de um diretório."""
    if os.path.exists(directory):
        for file_item in os.listdir(directory):
            file_path = os.path.join(directory, file_item)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Erro ao remover {file_path}: {e}")
        print(f"Diretório {directory} limpo com sucesso.")
    else:
        os.makedirs(directory)
        print(f"Diretório {directory} criado.")

def download_report_json(token: str, ticket: str, data_ref: datetime.date, output_dir: str) -> str:
    """
    Faz o download do relatório JSON de rentabilidade.
    
    Args:
        token: Token de autenticação
        ticket: Ticket da requisição
        data_ref: Data de referência
        output_dir: Diretório para salvar o arquivo
        
    Returns:
        str: Caminho para o arquivo JSON salvo
    """
    headers = {
        "accept": "application/json",
        "X-SecureConnect-Token": token
    }
    
    params = {
        "ticketId": ticket,
        "pageNumber": 1
    }
    
    # Nome do arquivo baseado na data
    file_name = f"{data_ref.strftime('%d_%m')}_rentabilidade_btg.json"
    json_path = os.path.join(output_dir, file_name)
    
    # Tentar várias vezes com intervalos
    for attempt in range(20):
        response = requests.get(TICKET_URL, headers=headers, params=params)
        print(f"Tentativa {attempt+1}: Status {response.status_code}, Content-Type: {response.headers.get('content-type', 'Não especificado')}")
        
        if response.status_code == 200:
            content_type = response.headers.get('content-type', '').lower()

            # Verificar se é um JSON de resposta indicando "Processando"
            if 'application/json' in content_type:
                try:
                    data = response.json()
                    if isinstance(data, dict) and data.get("result") == "Processando":
                        print(f"Relatório em processamento. Aguardando 25 segundos...")
                        time.sleep(25)
                        continue
                    
                    # Se chegou aqui e temos dados JSON válidos, salvar o resultado
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    return json_path
                    
                except ValueError:
                    # Se não for JSON válido, continuar tentando
                    print("Resposta não é um JSON válido")
                    time.sleep(15)
            else:
                # Se recebemos outro tipo de conteúdo, salvar como está
                with open(json_path, 'wb') as f:
                    f.write(response.content)
                return json_path
                
        elif response.status_code == 204:
            print(f"Relatório não está pronto. Aguardando 15 segundos...")
            time.sleep(15)
        else:
            print(f"Erro: {response.status_code} - {response.text}")
            time.sleep(15)
    
    raise Exception("Timeout ao aguardar o relatório ficar pronto")

def resolve_reference_date(n_days: int = None, specific_date: str = None) -> datetime.date:
    """
    Resolve a data de referência com base em uma data específica ou número de dias úteis.

    Args:
        n_days (int, optional): Número de dias úteis a subtrair da data atual.
        specific_date (str, optional): Data específica no formato YYYY-MM-DD.

    Returns:
        datetime.date: A data de referência resolvida.

    Raises:
        ValueError: Se o formato da data for inválido ou ocorrer erro ao calcular o dia útil.
    """

    if specific_date:
        try:
            return datetime.datetime.strptime(specific_date, "%Y-%m-%d").date()
        
        except ValueError as e:
            raise ValueError(
                f"Formato de data inválido: '{specific_date}'. "
                "Use o formato YYYY-MM-DD (ex: 2025-03-30)."
            ) from e
    else:
        today = datetime.date.today()
        try:
            return get_business_day(today, n_days + 1 if n_days is not None else 1)
        except Exception as e:
            raise ValueError(
                f"Erro ao calcular o {n_days or 1} dia útil anterior a {today}: {str(e)}"
            ) from e

def main(n_days: int = None, specific_date: str = None, output_dir: str = None) -> None:
    try:
        # Resolver a data de referência
        data_ref = resolve_reference_date(n_days, specific_date)
        print(f"Iniciando extração do relatório de rentabilidade para a data: {data_ref}")
        
        # Determinar o diretório de destino
        base_output_dir = output_dir or DEFAULT_DOWNLOAD_PATH
        
        # Criar o diretório com formato de data
        date_dir_name = get_formatted_date_directory(data_ref)
        final_output_dir = os.path.join(base_output_dir, date_dir_name)
        
        # Garantir que o diretório base existe
        if not os.path.exists(base_output_dir):
            os.makedirs(base_output_dir)
            print(f"Diretório base criado: {base_output_dir}")
        
        # Verificar se o diretório de data já existe - se não, criar
        if not os.path.exists(final_output_dir):
            os.makedirs(final_output_dir)
            print(f"Diretório de data criado: {final_output_dir}")
        else:
            print(f"Usando diretório de data existente: {final_output_dir}")

        # Obter token e ticket
        token = get_token()
        print(f"Token obtido com sucesso.")
        
        ticket = request_rentabilidade_ticket(token, data_ref)
        print(f"Ticket de relatório de rentabilidade obtido: {ticket}")
        
        # Download do JSON
        json_path = download_report_json(token, ticket, data_ref, final_output_dir)
        print(f"Relatório JSON foi salvo com sucesso em: {json_path}")

    except Exception as e:       
        print(f"Erro durante a execução do processo: {str(e)}")
        print(traceback.format_exc())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download de relatório de rentabilidade BTG para uma data específica.')
    
    # Grupo mutuamente exclusivo para a data
    date_group = parser.add_mutually_exclusive_group(required=False)
    date_group.add_argument('--n-days', type=int, help='Número de dias úteis a subtrair da data atual')
    date_group.add_argument('--date', type=str, help='Data específica no formato YYYY-MM-DD (ex: 2025-03-30)')
    
    # Opção para o diretório de saída
    parser.add_argument('--output-dir', type=str, help='Diretório onde os arquivos serão salvos')
    
    args = parser.parse_args()
    main(n_days=args.n_days, specific_date=args.date, output_dir=args.output_dir)