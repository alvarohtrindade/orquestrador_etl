import requests
import datetime
import os
import zipfile
import time
import argparse
import mysql.connector
import traceback

from dotenv import load_dotenv

load_dotenv()

# Configurações da API
AUTH_URL = os.getenv("AUTH_URL")
PORTFOLIO_URL = os.getenv("PORTFOLIO_URL")
TICKET_URL = os.getenv("TICKET_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SCOPE = os.getenv("SCOPE")

# Caminho onde os arquivos XLSX serão armazenados
DOWNLOAD_PATH = os.getenv("BTG_REPORTH_PATH")

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

def request_portfolio_ticket(token: str, data_ref: datetime.date) -> str:
    """
    Solicita o ticket para download do relatório de carteira diária.
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
            "typeReport": 10,  # TIPO ARQUIVO (pode ser ajustado conforme necessário)
            "fundName": ""
        },
        "pageSize": 100,
        "webhookEndpoint": ""
    }
    print(payload)
    
    response = requests.post(PORTFOLIO_URL, headers=headers, json=payload)
    
    if response.status_code == 200:
        return response.json().get("ticket")
    else:
        raise Exception(f"Erro ao solicitar ticket: {response.status_code} - {response.text}")

def download_report_zip(token: str, ticket: str) -> str:
    """Faz o download do relatório ZIP com arquivos XLSX."""
    headers = {
        "accept": "application/zip",
        "X-SecureConnect-Token": token
    }
    
    params = {
        "ticketId": ticket,
        "pageNumber": 1
    }
    
    # Tentar várias vezes com intervalos
    for attempt in range(20):
        response = requests.get(TICKET_URL, headers=headers, params=params)
        print(f"Tentativa {attempt+1}: Status {response.status_code}, Content-Type: {response.headers.get('content-type', 'Não especificado')}")
        
        if response.status_code == 200:
            # Se a resposta for um JSON, verificar se o arquivo está processando
            content_type = response.headers.get('content-type', '').lower()

            if 'application/json' in content_type:
                try:
                    data = response.json()
                    if data.get("result") == "Processando":
                        print(f"Relatório em processamento. Aguardando 25 segundos...")
                        time.sleep(25)
                    else:
                        print(f"Status do relatório: {data}")
                        time.sleep(25)
                except ValueError:
                    # Se não for JSON válido, continuar tentando
                    pass

            # Verificar se é um arquivo ZIP
            elif 'application/zip' in content_type or 'application/octet-stream' in content_type:
                zip_path = os.path.join(DOWNLOAD_PATH, f"relatorio_{ticket}.zip")
                with open(zip_path, 'wb') as f:
                    f.write(response.content)
                return zip_path
            else:
                print(f"Conteúdo inesperado: {response.headers.get('content-type')}. Tentando novamente...")
                time.sleep(15)
        elif response.status_code == 204:
            print(f"Relatório não está pronto. Aguardando 15 segundos...")
            time.sleep(15)
        else:
            print(f"Erro: {response.status_code} - {response.text}")
            time.sleep(15)
    
    raise Exception("Timeout ao aguardar o relatório ficar pronto")

def extract_zip(zip_path: str) -> None:
    """Extrai arquivos XLSX de um arquivo ZIP."""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(DOWNLOAD_PATH)

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
            return get_business_day(today, n_days + 1 or 1)
        except Exception as e:
            raise ValueError(
                f"Erro ao calcular o {n_days or 1} dia útil anterior a {today}: {str(e)}"
            ) from e

def main(n_days: int = None, specific_date: str = None) -> None:
    print('n_days:', n_days)
    print('specific_date:', specific_date)
    try:
        data_ref = resolve_reference_date(n_days, specific_date)
        print('data_ref:', data_ref)
        print(f"Iniciando extração do relatório para a data: {data_ref}")

        token = get_token()
        ticket = request_portfolio_ticket(token, data_ref)
        zip_path = download_report_zip(token, ticket)

        print(f"Descompactando o arquivo ZIP em {zip_path}...")
        extract_zip(zip_path)

        print("Arquivos XLSX foram salvos com sucesso!")

    except Exception as e:       
        print(f"Erro durante a execução do processo: {str(e)}")
        print(traceback.format_exc())  

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download de relatório BTG para uma data específica.')
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument('--n-days', type=int, help='Número de dias úteis a subtrair da data atual')
    group.add_argument('--date',   type=str, help='Data específica no formato YYYY-MM-DD (ex: 2025-03-30)')
    
    args = parser.parse_args()
    main(n_days=args.n_days, specific_date=args.date)
