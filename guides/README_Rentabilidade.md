# Sistema de Rentabilidade BTG

## Visão Geral

Este sistema é responsável pela extração, processamento e armazenamento de dados de rentabilidade de fundos da API BTG FaaS. O fluxo completo é dividido em duas etapas principais:

1. **Extração de dados** (`api_faas_rentabilidade.py`): Conecta à API do BTG, obtém relatórios de rentabilidade e salva os arquivos JSON em um diretório organizado por data.
2. **Processamento e carga** (`insert_rentabilidade.py`): Processa os arquivos JSON baixados, padroniza os dados e os insere no banco de dados MySQL.

## Arquivos do Sistema

- `api_faas_rentabilidade.py` - Script para extração de dados de rentabilidade da API BTG
- `insert_rentabilidade.py` - Script para processamento e carga dos dados de rentabilidade no banco
- `map_nmfundos.json` - Arquivo de mapeamento para padronização de nomes de fundos

## Requisitos

- Python 3.8+
- Bibliotecas listadas em `requirements.txt`
- Credenciais de acesso à API BTG FaaS
- Banco de dados MySQL configurado
- Arquivo `.env` com as configurações necessárias

## Configuração

1. Clone o repositório
2. Instale as dependências: `pip install -r requirements.txt`
3. Copie o arquivo `.env.example` para `.env` e preencha com suas configurações:
   ```
   # Credenciais para API do BTG FaaS
   AUTH_URL=https://funds.btgpactual.com/connect/token
   NAV_PERFORMANCE_URL=https://funds.btgpactual.com/reports/NAVPerformance
   TICKET_URL=https://funds.btgpactual.com/reports/Ticket
   CLIENT_ID=seu_cliente_id
   CLIENT_SECRET=sua_senha
   SCOPE_PATRIMONIO=reports.nav_performance

   # Configurações do banco de dados MySQL
   DB_HOST=seu_host
   DB_USER=seu_usuario
   DB_PASSWORD=sua_senha
   DB_NAME=seu_banco
   DB_RENTABILIDADE=Rentabilidade_Fundos

   # Caminhos para armazenamento
   BTG_RENTABILIDADE=caminho/para/armazenar/relatorios/rentabilidade
   ```

## Guia de Uso - Extração de Dados (api_faas_rentabilidade.py)

Este script conecta à API do BTG, solicita relatórios de rentabilidade e salva os arquivos JSON em pastas organizadas por data.

### Opções disponíveis:

```
--n-days N        : Solicita dados de N dias úteis atrás
--date YYYY-MM-DD : Solicita dados de uma data específica
--output-dir DIR  : Especifica um diretório personalizado para salvar os arquivos
```

### Exemplos de uso:

```bash
# Obter dados do dia útil anterior e salvá-los na pasta padrão
python api_faas_rentabilidade.py --n-days 1

# Obter dados de uma data específica
python api_faas_rentabilidade.py --date 2025-04-10

# Obter dados e salvá-los em um diretório personalizado
python api_faas_rentabilidade.py --n-days 1 --output-dir /dados/relatorios/rentabilidade
```

### Funcionamento:

1. O script obtém um token de autenticação da API BTG
2. Solicita um ticket para o relatório de rentabilidade da data especificada
3. Faz polling para aguardar o processamento do relatório
4. Baixa o arquivo JSON para o diretório de data (formato "dd.mm", ex: "10.04")
5. O arquivo é salvo com o nome "dd_mm_rentabilidade_btg.json" (ex: "10_04_rentabilidade_btg.json")

## Guia de Uso - Processamento e Carga (insert_rentabilidade.py)

Este script processa os arquivos JSON baixados, estrutura os dados de rentabilidade e os insere no banco de dados MySQL.

### Opções disponíveis:

```
--json-file FILE   : Caminho para o arquivo JSON a ser processado (opcional)
--json-dir DIR     : Diretório onde buscar o arquivo JSON mais recente (opcional)
--mapping-file FILE: Caminho para o arquivo de mapeamento de nomes de fundos
--batch-size N     : Tamanho do lote para inserção (padrão: 100)
--save-csv         : Salva os dados processados em um arquivo CSV
--output-csv FILE  : Caminho para salvar o arquivo CSV
--auto             : Executa automaticamente sem pedir confirmação
```

### Exemplos de uso:

```bash
# Processar o arquivo JSON mais recente no diretório padrão (definido em .env)
python insert_rentabilidade.py

# Processar um arquivo JSON específico
python insert_rentabilidade.py --json-file /caminho/para/arquivo/10_04_rentabilidade_btg.json

# Processar o arquivo mais recente em um diretório específico
python insert_rentabilidade.py --json-dir /caminho/para/pasta/10.04

# Processar automaticamente sem confirmação 
python insert_rentabilidade.py --auto

# Salvar em CSV sem inserir no banco
python insert_rentabilidade.py --save-csv --output-csv rentabilidade_processada.csv
```

### Funcionamento:

1. O script identifica o arquivo JSON a ser processado (específico ou mais recente)
2. Extrai e estrutura os dados de rentabilidade de cada fundo:
   - Rentabilidade nominal (dia, mês, ano, 12 meses, 24 meses, 36 meses)
   - Rentabilidade vs CDI (dia, mês, ano, 12 meses, 24 meses, 36 meses)
   - Informações dos fundos (nome, CNPJ, patrimônio, etc.)
3. Padroniza os nomes dos fundos se um arquivo de mapeamento for fornecido
4. Insere os dados em lotes no banco MySQL
5. Opcionalmente executa uma procedure para atualização de dados relacionados

## Ciclo de Execução Típico

1. Execute o script de extração para obter os dados da API BTG:
   ```bash
   python api_faas_rentabilidade.py --n-days 1
   ```

2. Isso criará uma pasta como "10.04" com o arquivo JSON "10_04_rentabilidade_btg.json"

3. Execute o script de processamento e carga para inserir os dados no banco:
   ```bash
   python insert_rentabilidade.py --json-dir /caminho/para/pasta/10.04 --auto
   ```

4. Os dados estarão disponíveis no banco de dados MySQL na tabela Rentabilidade_Fundos

## Modo Automático

Para uso em pipelines automáticos, você pode combinar as etapas usando o modo auto:

```bash
# Etapa 1: Extrair dados
python api_faas_rentabilidade.py --n-days 1 --output-dir