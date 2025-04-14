# Sistema de Carteira Diária BTG

## Visão Geral

Este sistema é responsável pela extração, processamento e armazenamento de dados de carteiras diárias da API BTG FaaS. O fluxo completo é dividido em duas etapas principais:

1. **Extração de dados** (`api_faas_portfolio.py`): Conecta à API do BTG, obtém relatórios de carteira diária e salva os arquivos XLSX em um diretório organizado por data.
2. **Processamento e carga** (`insert_carteira.py`): Processa os arquivos XLSX baixados, padroniza os dados e os insere no banco de dados MySQL.

## Arquivos do Sistema

- `api_faas_portfolio.py` - Script para extração de dados da API BTG
- `insert_carteira.py` - Script para processamento e carga dos dados no banco
- `map_nmfundos.json` - Arquivo de mapeamento para padronização de nomes de fundos
- `map_description.json` - Arquivo de mapeamento para padronização de descrições

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
   PORTFOLIO_URL=https://funds.btgpactual.com/reports/Portfolio
   TICKET_URL=https://funds.btgpactual.com/reports/Ticket
   CLIENT_ID=seu_cliente_id
   CLIENT_SECRET=sua_senha
   SCOPE_CARTEIRA=reports.portfolio

   # Configurações do banco de dados MySQL
   DB_HOST=seu_host
   DB_USER=seu_usuario
   DB_PASSWORD=sua_senha
   DB_NAME=seu_banco
   DB_TABLE=Ft_CarteiraDiaria

   # Caminhos para armazenamento
   BTG_REPORT_PATH=caminho/para/armazenar/relatorios
   ```

## Guia de Uso - Extração de Dados (api_faas_portfolio.py)

Este script conecta à API do BTG, solicita relatórios de carteira diária, baixa os arquivos ZIP, e extrai os arquivos XLSX em pastas organizadas por data.

### Opções disponíveis:

```
--n-days N        : Solicita dados de N dias úteis atrás
--date YYYY-MM-DD : Solicita dados de uma data específica
--output-dir DIR  : Especifica um diretório personalizado para salvar os arquivos
```

### Exemplos de uso:

```bash
# Obter dados do dia útil anterior e salvá-los na pasta padrão
python api_faas_portfolio.py --n-days 1

# Obter dados de uma data específica
python api_faas_portfolio.py --date 2025-04-10

# Obter dados e salvá-los em um diretório personalizado
python api_faas_portfolio.py --n-days 1 --output-dir /dados/relatorios/btg
```

### Funcionamento:

1. O script obtém um token de autenticação da API BTG
2. Solicita um ticket para o relatório da data especificada
3. Faz polling para aguardar o processamento do relatório
4. Baixa o arquivo ZIP para o diretório base
5. Extrai os arquivos XLSX em uma subpasta com formato "dd.mm" (ex: "10.04")
6. Remove o arquivo ZIP após a extração
7. Informa o total de arquivos extraídos

## Guia de Uso - Processamento e Carga (insert_carteira.py)

Este script processa os arquivos XLSX extraídos, estrutura os dados e os insere no banco de dados MySQL.

### Opções disponíveis:

```
input_directory    : Diretório contendo os arquivos XLSX a serem processados
--no-mapping       : Desativa o mapeamento de nomes de fundos (opcional)
```

### Exemplo de uso:

```bash
# Processar arquivos na pasta especificada
python insert_carteira.py /caminho/para/pasta/10.04

# Processar arquivos sem aplicar mapeamento de nomes
python insert_carteira.py /caminho/para/pasta/10.04 --no-mapping
```

### Funcionamento:

1. O script lê todos os arquivos XLSX no diretório especificado
2. Processa cada arquivo, extraindo informações como:
   - Portfolio Investido
   - Títulos Públicos
   - Títulos Privados
   - Ações
   - Despesas
   - Saldo de Caixa
3. Padroniza os nomes dos fundos usando o arquivo `map_nmfundos.json`
4. Estrutura os dados no formato esperado pelo banco de dados
5. Insere os dados em lotes no banco MySQL
6. Executa uma procedure para atualizar os CNPJs dos fundos

## Ciclo de Execução Típico

1. Execute o script de extração para obter os dados da API BTG:
   ```bash
   python api_faas_portfolio.py --n-days 1
   ```

2. Isso criará uma pasta como "10.04" com os arquivos XLSX extraídos

3. Execute o script de processamento e carga para inserir os dados no banco:
   ```bash
   python insert_carteira.py /caminho/para/pasta/10.04
   ```

4. Os dados estarão disponíveis no banco de dados MySQL na tabela Ft_CarteiraDiaria

## Troubleshooting

### Problemas comuns na extração:

- **Erro de autenticação**: Verifique as credenciais CLIENT_ID e CLIENT_SECRET
- **Timeout na obtenção do relatório**: Aumente o número de tentativas no código
- **Arquivos XLSX não encontrados**: Verifique se o ZIP foi extraído corretamente

### Problemas comuns no processamento:

- **Erro ao mapear nomes de fundos**: Verifique se o arquivo map_nmfundos.json está correto
- **Falha ao inserir no banco**: Verifique as credenciais do banco e a estrutura da tabela
- **Fundos não processados**: Verifique se o formato dos arquivos XLSX está de acordo com o esperado

## Logs

Os logs são exibidos no console durante a execução dos scripts. Para salvar logs em arquivo, você pode redirecionar a saída:

```bash
python api_faas_portfolio.py --n-days 1 > log_extracao.txt 2>&1
python insert_carteira.py /caminho/para/pasta/10.04 > log_processamento.txt 2>&1
```