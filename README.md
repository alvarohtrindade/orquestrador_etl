# Autores 
Última data de modificação: 2025-04-07
Autores: Álvaro Henrique Trindade e Cesar Godoy


# BTG Portfolio Extractor

Este projeto é responsável por extrair, processar e armazenar dados de carteiras diárias da API BTG FaaS.

## Funcionalidades

- Autenticação na API BTG FaaS
- Download de relatórios de carteira diária
- Processamento de arquivos XLSX
- Estruturação dos dados em formato padronizado
- Armazenamento no banco de dados MySQL

## Requisitos

- Python 3.8+
- Bibliotecas listadas em `requirements.txt`
- Acesso à API BTG FaaS
- Banco de dados MySQL

## Instalação

1. Clone o repositório:
   ```bash
   git clone https://github.com/sua-empresa/btg-portfolio-extractor.git
   cd btg-portfolio-extractor
   ```

2. Crie e ative um ambiente virtual:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # ou
   venv\Scripts\activate  # Windows
   ```

3. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```

4. Configure as variáveis de ambiente:
   ```bash
   cp .env.example .env
   # Edite o arquivo .env com suas configurações
   ```

## Uso

### Linha de Comando

Extrair dados do dia útil anterior:
```bash
python api_btg_faas.py
```

Extrair dados de N dias úteis atrás:
```bash
python api_btg_faas.py --n-days 2
```

Extrair dados de uma data específica:
```bash
python api_btg_faas.py --date 2025-04-03
```


## Formato dos Dados

O extrator processa os arquivos XLSX e estrutura os dados no seguinte formato:

| Campo               | Descrição                                 |
|---------------------|-------------------------------------------|
| DocFundo            | CNPJ/Documento do fundo                   |
| NmFundo             | Nome do fundo                             |
| DtReferencia        | Data de referência no formato YYYY-MM-DD  |
| Grupo               | Seção principal (Resumo, Rentabilidade, etc) |
| Categoria           | Tipo de informação                        |
| Descricao           | Nome do item                              |
| Valor               | Valor financeiro                          |
| Quant               | Quantidade (para Portfolio_Investido)     |

## Logs

Os logs são armazenados no arquivo `btg_extractor.log` e também exibidos no console durante a execução.

## Contribuição

1. Faça um fork do projeto
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Faça commit das suas alterações (`git commit -am 'Adiciona nova feature'`)
4. Faça push para a branch (`git push origin feature/nova-feature`)
5. Abra um Pull Request