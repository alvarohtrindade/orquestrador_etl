# Guia do Orquestrador BTG ETL

Este guia explica como usar o orquestrador ETL para automatizar o fluxo de extração, transformação e carga dos dados BTG.

## Visão Geral

O orquestrador (`orquestrador_btg.py`) é um script Python que simplifica a execução do fluxo ETL completo, orquestrando automaticamente:

1. Extração de dados da API BTG (carteira e/ou rentabilidade)
2. Processamento dos dados extraídos
3. Carga dos dados no banco de dados MySQL

O orquestrador gerencia toda a sequência de operações, encontra os diretórios corretos e lida com possíveis erros, tornando o processo mais robusto e fácil de executar.

## Pré-requisitos

- Python 3.8+ instalado
- Todas as dependências do projeto instaladas
- Arquivo `.env` configurado corretamente
- Permissões de execução adequadas para os scripts

## Instalação

1. Certifique-se de que todos os scripts ETL estão no mesmo diretório:
   - `api_faas_portfolio.py`
   - `api_faas_rentabilidade.py`
   - `insert_carteira.py`
   - `insert_rentabilidade.py`
   - `orquestrador_btg.py`

2. Crie um diretório `logs` na mesma pasta (ou ele será criado automaticamente):
   ```bash
   mkdir logs
   ```

## Uso Básico

Execute o orquestrador em modo padrão para processar carteira e rentabilidade do dia útil anterior:

```bash
python orquestrador_btg.py
```

Isso executará o fluxo completo:
- Extrair dados de carteira e rentabilidade de 1 dia útil atrás
- Processar os dados automaticamente
- Carregar os dados no banco MySQL

## Opções Disponíveis

O orquestrador aceita diversos parâmetros para personalizar a execução:

```
--tipo {carteira,rentabilidade,ambos}
                       Tipo de dado a processar (padrão: ambos)
--data YYYY-MM-DD      Data específica no formato YYYY-MM-DD
--dias-atras N         Número de dias úteis no passado (padrão: 1)
--dir-carteira DIR     Diretório personalizado para carteira
--dir-rentabilidade DIR  Diretório personalizado para rentabilidade
```

## Exemplos de Uso

### Processar apenas dados de carteira:

```bash
python orquestrador_btg.py --tipo carteira
```

### Processar apenas dados de rentabilidade:

```bash
python orquestrador_btg.py --tipo rentabilidade
```

### Processar dados de uma data específica:

```bash
python orquestrador_btg.py --data 2025-04-10
```

### Processar dados de 3 dias atrás:

```bash
python orquestrador_btg.py --dias-atras 3
```

### Usar diretórios personalizados:

```bash
python orquestrador_btg.py --dir-carteira /dados/btg/carteira --dir-rentabilidade /dados/btg/rentabilidade
```

### Combinar múltiplas opções:

```bash
python orquestrador_btg.py --tipo rentabilidade --data 2025-04-05 --dir-rentabilidade /dados/rentabilidade
```

## Logs e Monitoramento

O orquestrador gera logs detalhados que são:
- Exibidos no console durante a execução
- Salvos no diretório `logs` com nome `orquestrador_btg_YYYYMMDD.log`

Os logs incluem informações sobre:
- Início e fim de cada etapa do processo
- Comandos executados e seus resultados
- Erros encontrados e seus detalhes
- Status final da execução (SUCESSO ou FALHA)

## Integração com Agendadores

O orquestrador retorna códigos de saída compatíveis com agendadores:
- `0` para execução com sucesso
- `1` para execução com falha

Isso permite a integração com ferramentas como cron, Windows Task Scheduler ou outras ferramentas de agendamento.

### Exemplo de configuração no cron (Linux/Mac):

Para executar o orquestrador todos os dias úteis (segunda a sexta) às
8:30 da manhã:

```
30 8 * * 1-5 cd /caminho/para/scripts && python orquestrador_btg.py >> logs/cron_$(date +\%Y\%m\%d).log 2>&1
```

## Troubleshooting

### Problema: O orquestrador não encontra a pasta com os dados extraídos

**Solução**: Certifique-se de que os diretórios definidos em BTG_REPORT_PATH e BTG_RENTABILIDADE no arquivo .env estão corretos e acessíveis.

### Problema: Erro ao executar scripts individuais

**Solução**: Verifique se todos os scripts podem ser executados individualmente antes de usar o orquestrador.

### Problema: Problemas de permissão

**Solução**: Certifique-se de que o usuário que executa o orquestrador tem permissões para ler/escrever em todos os diretórios necessários.

### Problema: Falha na conexão com o banco de dados

**Solução**: Confirme que as credenciais do banco no arquivo .env estão corretas e que o banco está acessível.
