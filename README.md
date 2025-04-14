# Projeto BTG ETL

## 1. Estrutura do Projeto

O projeto agora está organizado da seguinte forma:

```
/btg_etl_project/
├── apis/                        # Diretório com scripts de APIs
│   ├── api_faas_portfolio.py    # Extração de dados de carteira
│   └── api_faas_rentabilidade.py # Extração de dados de rentabilidade
├── guides/                      # Diretório de documentação
│   ├── README_Carteira.md       # Documentação de carteira
│   └── README_Rentabilidade.md  # Documentação de rentabilidade
├── insert_db/                   # Diretório com scripts de inserção
│   ├── insert_carteira.py       # Processamento e carga de dados de carteira
│   └── insert_rentabilidade.py  # Processamento e carga de dados de rentabilidade
├── logs/                        # Diretório para logs
├── mappings/                    # Diretório com arquivos de mapeamento
│   ├── map_nmfundos.json        # Mapeamento de nomes de fundos
│   └── map_description.json     # Mapeamento de descrições
├── .env                         # Configurações do ambiente (não versionado)
├── .gitignore                   # Configuração de exclusão de arquivos do Git
├── orquestrador_btg.py          # NOVO: Orquestrador de fluxo ETL
├── requirements.txt             # ATUALIZADO: Dependências do projeto
└── revisao_final.md             # Documento de revisão do projeto
```

## 2. Melhorias Implementadas

### 2.1 Novo Orquestrador ETL

- Implementação de um script orquestrador que sequencia todas as etapas do fluxo ETL
- Suporte para processamento de carteira, rentabilidade ou ambos
- Parametrização flexível via linha de comando
- Sistema robusto de logging e tratamento de erros
- Compatibilidade com agendadores via códigos de saída
- Capacidade de identificar automaticamente diretórios de data

### 2.2 Atualizações nas Dependências

O arquivo `requirements.txt` foi atualizado para incluir:
- `pyarrow` para operações mais eficientes com o pandas
- Versões específicas e compatíveis de todas as bibliotecas

### 2.3 Documentação Aprimorada

- READMEs atualizados com instruções detalhadas para cada componente
- Novo guia específico para uso do orquestrador
- Exemplos de uso mais claros e práticos
- Seções de troubleshooting expandidas

## 3. Verificação Final de Qualidade

### 3.1 Scripts Python

✅ **api_faas_portfolio.py**
- Funcionalidade completa para extração de dados de carteira
- Organização em pastas por data (dd.mm)
- Limpeza automática do arquivo ZIP após extração
- Contagem de arquivos extraídos

✅ **api_faas_rentabilidade.py**
- Funcionalidade completa para extração de dados de rentabilidade
- Organização em pastas por data (dd.mm)
- Nomenclatura padronizada para arquivos JSON (dd_mm_rentabilidade_btg.json)

✅ **insert_carteira.py**
- Processamento completo de arquivos XLSX
- Mapeamento de nomes de fundos e descrições
- Inserção em lotes no banco de dados
- Execução de procedures de atualização

✅ **insert_rentabilidade.py**
- Capacidade de encontrar automaticamente o arquivo JSON mais recente
- Processamento completo de dados de rentabilidade
- Mapeamento de nomes de fundos
- Modo automático para operações sem intervenção

✅ **orquestrador_btg.py**
- Orquestração completa do fluxo ETL
- Interface de linha de comando intuitiva
- Logging detalhado para monitoramento
- Detecção automática de diretórios de dados

### 3.2 Arquivos de Configuração e Dados

✅ **map_nmfundos.json**
- Mapeamento completo para normalização de nomes de fundos

✅ **map_description.json**
- Mapeamento para normalização de descrições

✅ **requirements.txt**
- Lista atualizada de todas as dependências necessárias
- Versões específicas para garantir compatibilidade

### 3.3 Documentação

✅ **README_Carteira.md**
- Instruções completas para extração e processamento de dados de carteira
- Exemplos de uso claros e práticos
- Seção de troubleshooting detalhada

✅ **README_Rentabilidade.md**
- Instruções completas para extração e processamento de dados de rentabilidade
- Exemplos de uso claros e práticos
- Seção de troubleshooting detalhada

✅ **Guia do Orquestrador BTG ETL**
- Instruções detalhadas para uso do orquestrador
- Exemplos para todos os cenários de uso
- Guia para integração com agendadores

## 4. Recomendações Futuras

Para evolução futura do projeto, recomendo considerar:

1. **Notificações por e-mail ou Slack**: Implementar alertas automáticos em caso de falhas no processo ETL

2. **Dashboard de monitoramento**: Criar uma interface web simples para visualizar o status das execuções

3. **Migração para Docker**: Containerizar a aplicação para facilitar a implantação e garantir consistência de ambiente


## 5. Conclusão

O projeto BTG ETL agora possui um fluxo completo, documentado e orquestrado para extração, processamento e carga de dados de carteira e rentabilidade. A implementação do orquestrador simplifica significativamente a operação diária e reduz a necessidade de intervenção manual.

O sistema está pronto para uso em ambiente de produção, com capacidade para ser agendado e executado de forma automática.
