# Projeto BTG ETL - Grupo Catálise

## 1. Estrutura do Projeto

O projeto está organizado da seguinte forma:

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
└── README.md             # Documento de revisão do projeto
```

