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


## 2. Recomendações Futuras

Para evolução futura do projeto, recomendo considerar:

1. **Notificações por e-mail ou Slack**: Implementar alertas automáticos em caso de falhas no processo ETL

2. **Dashboard de monitoramento**: Criar uma interface web simples para visualizar o status das execuções

3. **Migração para Docker**: Containerizar a aplicação para facilitar a implantação e garantir consistência de ambiente


## 3. Conclusão

O projeto BTG ETL agora possui um fluxo completo, documentado e orquestrado para extração, processamento e carga de dados de carteira e rentabilidade. A implementação do orquestrador simplifica significativamente a operação diária e reduz a necessidade de intervenção manual.

O sistema está pronto para uso em ambiente de produção, com capacidade para ser agendado e executado de forma automática.
