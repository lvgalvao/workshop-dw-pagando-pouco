# Um DW pagando pouco

Bem-vindo ao workshop 03 que enfrenta o desafio empolgante de estruturar um ambiente analítico avançado para uma grande rede de supermercados. Com mais de 300 lojas físicas e uma presença robusta no e-commerce, esta rede busca uma solução de análise de dados que habilite a diretoria a tomar decisões informadas e estratégicas baseadas em dados reais e insights valiosos.

## Objetivos do Projeto

Nosso principal objetivo é fornecer uma visão holística e detalhada do desempenho de vendas, respondendo a questões cruciais como:

* **Análise de Receita Total**: Compreender a receita total gerada pela rede, um indicador vital para o sucesso do negócio.
* **Desempenho por Segmento**: Avaliar a receita gerada por cada loja individualmente e pelo canal digital, identificando áreas de força e oportunidades de crescimento.
* **Categorias de Produto**: Analisar a receita gerada por cada categoria de produto, oferecendo insights sobre preferências de consumo e tendências de mercado.
* **Impacto das Promoções**: Quantificar como as promoções afetam a receita total, essencial para estratégias de marketing eficazes.
* **Margem de Lucro**: Avaliar a lucratividade da rede, um indicador chave para a saúde financeira do negócio.

Além disso, visamos construir uma plataforma de dados que permita aos times de negócio desenvolver dashboards e relatórios personalizados, fomentando uma cultura data-driven e autossuficiente na empresa.

## Desafios e Considerações Econômicas

Este projeto não é apenas uma jornada técnica, mas também um exercício de eficiência de custos. A diretoria está comprometida com uma solução econômica que maximize o retorno sobre o investimento. Portanto, estamos focados em implementar uma estratégia que equilibre o desempenho, a escalabilidade e a acessibilidade financeira.

![Imagem](pic/happy.jpeg)

## Motivação

Você não precisa de PySpark, Databricks, Snowflake, Redshift, Fabric, etc. Você quer gerar valor, porém pagando pouco. 99% dos projetos não precisam dessas soluções e você sabe disso.

## Desafio

Neste projeto, vamos montar um DW com as seguintes características:

- Operação de uma grande rede de supermercados com 300 lojas e um Ecommerce
- Ingestão de dados com vendas das lojas físicas totalizando 1 milhão de linhas diárias
- Ingestão de dados via API com vendas do Ecommerce a cada hora
- Ingestão da dados via Excel com catalogo de produtos atualizado a cada 3 dias

## Estrutura do projeto

A estruturação de pastas em um projeto de Data Warehouse (DW) é crucial para manter o projeto organizado, escalável e gerenciável. Aqui está uma estrutura de pastas recomendada, considerando um projeto de DW que utiliza DuckDB para processamento e armazenamento de dados na AWS com Delta Lake, e que lida com várias fontes de dados como bancos de dados PostgreSQL, APIs e arquivos Excel:

```graphql
data-warehouse-project/
│
├── data/
│   ├── raw/              # Dados brutos extraídos das fontes
│   ├── interim/          # Dados em processo de transformação
│   └── processed/        # Dados transformados prontos para serem carregados no DW
│
├── notebooks/            # Jupyter notebooks para exploração e análise de dados
│
├── scripts/
│   ├── etl_scripts/      # Scripts para processos ETL
│   │   ├── extract/      # Scripts para extração de dados
│   │   ├── transform/    # Scripts para transformação de dados
│   │   └── load/         # Scripts para carregar dados no DW
│   └── utility_scripts/  # Scripts utilitários (ex.: conexão com APIs, funções auxiliares)
│
├── sql/
│   ├── queries/          # Consultas SQL para análises e relatórios
│   └── ddl/              # Scripts SQL para definição de esquema (DDL)
│
├── config/               # Arquivos de configuração (ex.: conexões de banco de dados, variáveis de ambiente)
│
├── logs/                 # Logs de execução dos scripts ETL
│
├── tests/                # Testes automatizados (ex.: testes de integridade de dados)
│
├── docker/               # Arquivos Docker para criar ambientes de execução consistentes
│
└── docs/                 # Documentação do projeto
    ├── data_dictionary/  # Dicionário de dados descrevendo as fontes de dados e modelos
    └── etl_documentation/ # Documentação dos processos ETL
```

### Explicação da Estrutura:

* **data/**: Armazena os dados em diferentes estágios do processo ETL. É importante manter uma separação clara entre dados brutos, dados em processo e dados processados.
* **notebooks/**: Para análises exploratórias e prototipagem rápida.
* **scripts/**: Contém os scripts ETL e scripts auxiliares.
* **sql/**: Armazena consultas e definições SQL.
* **config/**: Configurações gerais e sensíveis, como chaves de API, não devem ser versionadas.
* **logs/**: Para rastrear eventos e erros que ocorrem durante a execução dos scripts.
* **tests/**: Testes são essenciais para garantir a qualidade dos dados e a robustez do código.
* **docker/**: Facilita a criação de ambientes consistentes, especialmente útil em equipes grandes ou para implantações em produção.
* **docs/**: Documentação é vital para a manutenção e escalabilidade do projeto.

Esta estrutura pode ser ajustada conforme as necessidades específicas do seu projeto. É importante manter uma abordagem modular e flexível para acomodar mudanças e expansões futuras do projeto.

Literatura recomendada:

- [Big Data is Dead | MotherDuck](https://www.youtube.com/watch?v=lisIQ9ohU8g)
- [dbt-core and duckdb | Medium](https://medium.com/datamindedbe/use-dbt-and-duckdb-instead-of-spark-in-data-pipelines-9063a31ea2b5)

## AWS

Vamos utilizar o serviço S3 da AWS para armazenar os dados brutos e processados. Para isso, você precisa criar uma conta na AWS e configurar as credenciais de acesso no seu projeto local.

Para encontrar as informações necessárias, como `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` e o nome do seu bucket S3 na AWS (Amazon Web Services), você pode seguir estes passos:

### 1. **AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY**

Essas credenciais são parte das suas chaves de acesso da AWS. Para obtê-las, você deve criar ou usar uma chave de acesso existente no IAM (Identity and Access Management) da AWS. Veja como:

* Faça login no [Console de Gerenciamento da AWS](https://aws.amazon.com/console/).
* Navegue até o serviço IAM.
* No painel de navegação, escolha "Usuários".
* Escolha o seu usuário IAM (ou crie um novo usuário se necessário).
* Na aba "Credenciais de segurança", você pode criar uma nova chave de acesso. Ao criar uma nova chave, você receberá o `AWS_ACCESS_KEY_ID` e o `AWS_SECRET_ACCESS_KEY`. Lembre-se de baixar e guardar essas chaves em um lugar seguro, pois o `AWS_SECRET_ACCESS_KEY` não pode ser recuperado depois de criado.

### 2. **Nome do Bucket S3**

Para encontrar o nome do seu bucket S3:

* Faça login no [Console de Gerenciamento da AWS](https://aws.amazon.com/console/).
* Navegue até o serviço Amazon S3.
* Você verá uma lista de buckets S3. Procure pelo bucket que deseja usar e copie o nome exato.

### 3. **Configuração do Arquivo `.env`**

Após obter essas informações, você pode configurar o arquivo `.env` no seu projeto local da seguinte forma:

```makefile
AWS_ACCESS_KEY_ID=sua_access_key_id
AWS_SECRET_ACCESS_KEY=sua_secret_key
S3_BUCKET_NAME=nome_do_seu_bucket
```

### Notas de Segurança:

* **Nunca** compartilhe suas chaves de acesso (`AWS_ACCESS_KEY_ID` e `AWS_SECRET_ACCESS_KEY`) publicamente ou as inclua no controle de versão do seu código (como em repositórios Git).
* Considere atribuir políticas de permissão mínimas necessárias ao usuário IAM para aumentar a segurança. Por exemplo, se o usuário só precisa acessar determinados buckets S3, configure suas permissões para limitar o acesso apenas a esses recursos.
* Regularmente revise e gire suas chaves de acesso para manter a segurança da sua conta AWS.