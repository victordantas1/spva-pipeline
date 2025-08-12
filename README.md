# SPVA-Pipeline

## Descrição do Projeto

O **SPVA-Pipeline** é um pipeline de dados projetado para enriquecer e preparar dados de vagas de emprego e currículos para o ecossistema do portal de empregos SPVA. Utilizando uma arquitetura moderna de dados, o pipeline captura alterações em tempo real do banco de dados MySQL com o Debezium, processa esses eventos com Python e os carrega em um banco de dados MongoDB para consumo por outros serviços, como o **SPVA-Recommender**.

## Funcionalidades do Pipeline

O pipeline é orquestrado através de uma série de serviços conteinerizados e scripts:

1.  **Captura de Dados de Mudança (CDC):**

      - O Debezium monitora o banco de dados `mysql-spva` em busca de quaisquer inserções, atualizações ou exclusões nas tabelas.
      - Quando uma alteração é detectada, o Debezium publica um evento detalhado em um tópico do Apache Kafka.

2.  **Processamento e Enriquecimento:**

      - Um consumidor Kafka, implementado em `src/main.py`, escuta os tópicos do Kafka para receber os eventos de alteração de dados.
      - Para dados de candidatos, o pipeline acessa o Minio para buscar e extrair o texto de currículos em PDF.
      - Os dados passam por uma série de transformações, incluindo a limpeza de texto e a consolidação de vários campos em um único campo `document`, otimizado para buscas e processamento de linguagem natural.

3.  **Carregamento de Dados:**

      - Os dados processados e enriquecidos são carregados (com `upsert`) no MongoDB, garantindo que a base de dados de destino esteja sempre sincronizada com a fonte.

## Tecnologias Utilizadas

  - **Linguagem:** Python
  - **Mensageria:** Apache Kafka
  - **Captura de Dados de Mudança (CDC):** Debezium
  - **Manipulação de Dados:** Pandas
  - **Extração de PDF:** PyPDF2
  - **Armazenamento de Objetos:** Minio
  - **Bancos de Dados:**
      - MySQL (Fonte de dados)
      - MongoDB (Destino dos dados processados)
  - **Conteinerização:** Docker, Docker Compose

## Como Executar o Projeto

### Modo 1: Executando com Docker (Recomendado)

Este modo orquestra todos os serviços necessários (aplicação, bancos de dados, Kafka, Debezium, etc.) em contêineres.

**Pré-requisitos:**

  - Docker
  - Docker Compose

**Passos:**

1.  **Clonar o repositório:**

    ```bash
    git clone <URL_DO_REPOSITORIO>
    cd spva-pipeline
    ```

2.  **Configurar variáveis de ambiente:**

      - Crie um arquivo `.env` a partir do template fornecido no projeto.
        ```bash
        cp .env.template .env
        ```
      - Edite o arquivo `.env` com as credenciais e endpoints para as suas instâncias do Minio, MySQL e MongoDB.

3.  **Iniciar os serviços com Docker Compose:**

    ```bash
    docker-compose up -d --build
    ```

    Este comando irá construir as imagens e iniciar todos os contêineres. Aguarde alguns instantes para que todos os serviços (especialmente Kafka e Debezium Connect) estejam totalmente operacionais.

4.  **Registrar o Conector do Debezium:**
    Após os contêineres estarem em execução, execute o seguinte comando no seu terminal para registrar o conector do MySQL. Isso instrui o Debezium a começar a monitorar o banco de dados.

    ```bash
    curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d "@debezium/debezium_mysql_connector.json"
    ```

### Modo 2: Cenário para Desenvolvedores (Execução Local)

Este guia descreve como configurar e executar o pipeline em um ambiente de desenvolvimento local.

**Pré-requisitos:**

  - Python 3.10 ou superior
  - Acesso a instâncias do MySQL, Minio, MongoDB e Kafka.

**Passos:**

1.  **Clonar o repositório:**

    ```bash
    git clone <URL_DO_REPOSITORIO>
    cd spva-pipeline
    ```

2.  **Criar um ambiente virtual (recomendado):**

    ```bash
    python -m venv venv
    source venv/bin/activate  # No Windows, use `venv\Scripts\activate`
    ```

3.  **Instalar as dependências:**

    ```bash
    pip install -r requirements.txt
    ```

4.  **Configurar variáveis de ambiente:**

      - Crie um arquivo `.env` a partir do template fornecido.
        ```bash
        cp .env.template .env
        ```
      - Edite o arquivo `.env` com as credenciais e endpoints para as suas instâncias locais do Minio, MySQL, MongoDB e Kafka.

5.  **Executar o Pipeline:**
    Com o ambiente configurado, execute o script principal:

    ```bash
    python src/main.py
    ```

    Este comando iniciará o consumidor Kafka, que ficará escutando por mensagens e processando os dados em tempo real.