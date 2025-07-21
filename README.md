# Carijó - Rinha de Backend 2025

Este repositório contém a minha implementação para a [3ª edição da Rinha de Backend](https://github.com/zanfranceschi/rinha-de-backend-2025).

O projeto consiste em um intermediário de pagamentos que se comunica com dois serviços de processamento de pagamentos (um principal e um de fallback), buscando sempre a menor taxa e lidando com a instabilidade dos serviços.

## Tecnologias Utilizadas

- **Linguagem:** [Go](https://go.dev/)
- **Framework Web:** [Fiber](https://gofiber.io/)
- **Banco de Dados:** [Redis](https://redis.io/) para armazenamento de filas e dados temporários.
- **Load Balancer:** [Nginx](https://www.nginx.com/) para distribuir a carga entre as duas instâncias da aplicação.
- **Conteinerização:** [Docker](https://www.docker.com/) e [Docker Compose](https://docs.docker.com/compose/).

## Arquitetura

A solução é composta por:

- 2 instâncias da aplicação Go (`white-carijo` e `black-carijo`) que recebem as requisições de pagamento.
- 1 instância do Nginx (`balancer`) que atua como load balancer.
- 1 instância do Redis (`storage`) que funciona como fila para processamento assíncrono dos pagamentos.

Um dos serviços Go (`white-carijo`) também é responsável por monitorar a saúde dos processadores de pagamento, enquanto o outro (`black-carijo`) executa uma tarefa de limpeza (janitor) para pagamentos que possam ter falhado.

## Como Executar

1.  **Suba os processadores de pagamento:**
    Antes de iniciar a aplicação, é necessário ter os serviços de processamento de pagamento da rinha rodando. Siga as instruções no [repositório oficial da Rinha](https://github.com/zanfranceschi/rinha-de-backend-2025/tree/main/payment-processor).

2.  **Inicie a aplicação:**
    Com os processadores de pagamento no ar, execute o seguinte comando na raiz deste projeto:

    ```bash
    docker-compose up -d
    ```

3.  **Execute o teste de carga:**
    ```bash
    k6 run challenge/rinha-test/rinha.js
    ```

## Detalhes do Desafio

Para mais detalhes sobre o desafio, regras e endpoints, consulte o arquivo [INSTRUCOES.md](challenge/INSTRUCOES.md) no diretório `challenge`.
