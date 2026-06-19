# 🏦 Sistema Distribuído de Transações Bancárias

**INE5418 - Computação Distribuída — T2: Building Blocks**

> **Building Block:** Transações Distribuídas  
> **Algoritmo:** Two-Phase Commit (2PC) + Strict Two-Phase Locking (S2PL)  
> **Linguagem:** Python 3.11  
> **Comunicação:** Berkeley Sockets (TCP)

---

## 📋 Índice

- [Visão Geral](#-visão-geral)
- [Arquitetura](#-arquitetura)
- [Algoritmos Implementados](#-algoritmos-implementados)
- [Como Executar](#-como-executar)
- [Comandos do Cliente](#-comandos-do-cliente)
- [Cenários de Demonstração](#-cenários-de-demonstração)
- [Estrutura do Projeto](#-estrutura-do-projeto)

---

## 🔍 Visão Geral

Sistema bancário distribuído que implementa **transações distribuídas** como building block principal. A aplicação permite operações bancárias (depósito, saque, transferência) entre contas distribuídas em múltiplos nós, garantindo **atomicidade** e **isolamento** das transações.

O sistema utiliza:
- **Two-Phase Commit (2PC)** para garantir atomicidade de transações que envolvem múltiplos nós
- **Strict Two-Phase Locking (S2PL)** para controle de concorrência, garantindo serializabilidade

---

## 🏗️ Arquitetura

```
                    ┌─────────────┐
                    │  CLI Client  │
                    └──────┬──────┘
                           │ TCP :5000
                    ┌──────▼──────┐
                    │ Coordinator  │
                    │   (2PC)      │
                    └──┬───┬───┬──┘
                       │   │   │
            TCP :6001  │   │   │  TCP :6003
          ┌────────────┘   │   └────────────┐
          │          TCP :6002               │
   ┌──────▼──────┐ ┌──────▼──────┐ ┌───────▼─────┐
   │ Bank Node A  │ │ Bank Node B  │ │ Bank Node C  │
   │ Contas:      │ │ Contas:      │ │ Contas:      │
   │ 1000-1004    │ │ 2000-2004    │ │ 3000-3004    │
   │ (S2PL)       │ │ (S2PL)       │ │ (S2PL)       │
   └──────────────┘ └──────────────┘ └──────────────┘
```

### Componentes

| Componente | Descrição | Porta |
|------------|-----------|-------|
| **Coordinator** | Coordenador de transações. Recebe requisições dos clientes e executa o protocolo 2PC com os nós participantes. | 5000 |
| **Bank Node A** | Nó bancário gerenciando contas 1000-1004. Implementa S2PL e participa do 2PC. | 6001 |
| **Bank Node B** | Nó bancário gerenciando contas 2000-2004. Implementa S2PL e participa do 2PC. | 6002 |
| **Bank Node C** | Nó bancário gerenciando contas 3000-3004. Implementa S2PL e participa do 2PC. | 6003 |
| **CLI Client** | Cliente interativo de linha de comando. | — |

---

## ⚙️ Algoritmos Implementados

### Two-Phase Commit (2PC)

Garante a **atomicidade** de transações distribuídas (tudo ou nada):

**Fase 1 — Votação (PREPARE):**
1. O coordenador envia `PREPARE` + operações para cada nó participante
2. Cada nó adquire locks (S2PL), valida as operações e escreve no WAL
3. Cada nó responde com `VOTE_COMMIT` ou `VOTE_ABORT`

**Fase 2 — Decisão:**
- Se **todos** votaram `COMMIT` → coordenador envia `GLOBAL_COMMIT`
- Se **algum** votou `ABORT` → coordenador envia `GLOBAL_ABORT`

### Strict Two-Phase Locking (S2PL)

Garante **serializabilidade** e **isolamento** das transações concorrentes:

- **Fase de crescimento:** locks são adquiridos conforme necessário
- **Fase de encolhimento:** **todos** os locks são liberados somente no commit/abort
- **Strict:** write locks são mantidos até o commit → previne aborts em cascata
- **Prevenção de deadlock:** timeout na aquisição de locks → transação é abortada

### Protocolo de Comunicação

Mensagens JSON sobre TCP com framing por prefixo de tamanho (4 bytes big-endian):

```
┌──────────────┬──────────────────────────┐
│ 4 bytes: len │ N bytes: JSON payload    │
└──────────────┴──────────────────────────┘
```

---

## 🚀 Como Executar

### Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) e [Docker Compose](https://docs.docker.com/compose/install/) instalados

### 1. Iniciar o Sistema

```bash
# Construir as imagens e iniciar todos os containers em background
docker compose up --build -d
```

Isso irá iniciar:
- 3 Bank Nodes (A, B, C)
- 1 Transaction Coordinator
- Todos na mesma rede Docker

### 2. Acessar o Cliente Interativo

```bash
# Iniciar o cliente interativo (conecta automaticamente ao coordenador)
docker compose run --rm client
```

### 3. Parar o Sistema

```bash
# Parar e remover todos os containers
docker compose down
```

### 4. Visualizar Logs

```bash
# Ver logs de todos os serviços
docker compose logs -f

# Ver logs de um serviço específico
docker compose logs -f coordinator
docker compose logs -f bank-node-a
```

---

## 💻 Comandos do Cliente

```
balance <conta>                   Consultar saldo de uma conta
deposit <conta> <valor>           Depositar dinheiro em uma conta
withdraw <conta> <valor>          Sacar dinheiro de uma conta
transfer <origem> <destino> <valor>   Transferir entre contas
list                              Listar todas as contas e saldos
stress [threads] [tx_por_thread]  Teste de stress com transações concorrentes
help                              Mostrar ajuda
quit                              Sair do cliente
```

### Exemplos

```bash
# Consultar saldo
bank> balance 1000

# Depositar R$500 na conta 1000
bank> deposit 1000 500

# Transferência entre contas no mesmo nó (Node A)
bank> transfer 1000 1001 200

# Transferência cross-node (Node A → Node B) — usa 2PC!
bank> transfer 1000 2000 300

# Transferência cross-node (Node A → Node C) — usa 2PC!
bank> transfer 1001 3000 150

# Listar todas as contas
bank> list

# Teste de stress: 5 threads, 10 transações cada
bank> stress 5 10
```

---

## 🎯 Cenários de Demonstração

### Cenário 1: Execução Normal

```bash
bank> list                        # Ver saldos iniciais (1000 cada)
bank> transfer 1000 2000 200      # Transferência cross-node
bank> balance 1000                # Deve ser 800
bank> balance 2000                # Deve ser 1200
```

### Cenário 2: Concorrência (Stress Test)

```bash
bank> stress 5 10                 # 5 threads × 10 transações = 50 transações simultâneas
```

O teste de stress:
- Lança múltiplas threads fazendo transferências simultâneas
- Demonstra que o S2PL serializa transações conflitantes
- Verifica **conservação de dinheiro** (soma total não muda)
- Mostra transações abortadas por timeout de lock (prevenção de deadlock)

### Cenário 3: Saldo Insuficiente

```bash
bank> withdraw 1000 5000          # Saldo insuficiente → nó vota ABORT → 2PC aborta
```

### Cenário 4: Falha de Nó

```bash
# Em outro terminal, parar o Bank Node B
docker compose stop bank-node-b

# No cliente, tentar transferência para Node B
bank> transfer 1000 2000 100      # Coordenador não consegue conectar → ABORT

# Reiniciar o nó
docker compose start bank-node-b
```

---

## 📁 Estrutura do Projeto

```
INE5418_T2_Building_Blocks/
├── docker-compose.yml        # Orquestração dos containers
├── Dockerfile                # Imagem Docker (Python 3.11 slim)
├── README.md                 # Este arquivo
├── T2 - Building Blocks.pdf  # Enunciado do trabalho
└── src/
    ├── __init__.py
    ├── protocol.py           # Protocolo de comunicação (TCP + JSON)
    ├── bank_node.py          # Nó bancário (S2PL + participante 2PC)
    ├── coordinator.py        # Coordenador de transações (2PC)
    └── client.py             # Cliente interativo (CLI)
```

---

## 🔧 Detalhes Técnicos

### Comunicação via Berkeley Sockets

Toda a comunicação utiliza sockets TCP (módulo `socket` do Python), que implementa a API Berkeley Sockets:

- `socket()` → cria o socket
- `bind()` → associa a um endereço
- `listen()` → aceita conexões
- `accept()` → aceita conexão de cliente
- `connect()` → conecta ao servidor
- `send()`/`recv()` → envia/recebe dados
- `close()` → fecha o socket

### Write-Ahead Log (WAL)

Cada nó mantém um log de transações (em memória) para fins de recuperação:
- `PREPARED` — transação pronta para commit
- `COMMITTED` — transação efetivada
- `ABORTED` — transação cancelada

### Prevenção de Deadlock

O sistema usa **timeout** na aquisição de locks para prevenir deadlocks:
- Se um lock não pode ser adquirido em 5 segundos, a transação é abortada
- Isso garante que o sistema não trava em cenários de concorrência