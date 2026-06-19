"""
No Bancario - Sistema Distribuido de Transacoes Bancarias

Cada no bancario gerencia uma particao de contas e participa do
protocolo Two-Phase Commit (2PC) como participante/cohort.

Controle de concorrencia: Strict Two-Phase Locking (S2PL)
- Fase de crescimento: locks sao adquiridos conforme necessario
- Fase de encolhimento: TODOS os locks sao liberados apenas no commit/abort
- Variante Strict: write locks mantidos ate o commit, prevenindo aborts em cascata
- Prevencao de deadlock: aquisicao de lock com timeout, abort em caso de timeout
"""

import os
import sys
import json
import socket
import threading
import logging
import time
import uuid
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, Tuple

# Adiciona o diretorio pai para importacoes
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from protocol import (
    send_message, recv_message, create_server_socket, make_message,
    MSG_PREPARE, MSG_VOTE_COMMIT, MSG_VOTE_ABORT,
    MSG_GLOBAL_COMMIT, MSG_GLOBAL_ABORT,
    MSG_NODE_BALANCE, MSG_NODE_LIST,
    MSG_RESPONSE, MSG_ERROR, MSG_ACK,
)

# --- Configuracao ---

LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, stream=sys.stdout)

LOCK_TIMEOUT = 5.0       # Tempo maximo (segundos) para aguardar um lock antes de abortar
PREPARE_HOLD_TIME = 30.0  # Tempo maximo (segundos) para manter o estado PREPARED


# --- Tipos de Lock ---

class LockType(Enum):
    READ = "READ"
    WRITE = "WRITE"


@dataclass
class LockEntry:
    """Representa um lock mantido sobre uma conta."""
    lock_type: LockType
    holders: Set[str] = field(default_factory=set)  # IDs das transacoes que detem o lock
    waiters: list = field(default_factory=list)       # (tx_id, lock_type, event) aguardando


# --- Estado da Transacao ---

class TxState(Enum):
    ACTIVE = "ACTIVE"
    PREPARED = "PREPARED"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"


@dataclass
class TransactionContext:
    """Contexto de uma transacao neste no."""
    tx_id: str
    state: TxState = TxState.ACTIVE
    locked_accounts: Dict[int, LockType] = field(default_factory=dict)
    pending_writes: Dict[int, float] = field(default_factory=dict)  # conta -> novo saldo
    operations: list = field(default_factory=list)  # Lista de operacoes para o WAL
    created_at: float = field(default_factory=time.time)


# --- No Bancario ---

class BankNode:
    """
    No bancario que gerencia uma particao de contas.

    Implementa Strict Two-Phase Locking (S2PL) para controle de concorrencia
    e participa do Two-Phase Commit (2PC) como cohort.
    """

    def __init__(self, node_id: str, host: str, port: int,
                 account_range: Tuple[int, int], initial_balance: float = 1000.0):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.account_start, self.account_end = account_range
        self.logger = logging.getLogger(f"BankNode-{node_id}")

        # Armazenamento de contas: account_id -> saldo
        self.accounts: Dict[int, float] = {}
        for acc_id in range(self.account_start, self.account_end + 1):
            self.accounts[acc_id] = initial_balance

        # Tabela de locks: account_id -> LockEntry
        self.lock_table: Dict[int, LockEntry] = {}
        self.lock_table_lock = threading.Lock()  # Mutex para a propria tabela de locks

        # Transacoes ativas: tx_id -> TransactionContext
        self.transactions: Dict[str, TransactionContext] = {}
        self.tx_lock = threading.Lock()

        # Write-Ahead Log
        self.wal: list = []
        self.wal_lock = threading.Lock()

        self.server_socket = None
        self.running = False

        self.logger.info(
            f"Inicializado com contas {self.account_start}-{self.account_end}, "
            f"saldo inicial: {initial_balance}"
        )

    # --- Gerenciador de Locks (S2PL) ---

    def acquire_lock(self, tx_id: str, account_id: int,
                     lock_type: LockType) -> bool:
        """
        Adquire um lock sobre uma conta para uma transacao.

        Regras do S2PL:
        - Multiplos locks READ podem coexistir (compartilhados)
        - Lock WRITE eh exclusivo
        - Se o lock nao puder ser adquirido dentro do timeout, retorna False
          (prevencao de deadlock)
        """
        deadline = time.time() + LOCK_TIMEOUT
        event = threading.Event()

        while time.time() < deadline:
            with self.lock_table_lock:
                if account_id not in self.lock_table:
                    self.lock_table[account_id] = LockEntry(lock_type, {tx_id})
                    self.logger.debug(
                        f"TX {tx_id[:8]}: lock {lock_type.value} adquirido na conta {account_id}"
                    )
                    return True

                entry = self.lock_table[account_id]

                # Verifica se esta transacao ja detem o lock
                if tx_id in entry.holders:
                    if lock_type == LockType.WRITE and entry.lock_type == LockType.READ:
                        # Upgrade de lock: READ -> WRITE
                        if len(entry.holders) == 1:
                            entry.lock_type = LockType.WRITE
                            self.logger.debug(
                                f"TX {tx_id[:8]}: upgrade para lock WRITE na conta {account_id}"
                            )
                            return True
                        # Nao pode fazer upgrade se outros detem locks de leitura
                    else:
                        return True  # Ja possui lock compativel

                # Verifica se pode conceder o lock
                if entry.lock_type == LockType.READ and lock_type == LockType.READ:
                    # Locks de leitura compartilhados
                    entry.holders.add(tx_id)
                    self.logger.debug(
                        f"TX {tx_id[:8]}: lock READ compartilhado adquirido na conta {account_id}"
                    )
                    return True

                # Precisa aguardar - adiciona na fila de espera
                if not any(w[0] == tx_id for w in entry.waiters):
                    entry.waiters.append((tx_id, lock_type, event))

            # Aguarda o lock ficar disponivel
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            event.wait(timeout=min(remaining, 0.5))
            event.clear()

        # Timeout - remove da fila de espera
        with self.lock_table_lock:
            if account_id in self.lock_table:
                entry = self.lock_table[account_id]
                entry.waiters = [w for w in entry.waiters if w[0] != tx_id]

        self.logger.warning(
            f"TX {tx_id[:8]}: timeout de lock na conta {account_id} "
            f"({lock_type.value}) - possivel deadlock"
        )
        return False

    def release_locks(self, tx_id: str) -> None:
        """
        Libera TODOS os locks mantidos por uma transacao.

        No S2PL, isso so acontece no momento do commit/abort.
        """
        with self.lock_table_lock:
            accounts_to_release = []
            for account_id, entry in self.lock_table.items():
                if tx_id in entry.holders:
                    accounts_to_release.append(account_id)

            for account_id in accounts_to_release:
                entry = self.lock_table[account_id]
                entry.holders.discard(tx_id)

                if not entry.holders:
                    # Lock livre - concede para o proximo da fila
                    if entry.waiters:
                        next_tx_id, next_lock_type, next_event = entry.waiters.pop(0)
                        entry.lock_type = next_lock_type
                        entry.holders = {next_tx_id}
                        next_event.set()
                    else:
                        del self.lock_table[account_id]

                self.logger.debug(
                    f"TX {tx_id[:8]}: lock liberado na conta {account_id}"
                )

    # --- Operacoes de Transacao ---

    def begin_transaction(self, tx_id: str) -> TransactionContext:
        """Inicia uma nova transacao neste no."""
        with self.tx_lock:
            ctx = TransactionContext(tx_id=tx_id)
            self.transactions[tx_id] = ctx
            self.logger.info(f"TX {tx_id[:8]}: BEGIN")
            return ctx

    def read_balance(self, tx_id: str, account_id: int) -> Optional[float]:
        """Le o saldo de uma conta dentro de uma transacao (adquire lock READ)."""
        if account_id not in self.accounts:
            return None

        ctx = self.transactions.get(tx_id)
        if not ctx:
            return None

        if not self.acquire_lock(tx_id, account_id, LockType.READ):
            return None

        ctx.locked_accounts[account_id] = LockType.READ

        # Retorna o valor da escrita pendente se existir, senao o saldo atual
        if account_id in ctx.pending_writes:
            return ctx.pending_writes[account_id]
        return self.accounts[account_id]

    def write_balance(self, tx_id: str, account_id: int,
                      new_balance: float) -> bool:
        """Escreve um novo saldo dentro de uma transacao (adquire lock WRITE)."""
        if account_id not in self.accounts:
            return False

        ctx = self.transactions.get(tx_id)
        if not ctx:
            return False

        if not self.acquire_lock(tx_id, account_id, LockType.WRITE):
            return False

        ctx.locked_accounts[account_id] = LockType.WRITE
        ctx.pending_writes[account_id] = new_balance
        ctx.operations.append({
            "op": "WRITE",
            "account": account_id,
            "old_value": self.accounts[account_id],
            "new_value": new_balance
        })
        self.logger.info(
            f"TX {tx_id[:8]}: WRITE conta {account_id} = {new_balance:.2f} (pendente)"
        )
        return True

    # --- Protocolo 2PC (Participante) ---

    def handle_prepare(self, tx_id: str, operations: list) -> bool:
        """
        2PC Fase 1: PREPARE

        Executa as operacoes tentativamente:
        1. Adquire locks (fase de crescimento do S2PL)
        2. Valida operacoes (ex.: saldo suficiente)
        3. Escreve no WAL
        4. Vota COMMIT ou ABORT
        """
        ctx = self.begin_transaction(tx_id)

        for op in operations:
            op_type = op["operation"]
            account_id = op["account"]

            if account_id not in self.accounts:
                self.logger.warning(
                    f"TX {tx_id[:8]}: conta {account_id} nao encontrada neste no"
                )
                self._abort_transaction(tx_id)
                return False

            if op_type == "DEPOSIT":
                balance = self.read_balance(tx_id, account_id)
                if balance is None:
                    self._abort_transaction(tx_id)
                    return False
                new_balance = balance + op["amount"]
                if not self.write_balance(tx_id, account_id, new_balance):
                    self._abort_transaction(tx_id)
                    return False

            elif op_type == "WITHDRAW":
                balance = self.read_balance(tx_id, account_id)
                if balance is None:
                    self._abort_transaction(tx_id)
                    return False
                new_balance = balance - op["amount"]
                if new_balance < 0:
                    self.logger.warning(
                        f"TX {tx_id[:8]}: saldo insuficiente na conta {account_id} "
                        f"(saldo: {balance:.2f}, solicitado: {op['amount']:.2f})"
                    )
                    self._abort_transaction(tx_id)
                    return False
                if not self.write_balance(tx_id, account_id, new_balance):
                    self._abort_transaction(tx_id)
                    return False

            elif op_type == "READ":
                balance = self.read_balance(tx_id, account_id)
                if balance is None:
                    self._abort_transaction(tx_id)
                    return False

        # Escreve no WAL
        self._wal_write(tx_id, "PREPARED", ctx.operations)

        ctx.state = TxState.PREPARED
        self.logger.info(f"TX {tx_id[:8]}: VOTE_COMMIT")
        return True

    def handle_commit(self, tx_id: str) -> bool:
        """
        2PC Fase 2: COMMIT

        Aplica todas as escritas pendentes e libera os locks
        (fase de encolhimento do S2PL).
        """
        with self.tx_lock:
            ctx = self.transactions.get(tx_id)
            if not ctx:
                self.logger.warning(f"TX {tx_id[:8]}: COMMIT para transacao desconhecida")
                return False

        # Aplica as escritas pendentes no armazenamento estavel
        for account_id, new_balance in ctx.pending_writes.items():
            old_balance = self.accounts[account_id]
            self.accounts[account_id] = new_balance
            self.logger.info(
                f"TX {tx_id[:8]}: COMMITTED conta {account_id}: "
                f"{old_balance:.2f} -> {new_balance:.2f}"
            )

        # WAL: marca como committed
        self._wal_write(tx_id, "COMMITTED", [])

        # Libera todos os locks (S2PL: somente no commit)
        self.release_locks(tx_id)

        ctx.state = TxState.COMMITTED
        with self.tx_lock:
            del self.transactions[tx_id]

        self.logger.info(f"TX {tx_id[:8]}: GLOBAL_COMMIT aplicado")
        return True

    def handle_abort(self, tx_id: str) -> bool:
        """
        2PC Fase 2: ABORT

        Descarta todas as escritas pendentes e libera os locks.
        """
        self._abort_transaction(tx_id)
        self.logger.info(f"TX {tx_id[:8]}: GLOBAL_ABORT aplicado")
        return True

    def _abort_transaction(self, tx_id: str) -> None:
        """Aborta uma transacao: descarta escritas pendentes e libera locks."""
        with self.tx_lock:
            ctx = self.transactions.get(tx_id)
            if not ctx:
                return
            ctx.state = TxState.ABORTED

        self._wal_write(tx_id, "ABORTED", [])
        self.release_locks(tx_id)

        with self.tx_lock:
            self.transactions.pop(tx_id, None)

    def _wal_write(self, tx_id: str, state: str, operations: list) -> None:
        """Escreve uma entrada no Write-Ahead Log."""
        with self.wal_lock:
            entry = {
                "timestamp": time.time(),
                "tx_id": tx_id,
                "state": state,
                "operations": operations
            }
            self.wal.append(entry)
            self.logger.debug(f"WAL: {tx_id[:8]} -> {state}")

    # --- Consultas diretas de saldo/listagem ---

    def get_balance(self, account_id: int) -> Optional[float]:
        """Retorna o saldo committed atual (sem contexto de transacao)."""
        return self.accounts.get(account_id)

    def get_all_accounts(self) -> Dict[int, float]:
        """Retorna todas as contas e seus saldos committed."""
        return dict(self.accounts)

    # --- Servidor de rede ---

    def start(self) -> None:
        """Inicia o servidor do no bancario."""
        self.running = True
        self.server_socket = create_server_socket(self.host, self.port)
        self.logger.info(f"No bancario {self.node_id} iniciado em {self.host}:{self.port}")

        # Inicia thread de limpeza para transacoes obsoletas
        cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        cleanup_thread.start()

        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                try:
                    client_sock, addr = self.server_socket.accept()
                except socket.timeout:
                    continue
                self.logger.debug(f"Conexao recebida de {addr}")
                handler = threading.Thread(
                    target=self._handle_connection,
                    args=(client_sock, addr),
                    daemon=True
                )
                handler.start()
            except OSError:
                break

    def stop(self) -> None:
        """Para o servidor do no bancario."""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        self.logger.info(f"No bancario {self.node_id} parado")

    def _handle_connection(self, sock: socket.socket, addr) -> None:
        """Trata uma conexao individual do coordenador."""
        try:
            while self.running:
                msg = recv_message(sock, timeout=60.0)
                if msg is None:
                    break
                response = self._process_message(msg)
                if response:
                    send_message(sock, response)
        except Exception as e:
            self.logger.error(f"Erro ao tratar conexao de {addr}: {e}")
        finally:
            sock.close()

    def _process_message(self, msg: dict) -> Optional[dict]:
        """Processa uma mensagem recebida e retorna uma resposta."""
        msg_type = msg.get("type")
        tx_id = msg.get("tx_id", "")

        if msg_type == MSG_PREPARE:
            operations = msg.get("operations", [])
            success = self.handle_prepare(tx_id, operations)
            if success:
                # Coleta resultados de saldo para operacoes READ
                ctx = self.transactions.get(tx_id)
                balances = {}
                if ctx:
                    for op in msg.get("operations", []):
                        if op["operation"] == "READ":
                            acc = op["account"]
                            if acc in ctx.pending_writes:
                                balances[str(acc)] = ctx.pending_writes[acc]
                            else:
                                balances[str(acc)] = self.accounts.get(acc, 0)
                return make_message(MSG_VOTE_COMMIT, tx_id=tx_id, balances=balances)
            else:
                return make_message(MSG_VOTE_ABORT, tx_id=tx_id,
                                   reason="Prepare falhou")

        elif msg_type == MSG_GLOBAL_COMMIT:
            self.handle_commit(tx_id)
            return make_message(MSG_ACK, tx_id=tx_id, status="COMMITTED")

        elif msg_type == MSG_GLOBAL_ABORT:
            self.handle_abort(tx_id)
            return make_message(MSG_ACK, tx_id=tx_id, status="ABORTED")

        elif msg_type == MSG_NODE_BALANCE:
            account_id = msg.get("account")
            balance = self.get_balance(account_id)
            if balance is not None:
                return make_message(MSG_RESPONSE, account=account_id,
                                   balance=balance)
            else:
                return make_message(MSG_ERROR,
                                   reason=f"Conta {account_id} nao encontrada")

        elif msg_type == MSG_NODE_LIST:
            accounts = self.get_all_accounts()
            return make_message(MSG_RESPONSE, accounts=accounts)

        else:
            self.logger.warning(f"Tipo de mensagem desconhecido: {msg_type}")
            return make_message(MSG_ERROR, reason=f"Tipo de mensagem desconhecido: {msg_type}")

    def _cleanup_loop(self) -> None:
        """Limpa periodicamente transacoes preparadas que ficaram obsoletas."""
        while self.running:
            time.sleep(10)
            now = time.time()
            stale = []
            with self.tx_lock:
                for tx_id, ctx in self.transactions.items():
                    if (ctx.state == TxState.PREPARED and
                            now - ctx.created_at > PREPARE_HOLD_TIME):
                        stale.append(tx_id)

            for tx_id in stale:
                self.logger.warning(
                    f"TX {tx_id[:8]}: transacao preparada obsoleta - abortando"
                )
                self._abort_transaction(tx_id)


# --- Ponto de entrada ---

def main():
    node_id = os.environ.get("NODE_ID", "A")
    host = os.environ.get("NODE_HOST", "0.0.0.0")
    port = int(os.environ.get("NODE_PORT", "6001"))

    # Faixa de contas a partir de variaveis de ambiente
    acc_start = int(os.environ.get("ACCOUNT_START", "1000"))
    acc_end = int(os.environ.get("ACCOUNT_END", "1004"))
    initial_balance = float(os.environ.get("INITIAL_BALANCE", "1000.0"))

    node = BankNode(
        node_id=node_id,
        host=host,
        port=port,
        account_range=(acc_start, acc_end),
        initial_balance=initial_balance
    )

    try:
        node.start()
    except KeyboardInterrupt:
        node.stop()


if __name__ == "__main__":
    main()
