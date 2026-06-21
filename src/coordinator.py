import os
import sys
import socket
import threading
import logging
import time
import uuid
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from protocol import (
    send_message, recv_message, create_server_socket, connect_to_server,
    make_message,
    MSG_BALANCE, MSG_DEPOSIT, MSG_WITHDRAW, MSG_TRANSFER, MSG_LIST_ACCOUNTS,
    MSG_PREPARE, MSG_VOTE_COMMIT, MSG_VOTE_ABORT,
    MSG_GLOBAL_COMMIT, MSG_GLOBAL_ABORT,
    MSG_NODE_BALANCE, MSG_NODE_LIST,
    MSG_RESPONSE, MSG_ERROR,
)

# --- Configuracao ---

LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, stream=sys.stdout)

NODE_TIMEOUT = 10.0  # Tempo maximo (segundos) para aguardar resposta dos nos


# --- Registro de Nos ---

class NodeInfo:
    # Informacoes sobre um no bancario.
    def __init__(self, node_id: str, host: str, port: int,
                 account_start: int, account_end: int):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.account_start = account_start
        self.account_end = account_end

    def owns_account(self, account_id: int) -> bool:
        return self.account_start <= account_id <= self.account_end


# --- Coordenador de Transacoes ---

class TransactionCoordinator:
    # Coordena transacoes distribuidas utilizando o protocolo 2PC.
    # 
    # Recebe requisicoes de clientes, determina os nos participantes
    # e garante a atomicidade de transacoes entre nos.

    def __init__(self, host: str, port: int, nodes: List[NodeInfo]):
        self.host = host
        self.port = port
        self.nodes = nodes
        self.logger = logging.getLogger("Coordinator")
        self.server_socket = None
        self.running = False

        # Log de transacoes para recuperacao
        self.tx_log: Dict[str, dict] = {}
        self.tx_log_lock = threading.Lock()

    def find_node(self, account_id: int) -> Optional[NodeInfo]:
        # Encontra qual no eh responsavel por uma determinada conta.
        for node in self.nodes:
            if node.owns_account(account_id):
                return node
        return None

    def connect_to_node(self, node: NodeInfo) -> Optional[socket.socket]:
        # Estabelece uma conexao com um no bancario.
        try:
            sock = connect_to_server(node.host, node.port, timeout=NODE_TIMEOUT)
            return sock
        except (ConnectionRefusedError, socket.timeout, OSError) as e:
            self.logger.error(
                f"Nao foi possivel conectar ao no {node.node_id} "
                f"({node.host}:{node.port}): {e}"
            )
            return None

    # --- Protocolo 2PC ---

    def execute_transaction(self, operations_by_node: Dict[str, Tuple[NodeInfo, list]]) -> dict:
        # Executa uma transacao distribuida utilizando 2PC.
        # 
        # Args:
        # operations_by_node: dicionario de node_id -> (NodeInfo, lista_de_operacoes)
        # 
        # Returns:
        # Dicionario de resultado com status de sucesso e dados
        tx_id = str(uuid.uuid4())
        self.logger.info(f"TX {tx_id[:8]}: iniciando transacao distribuida")
        self.logger.info(f"TX {tx_id[:8]}: nos participantes: {list(operations_by_node.keys())}")
        for node_id, (node, ops) in operations_by_node.items():
            for op in ops:
                self.logger.info(f"TX {tx_id[:8]}: no {node_id}: {op}")

        # Registra inicio da transacao
        with self.tx_log_lock:
            self.tx_log[tx_id] = {
                "state": "STARTED",
                "nodes": list(operations_by_node.keys()),
                "start_time": time.time()
            }

        # --- Fase 1: PREPARE ---

        self.logger.info(f"TX {tx_id[:8]}: FASE 1 - PREPARE")

        connections: Dict[str, socket.socket] = {}
        votes: Dict[str, str] = {}
        balances: Dict[str, float] = {}
        all_committed = True

        for node_id, (node, operations) in operations_by_node.items():
            sock = self.connect_to_node(node)
            if sock is None:
                self.logger.error(
                    f"TX {tx_id[:8]}: no {node_id} inacessivel - votando ABORT"
                )
                all_committed = False
                break

            connections[node_id] = sock

            # Envia mensagem PREPARE
            prepare_msg = make_message(
                MSG_PREPARE,
                tx_id=tx_id,
                operations=operations
            )
            try:
                send_message(sock, prepare_msg)
                response = recv_message(sock, timeout=NODE_TIMEOUT)

                if response is None:
                    self.logger.error(
                        f"TX {tx_id[:8]}: no {node_id} nao respondeu - votando ABORT"
                    )
                    votes[node_id] = "ABORT"
                    all_committed = False
                elif response.get("type") == MSG_VOTE_COMMIT:
                    votes[node_id] = "COMMIT"
                    # Coleta informacoes de saldo de operacoes de leitura
                    node_balances = response.get("balances", {})
                    balances.update(node_balances)
                    self.logger.info(
                        f"TX {tx_id[:8]}: no {node_id} votou COMMIT"
                    )
                else:
                    votes[node_id] = "ABORT"
                    reason = response.get("reason", "desconhecido")
                    self.logger.info(
                        f"TX {tx_id[:8]}: no {node_id} votou ABORT ({reason})"
                    )
                    all_committed = False
            except Exception as e:
                self.logger.error(
                    f"TX {tx_id[:8]}: erro de comunicacao com no {node_id}: {e}"
                )
                votes[node_id] = "ABORT"
                all_committed = False

        # --- Fase 2: DECISAO ---

        if all_committed and all(v == "COMMIT" for v in votes.values()):
            # GLOBAL COMMIT
            self.logger.info(f"TX {tx_id[:8]}: FASE 2 - GLOBAL_COMMIT")

            with self.tx_log_lock:
                self.tx_log[tx_id]["state"] = "COMMITTED"

            for node_id, sock in connections.items():
                try:
                    commit_msg = make_message(MSG_GLOBAL_COMMIT, tx_id=tx_id)
                    send_message(sock, commit_msg)
                    ack = recv_message(sock, timeout=NODE_TIMEOUT)
                    if ack:
                        self.logger.info(
                            f"TX {tx_id[:8]}: no {node_id} confirmou COMMIT"
                        )
                except Exception as e:
                    self.logger.error(
                        f"TX {tx_id[:8]}: erro ao enviar COMMIT para no {node_id}: {e}"
                    )

            self.logger.info(f"TX {tx_id[:8]}: transacao COMMITTED com sucesso")
            result = {"success": True, "tx_id": tx_id, "balances": balances}

        else:
            # GLOBAL ABORT
            self.logger.info(f"TX {tx_id[:8]}: FASE 2 - GLOBAL_ABORT")

            with self.tx_log_lock:
                self.tx_log[tx_id]["state"] = "ABORTED"

            for node_id, sock in connections.items():
                try:
                    abort_msg = make_message(MSG_GLOBAL_ABORT, tx_id=tx_id)
                    send_message(sock, abort_msg)
                    ack = recv_message(sock, timeout=NODE_TIMEOUT)
                    if ack:
                        self.logger.info(
                            f"TX {tx_id[:8]}: no {node_id} confirmou ABORT"
                        )
                except Exception as e:
                    self.logger.error(
                        f"TX {tx_id[:8]}: erro ao enviar ABORT para no {node_id}: {e}"
                    )

            # Determina o motivo do abort
            abort_reasons = []
            for node_id, vote in votes.items():
                if vote == "ABORT":
                    abort_reasons.append(f"no {node_id} votou ABORT")
            for node_id in operations_by_node:
                if node_id not in votes:
                    abort_reasons.append(f"no {node_id} inacessivel")

            reason = "; ".join(abort_reasons) if abort_reasons else "desconhecido"
            self.logger.info(f"TX {tx_id[:8]}: transacao ABORTED ({reason})")
            result = {"success": False, "tx_id": tx_id, "reason": reason}

        # Fecha conexoes
        for sock in connections.values():
            try:
                sock.close()
            except OSError:
                pass

        return result

    # --- Tratadores de requisicoes do cliente ---

    def handle_balance(self, account_id: int) -> dict:
        # Trata consulta de saldo - leitura direta (nao necessita 2PC).
        node = self.find_node(account_id)
        if not node:
            return {"success": False, "reason": f"Conta {account_id} nao encontrada"}

        sock = self.connect_to_node(node)
        if not sock:
            return {"success": False, "reason": f"No {node.node_id} inacessivel"}

        try:
            send_message(sock, make_message(MSG_NODE_BALANCE, account=account_id))
            response = recv_message(sock, timeout=NODE_TIMEOUT)
            if response and response.get("type") == MSG_RESPONSE:
                return {
                    "success": True,
                    "account": account_id,
                    "balance": response["balance"],
                    "node": node.node_id
                }
            else:
                reason = response.get("reason", "Desconhecido") if response else "Sem resposta"
                return {"success": False, "reason": reason}
        finally:
            sock.close()

    def handle_deposit(self, account_id: int, amount: float) -> dict:
        # Trata deposito - transacao em um unico no via 2PC.
        node = self.find_node(account_id)
        if not node:
            return {"success": False, "reason": f"Conta {account_id} nao encontrada"}

        operations = [{"operation": "DEPOSIT", "account": account_id, "amount": amount}]
        return self.execute_transaction({
            node.node_id: (node, operations)
        })

    def handle_withdraw(self, account_id: int, amount: float) -> dict:
        # Trata saque - transacao em um unico no via 2PC.
        node = self.find_node(account_id)
        if not node:
            return {"success": False, "reason": f"Conta {account_id} nao encontrada"}

        operations = [{"operation": "WITHDRAW", "account": account_id, "amount": amount}]
        return self.execute_transaction({
            node.node_id: (node, operations)
        })

    def handle_transfer(self, from_account: int, to_account: int,
                        amount: float) -> dict:
        # Trata transferencia - transacao potencialmente entre nos via 2PC.
        # 
        # Esta eh a demonstracao principal de transacoes distribuidas:
        # dinheiro eh sacado de uma conta e depositado em outra,
        # potencialmente em nos diferentes, de forma atomica.
        from_node = self.find_node(from_account)
        to_node = self.find_node(to_account)

        if not from_node:
            return {"success": False,
                    "reason": f"Conta de origem {from_account} nao encontrada"}
        if not to_node:
            return {"success": False,
                    "reason": f"Conta de destino {to_account} nao encontrada"}

        operations_by_node: Dict[str, Tuple[NodeInfo, list]] = {}

        if from_node.node_id == to_node.node_id:
            # Mesmo no: entrada unica com ambas operacoes
            operations_by_node[from_node.node_id] = (from_node, [
                {"operation": "WITHDRAW", "account": from_account, "amount": amount},
                {"operation": "DEPOSIT", "account": to_account, "amount": amount},
            ])
        else:
            # Nos diferentes: transacao distribuida entre nos
            operations_by_node[from_node.node_id] = (from_node, [
                {"operation": "WITHDRAW", "account": from_account, "amount": amount},
            ])
            operations_by_node[to_node.node_id] = (to_node, [
                {"operation": "DEPOSIT", "account": to_account, "amount": amount},
            ])

        self.logger.info(
            f"Transferencia: {from_account} -> {to_account}, valor: {amount:.2f}"
            + (f" (CROSS-NODE: {from_node.node_id} -> {to_node.node_id})"
               if from_node.node_id != to_node.node_id
               else f" (SAME-NODE: {from_node.node_id})")
        )

        return self.execute_transaction(operations_by_node)

    def handle_list_accounts(self) -> dict:
        # Lista todas as contas de todos os nos.
        all_accounts = {}
        for node in self.nodes:
            sock = self.connect_to_node(node)
            if not sock:
                self.logger.warning(f"Nao foi possivel acessar no {node.node_id}")
                continue
            try:
                send_message(sock, make_message(MSG_NODE_LIST))
                response = recv_message(sock, timeout=NODE_TIMEOUT)
                if response and response.get("type") == MSG_RESPONSE:
                    node_accounts = response.get("accounts", {})
                    for acc_id, balance in node_accounts.items():
                        all_accounts[acc_id] = {
                            "balance": balance,
                            "node": node.node_id
                        }
            finally:
                sock.close()

        return {"success": True, "accounts": all_accounts}

    # --- Tratamento de conexoes de clientes ---

    def _handle_client(self, sock: socket.socket, addr) -> None:
        # Trata uma conexao de cliente.
        try:
            while self.running:
                msg = recv_message(sock, timeout=120.0)
                if msg is None:
                    break

                msg_type = msg.get("type")
                self.logger.info(f"Requisicao do cliente: {msg_type} de {addr}")

                if msg_type == MSG_BALANCE:
                    result = self.handle_balance(msg["account"])
                elif msg_type == MSG_DEPOSIT:
                    result = self.handle_deposit(msg["account"], msg["amount"])
                elif msg_type == MSG_WITHDRAW:
                    result = self.handle_withdraw(msg["account"], msg["amount"])
                elif msg_type == MSG_TRANSFER:
                    result = self.handle_transfer(
                        msg["from_account"], msg["to_account"], msg["amount"]
                    )
                elif msg_type == MSG_LIST_ACCOUNTS:
                    result = self.handle_list_accounts()
                else:
                    result = {"success": False, "reason": f"Comando desconhecido: {msg_type}"}

                send_message(sock, make_message(MSG_RESPONSE, **result))

        except Exception as e:
            self.logger.error(f"Erro ao tratar cliente {addr}: {e}")
        finally:
            sock.close()

    # --- Servidor ---

    def start(self) -> None:
        # Inicia o servidor do coordenador.
        self.running = True
        self.server_socket = create_server_socket(self.host, self.port)
        self.logger.info(f"Coordenador de transacoes iniciado em {self.host}:{self.port}")
        self.logger.info(f"Nos registrados: {[n.node_id for n in self.nodes]}")

        # Aguarda os nos ficarem prontos
        self._wait_for_nodes()

        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                try:
                    client_sock, addr = self.server_socket.accept()
                except socket.timeout:
                    continue
                self.logger.info(f"Cliente conectado de {addr}")
                handler = threading.Thread(
                    target=self._handle_client,
                    args=(client_sock, addr),
                    daemon=True
                )
                handler.start()
            except OSError:
                break

    def _wait_for_nodes(self) -> None:
        # Aguarda todos os nos bancarios ficarem prontos.
        self.logger.info("Aguardando nos bancarios ficarem prontos...")
        for node in self.nodes:
            retries = 0
            while retries < 30:
                try:
                    sock = connect_to_server(node.host, node.port, timeout=2.0)
                    # Envia mensagem de teste
                    send_message(sock, make_message(MSG_NODE_LIST))
                    response = recv_message(sock, timeout=5.0)
                    sock.close()
                    if response:
                        self.logger.info(
                            f"No {node.node_id} ({node.host}:{node.port}) pronto"
                        )
                        break
                except (ConnectionRefusedError, socket.timeout, OSError):
                    pass
                retries += 1
                time.sleep(1)
            else:
                self.logger.warning(
                    f"No {node.node_id} ({node.host}:{node.port}) "
                    f"nao ficou pronto apos 30 tentativas"
                )

    def stop(self) -> None:
        # Para o servidor do coordenador.
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        self.logger.info("Coordenador parado")


# --- Ponto de entrada ---

def main():
    host = os.environ.get("COORDINATOR_HOST", "0.0.0.0")
    port = int(os.environ.get("COORDINATOR_PORT", "5000"))

    # Carrega configuracao dos nos a partir de variaveis de ambiente
    # Formato: NODE_<ID>_HOST, NODE_<ID>_PORT, NODE_<ID>_ACC_START, NODE_<ID>_ACC_END
    nodes = []
    for node_id in ["A", "B", "C"]:
        node_host = os.environ.get(f"NODE_{node_id}_HOST", f"bank-node-{node_id.lower()}")
        node_port = int(os.environ.get(f"NODE_{node_id}_PORT", str(6000 + ord(node_id) - ord('A') + 1)))
        acc_start = int(os.environ.get(f"NODE_{node_id}_ACC_START", str(1000 * (ord(node_id) - ord('A') + 1))))
        acc_end = int(os.environ.get(f"NODE_{node_id}_ACC_END", str(1000 * (ord(node_id) - ord('A') + 1) + 4)))

        nodes.append(NodeInfo(node_id, node_host, node_port, acc_start, acc_end))

    coordinator = TransactionCoordinator(host, port, nodes)

    try:
        coordinator.start()
    except KeyboardInterrupt:
        coordinator.stop()


if __name__ == "__main__":
    main()
