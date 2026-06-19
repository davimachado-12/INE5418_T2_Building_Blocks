"""
Cliente CLI - Sistema Distribuido de Transacoes Bancarias

Cliente interativo de linha de comando que se conecta ao Coordenador de
Transacoes para realizar operacoes bancarias (saldo, deposito, saque,
transferencia).

Tambem suporta um modo de teste de stress para demonstrar o controle
de concorrencia.
"""

import os
import sys
import socket
import threading
import time
import random
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from protocol import (
    send_message, recv_message, connect_to_server, make_message,
    MSG_BALANCE, MSG_DEPOSIT, MSG_WITHDRAW, MSG_TRANSFER, MSG_LIST_ACCOUNTS,
    MSG_RESPONSE,
)


# --- Cliente ---

class BankClient:
    def __init__(self, coord_host: str, coord_port: int):
        self.coord_host = coord_host
        self.coord_port = coord_port

    def _send_request(self, msg: dict) -> dict:
        """Envia uma requisicao ao coordenador e retorna a resposta."""
        sock = connect_to_server(self.coord_host, self.coord_port, timeout=30.0)
        try:
            send_message(sock, msg)
            response = recv_message(sock, timeout=30.0)
            if response is None:
                return {"success": False, "reason": "Sem resposta do coordenador"}
            return response
        finally:
            sock.close()

    def balance(self, account_id: int) -> dict:
        return self._send_request(
            make_message(MSG_BALANCE, account=account_id)
        )

    def deposit(self, account_id: int, amount: float) -> dict:
        return self._send_request(
            make_message(MSG_DEPOSIT, account=account_id, amount=amount)
        )

    def withdraw(self, account_id: int, amount: float) -> dict:
        return self._send_request(
            make_message(MSG_WITHDRAW, account=account_id, amount=amount)
        )

    def transfer(self, from_acc: int, to_acc: int, amount: float) -> dict:
        return self._send_request(
            make_message(MSG_TRANSFER, from_account=from_acc,
                        to_account=to_acc, amount=amount)
        )

    def list_accounts(self) -> dict:
        return self._send_request(make_message(MSG_LIST_ACCOUNTS))


# --- Teste de Stress ---

def run_stress_test(client: BankClient, num_threads: int = 5,
                    num_transactions: int = 10):
    """
    Executa transacoes concorrentes para demonstrar o controle de concorrencia.

    Multiplas threads realizam transferencias simultaneas. Algumas dessas
    transferencias irao conflitar e o mecanismo S2PL ira serializa-las.
    """
    print("\n--- TESTE DE STRESS: Transacoes Concorrentes ---\n")

    # Obtem o saldo total inicial
    print("Consultando saldos iniciais...")
    result = client.list_accounts()
    initial_total = 0
    if result.get("success"):
        accounts = result.get("accounts", {})
        for acc_id, info in sorted(accounts.items(), key=lambda x: int(x[0])):
            initial_total += info["balance"]
    print(f"Saldo total inicial: {initial_total:.2f}")

    # Contas disponiveis para transferencias
    all_accounts = [1000, 1001, 1002, 1003, 1004,
                    2000, 2001, 2002, 2003, 2004,
                    3000, 3001, 3002, 3003, 3004]

    results_lock = threading.Lock()
    results = {"committed": 0, "aborted": 0, "errors": 0}
    start_barrier = threading.Barrier(num_threads)

    def worker(worker_id: int):
        """Thread worker que executa transferencias aleatorias."""
        start_barrier.wait()  # Sincroniza todas as threads para iniciar simultaneamente

        for i in range(num_transactions):
            from_acc = random.choice(all_accounts)
            to_acc = random.choice([a for a in all_accounts if a != from_acc])
            amount = round(random.uniform(1, 50), 2)

            try:
                result = client.transfer(from_acc, to_acc, amount)
                with results_lock:
                    if result.get("success"):
                        results["committed"] += 1
                        status = "COMMITTED"
                    else:
                        results["aborted"] += 1
                        status = f"ABORTED ({result.get('reason', '?')})"

                tx_id = result.get("tx_id", "?")[:8]
                print(f"  Thread {worker_id} TX {tx_id}: "
                      f"{from_acc} -> {to_acc} ${amount:.2f} | {status}")

            except Exception as e:
                with results_lock:
                    results["errors"] += 1
                print(f"  Thread {worker_id}: erro - {e}")

    # Lanca as threads workers
    print(f"\nIniciando {num_threads} workers concorrentes, "
          f"{num_transactions} transacoes cada...\n")

    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # Resultados finais
    print("\n--- RESULTADOS ---\n")

    total_tx = results["committed"] + results["aborted"] + results["errors"]
    print(f"  Total de transacoes: {total_tx}")
    print(f"  Committed:           {results['committed']}")
    print(f"  Aborted:             {results['aborted']}")
    print(f"  Erros:               {results['errors']}")

    # Verifica conservacao de dinheiro (total deve permanecer inalterado)
    print("\nVerificando conservacao de dinheiro...")
    time.sleep(0.5)
    result = client.list_accounts()
    final_total = 0
    if result.get("success"):
        accounts = result.get("accounts", {})
        for acc_id, info in sorted(accounts.items(), key=lambda x: int(x[0])):
            final_total += info["balance"]

    print(f"  Saldo total inicial: {initial_total:.2f}")
    print(f"  Saldo total final:   {final_total:.2f}")

    if abs(final_total - initial_total) < 0.01:
        print("  CONSERVACAO VERIFICADA: saldo total inalterado.")
        print("  Isto comprova a atomicidade das transacoes distribuidas.")
    else:
        print(f"  CONSERVACAO VIOLADA: diferenca = {final_total - initial_total:.2f}")

    print()


# --- CLI Interativo ---

def print_help():
    print("""
Comandos disponiveis:

  balance <conta>                       Consultar saldo de uma conta
  deposit <conta> <valor>               Depositar em uma conta
  withdraw <conta> <valor>              Sacar de uma conta
  transfer <origem> <destino> <valor>   Transferir entre contas
  list                                  Listar todas as contas e saldos
  stress [threads] [tx_por_thread]      Teste de stress concorrente
  help                                  Exibir esta ajuda
  quit                                  Sair do cliente

Faixas de contas:
  No A: contas 1000-1004
  No B: contas 2000-2004
  No C: contas 3000-3004
""")


def interactive_cli(client: BankClient):
    """Executa a interface de linha de comando interativa."""
    print("\n--- Sistema Distribuido de Transacoes Bancarias ---")
    print("--- Building Block: Transacoes Distribuidas (2PC + S2PL) ---\n")
    print_help()

    while True:
        try:
            raw = input("bank> ").strip()
            if not raw:
                continue

            parts = raw.split()
            cmd = parts[0].lower()

            if cmd in ("quit", "exit", "q"):
                print("Encerrando cliente.")
                break

            elif cmd == "help":
                print_help()

            elif cmd == "balance":
                if len(parts) != 2:
                    print("  Uso: balance <conta>")
                    continue
                account = int(parts[1])
                result = client.balance(account)
                if result.get("success"):
                    print(
                        f"  Conta {account} (No {result.get('node', '?')}): "
                        f"${result['balance']:.2f}"
                    )
                else:
                    print(f"  Erro: {result.get('reason', 'Desconhecido')}")

            elif cmd == "deposit":
                if len(parts) != 3:
                    print("  Uso: deposit <conta> <valor>")
                    continue
                account = int(parts[1])
                amount = float(parts[2])
                if amount <= 0:
                    print("  O valor deve ser positivo.")
                    continue
                result = client.deposit(account, amount)
                if result.get("success"):
                    print(f"  Deposito de ${amount:.2f} na conta {account} realizado.")
                else:
                    print(f"  Deposito falhou: {result.get('reason', 'Desconhecido')}")

            elif cmd == "withdraw":
                if len(parts) != 3:
                    print("  Uso: withdraw <conta> <valor>")
                    continue
                account = int(parts[1])
                amount = float(parts[2])
                if amount <= 0:
                    print("  O valor deve ser positivo.")
                    continue
                result = client.withdraw(account, amount)
                if result.get("success"):
                    print(f"  Saque de ${amount:.2f} da conta {account} realizado.")
                else:
                    print(f"  Saque falhou: {result.get('reason', 'Desconhecido')}")

            elif cmd == "transfer":
                if len(parts) != 4:
                    print("  Uso: transfer <conta_origem> <conta_destino> <valor>")
                    continue
                from_acc = int(parts[1])
                to_acc = int(parts[2])
                amount = float(parts[3])
                if amount <= 0:
                    print("  O valor deve ser positivo.")
                    continue
                if from_acc == to_acc:
                    print("  Nao e possivel transferir para a mesma conta.")
                    continue

                result = client.transfer(from_acc, to_acc, amount)
                if result.get("success"):
                    print(
                        f"  Transferencia de ${amount:.2f} da conta {from_acc} "
                        f"para conta {to_acc} realizada."
                    )
                else:
                    print(f"  Transferencia falhou: {result.get('reason', 'Desconhecido')}")

            elif cmd == "list":
                result = client.list_accounts()
                if result.get("success"):
                    accounts = result.get("accounts", {})
                    print("\n  Visao geral das contas:")
                    print("  " + "-" * 35)

                    current_node = None
                    total = 0
                    for acc_id, info in sorted(accounts.items(), key=lambda x: int(x[0])):
                        node = info["node"]
                        if node != current_node:
                            current_node = node
                            print(f"\n  No {node}:")
                        balance = info["balance"]
                        total += balance
                        print(f"    Conta {acc_id}: ${balance:.2f}")

                    print("  " + "-" * 35)
                    print(f"  Total: ${total:.2f}")
                    print()
                else:
                    print(f"  Erro: {result.get('reason', 'Desconhecido')}")

            elif cmd == "stress":
                threads = int(parts[1]) if len(parts) > 1 else 5
                tx_per_thread = int(parts[2]) if len(parts) > 2 else 10
                run_stress_test(client, threads, tx_per_thread)

            else:
                print(f"  Comando desconhecido: {cmd}. Digite 'help' para ver os comandos.")

        except ValueError as e:
            print(f"  Entrada invalida: {e}")
        except (ConnectionRefusedError, OSError) as e:
            print(f"  Erro de conexao: {e}")
            print("  Verifique se o coordenador esta em execucao.")
        except KeyboardInterrupt:
            print("\nEncerrando cliente.")
            break
        except EOFError:
            print("\nEncerrando cliente.")
            break


# --- Ponto de entrada ---

def main():
    coord_host = os.environ.get("COORDINATOR_HOST", "coordinator")
    coord_port = int(os.environ.get("COORDINATOR_PORT", "5000"))

    # Aguarda o coordenador ficar pronto
    print(f"Conectando ao coordenador em {coord_host}:{coord_port}...")
    retries = 0
    while retries < 30:
        try:
            sock = connect_to_server(coord_host, coord_port, timeout=2.0)
            sock.close()
            print("Conectado.\n")
            break
        except (ConnectionRefusedError, socket.timeout, OSError):
            retries += 1
            if retries % 5 == 0:
                print(f"  Aguardando... ({retries}s)")
            time.sleep(1)
    else:
        print("Nao foi possivel conectar ao coordenador. Encerrando.")
        sys.exit(1)

    client = BankClient(coord_host, coord_port)
    interactive_cli(client)


if __name__ == "__main__":
    main()
