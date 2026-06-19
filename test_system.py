"""Teste automatizado para verificar o funcionamento do sistema bancario distribuido."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from protocol import send_message, recv_message, connect_to_server, make_message
from protocol import MSG_BALANCE, MSG_DEPOSIT, MSG_WITHDRAW, MSG_TRANSFER, MSG_LIST_ACCOUNTS

HOST = os.environ.get("COORDINATOR_HOST", "localhost")
PORT = int(os.environ.get("COORDINATOR_PORT", "5000"))

def send_req(msg):
    sock = connect_to_server(HOST, PORT, timeout=10.0)
    try:
        send_message(sock, msg)
        return recv_message(sock, timeout=10.0)
    finally:
        sock.close()

print("=== Teste 1: Listar todas as contas ===")
r = send_req(make_message(MSG_LIST_ACCOUNTS))
print(f"  Sucesso: {r.get('success')}")
accounts = r.get('accounts', {})
total = sum(a['balance'] for a in accounts.values())
print(f"  Total de contas: {len(accounts)}")
print(f"  Saldo total: {total:.2f}")

print("\n=== Teste 2: Consultar saldo ===")
r = send_req(make_message(MSG_BALANCE, account=1000))
print(f"  Saldo da conta 1000: {r.get('balance')}")

print("\n=== Teste 3: Deposito ===")
r = send_req(make_message(MSG_DEPOSIT, account=1000, amount=500))
print(f"  Deposito de $500 na conta 1000: {'OK' if r.get('success') else 'FALHOU'}")
r = send_req(make_message(MSG_BALANCE, account=1000))
print(f"  Saldo da conta 1000: {r.get('balance')} (esperado: 1500)")

print("\n=== Teste 4: Saque ===")
r = send_req(make_message(MSG_WITHDRAW, account=1000, amount=200))
print(f"  Saque de $200 da conta 1000: {'OK' if r.get('success') else 'FALHOU'}")
r = send_req(make_message(MSG_BALANCE, account=1000))
print(f"  Saldo da conta 1000: {r.get('balance')} (esperado: 1300)")

print("\n=== Teste 5: Transferencia cross-node (No A -> No B) ===")
r = send_req(make_message(MSG_TRANSFER, from_account=1000, to_account=2000, amount=300))
print(f"  Transferencia de $300 (1000 -> 2000): {'OK' if r.get('success') else 'FALHOU'}")
r = send_req(make_message(MSG_BALANCE, account=1000))
print(f"  Saldo da conta 1000: {r.get('balance')} (esperado: 1000)")
r = send_req(make_message(MSG_BALANCE, account=2000))
print(f"  Saldo da conta 2000: {r.get('balance')} (esperado: 1300)")

print("\n=== Teste 6: Saldo insuficiente ===")
r = send_req(make_message(MSG_WITHDRAW, account=3000, amount=5000))
print(f"  Saque de $5000 da conta 3000 (saldo 1000): {'ABORT' if not r.get('success') else 'FALHOU - deveria ter abortado'}")

print("\n=== Teste 7: Verificacao de conservacao ===")
r = send_req(make_message(MSG_LIST_ACCOUNTS))
total_after_ops = sum(a['balance'] for a in r.get('accounts', {}).values())
# Esperado: 15000 + 500 (deposito) - 200 (saque) = 15300
# A transferencia cross-node nao deve alterar o total
print(f"  Total esperado: 15300.00 (15000 + 500 deposito - 200 saque)")
print(f"  Total obtido:   {total_after_ops:.2f}")
print(f"  Conservacao:    {'OK' if abs(total_after_ops - 15300) < 0.01 else 'FALHOU'}")

print("\n=== Todos os testes concluidos ===")
