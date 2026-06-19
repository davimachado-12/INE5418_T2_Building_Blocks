"""
Modulo de protocolo para o sistema distribuido de transacoes bancarias.

Implementa um protocolo de mensagens baseado em JSON sobre TCP
utilizando Berkeley sockets. As mensagens sao prefixadas com 4 bytes
(big-endian) indicando o tamanho do payload, garantindo framing confiavel.
"""

import json
import struct
import socket
import logging

logger = logging.getLogger(__name__)

# --- Tipos de mensagem ---

# Cliente -> Coordenador
MSG_BALANCE        = "BALANCE"
MSG_DEPOSIT        = "DEPOSIT"
MSG_WITHDRAW       = "WITHDRAW"
MSG_TRANSFER       = "TRANSFER"
MSG_LIST_ACCOUNTS  = "LIST_ACCOUNTS"

# Coordenador -> No bancario (2PC Fase 1)
MSG_PREPARE        = "PREPARE"

# No bancario -> Coordenador (resposta da Fase 1 do 2PC)
MSG_VOTE_COMMIT    = "VOTE_COMMIT"
MSG_VOTE_ABORT     = "VOTE_ABORT"

# Coordenador -> No bancario (2PC Fase 2)
MSG_GLOBAL_COMMIT  = "GLOBAL_COMMIT"
MSG_GLOBAL_ABORT   = "GLOBAL_ABORT"

# Coordenador -> No bancario (operacoes diretas)
MSG_NODE_BALANCE   = "NODE_BALANCE"
MSG_NODE_LIST      = "NODE_LIST"

# Resposta generica
MSG_RESPONSE       = "RESPONSE"
MSG_ERROR          = "ERROR"

# Confirmacao
MSG_ACK            = "ACK"

# Tamanho do cabecalho: 4 bytes para o comprimento da mensagem
HEADER_SIZE = 4
MAX_MSG_SIZE = 1024 * 1024  # Tamanho maximo da mensagem: 1 MB


def send_message(sock: socket.socket, msg: dict) -> None:
    """
    Envia uma mensagem JSON pelo socket TCP com framing por prefixo de tamanho.

    Formato do protocolo:
        [4 bytes: tamanho da mensagem (big-endian)] [N bytes: payload JSON]
    """
    try:
        payload = json.dumps(msg).encode('utf-8')
        header = struct.pack('!I', len(payload))
        sock.sendall(header + payload)
    except (BrokenPipeError, ConnectionResetError, OSError) as e:
        logger.error(f"Falha ao enviar mensagem: {e}")
        raise


def recv_message(sock: socket.socket, timeout: float = None) -> dict | None:
    """
    Recebe uma mensagem JSON do socket TCP com framing por prefixo de tamanho.

    Retorna None se a conexao foi fechada ou em caso de timeout.
    """
    old_timeout = sock.gettimeout()
    if timeout is not None:
        sock.settimeout(timeout)

    try:
        # Le o cabecalho de 4 bytes
        header = _recv_exactly(sock, HEADER_SIZE)
        if header is None:
            return None

        msg_len = struct.unpack('!I', header)[0]
        if msg_len > MAX_MSG_SIZE:
            logger.error(f"Mensagem excede tamanho maximo: {msg_len} bytes")
            return None

        # Le o payload
        payload = _recv_exactly(sock, msg_len)
        if payload is None:
            return None

        return json.loads(payload.decode('utf-8'))

    except socket.timeout:
        logger.debug("Timeout ao receber mensagem")
        return None
    except (json.JSONDecodeError, struct.error) as e:
        logger.error(f"Falha ao decodificar mensagem: {e}")
        return None
    except (ConnectionResetError, OSError) as e:
        logger.debug(f"Erro de conexao durante recebimento: {e}")
        return None
    finally:
        if timeout is not None:
            sock.settimeout(old_timeout)


def _recv_exactly(sock: socket.socket, n: int) -> bytes | None:
    """Recebe exatamente n bytes do socket. Retorna None se a conexao foi fechada."""
    data = b''
    while len(data) < n:
        try:
            chunk = sock.recv(n - len(data))
            if not chunk:
                return None
            data += chunk
        except (ConnectionResetError, OSError):
            return None
    return data


def create_server_socket(host: str, port: int, backlog: int = 10) -> socket.socket:
    """
    Cria um socket TCP servidor utilizando a API Berkeley sockets.

    Utiliza SO_REUSEADDR para permitir reinicializacao rapida.
    """
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((host, port))
    server_sock.listen(backlog)
    logger.info(f"Servidor escutando em {host}:{port}")
    return server_sock


def connect_to_server(host: str, port: int, timeout: float = 5.0) -> socket.socket:
    """
    Cria um socket TCP cliente e conecta ao servidor.

    Retentativas sao tratadas pelo chamador.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    sock.connect((host, port))
    return sock


def make_message(msg_type: str, **kwargs) -> dict:
    """Cria uma mensagem do protocolo com o tipo e campos adicionais especificados."""
    msg = {"type": msg_type}
    msg.update(kwargs)
    return msg
