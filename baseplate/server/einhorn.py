"""Client library for children of Einhorn."""
import contextlib
import json
import os
import socket


class NotEinhornWorker(Exception):
    pass


def is_worker() -> bool:
    """Return if this process is an Einhorn worker."""
    return os.getppid() == int(os.environ.get("EINHORN_MASTER_PID", -1))


def get_socket_count() -> int:
    """Return how many sockets are bound."""
    if not is_worker():
        raise NotEinhornWorker

    return int(os.environ.get("EINHORN_FD_COUNT", 0))


def get_socket(index: int = 0) -> socket.socket:
    """Get an Einhorn-bound socket from the environment.

    Einhorn can bind multiple sockets (via multiple -b arguments), the
    ``index`` parameter can be used to choose which socket to retrieve.  When
    sockets are bound, Einhorn provides several environment variables to child
    worker processes:

    - EINHORN_FD_COUNT: the number of sockets bound
    - EINHORN_FD_#: for each socket bound, the file descriptor for that socket
    - EINHORN_FD_FAMILY_#: for each socket bound, the protocol family of that
        socket (this is a recent addition, so if it's not present default to
        AF_INET)

    :param index: The socket number to get.

    """
    if not is_worker():
        raise NotEinhornWorker

    fd_count = get_socket_count()
    if not 0 <= index < fd_count:
        raise IndexError

    fileno = int(os.environ[f"EINHORN_FD_{index:d}"])
    family_name = os.environ.get(f"EINHORN_FD_FAMILY_{index:d}", "AF_INET")
    assert family_name.startswith("AF_"), "invalid socket family name"
    family = getattr(socket, family_name)
    return socket.fromfd(fileno, family, socket.SOCK_STREAM)


def ack_startup() -> None:
    """Send acknowledgement that we started up to the Einhorn master."""
    if not is_worker():
        raise NotEinhornWorker

    control_sock_name = os.environ["EINHORN_SOCK_PATH"]
    control_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    control_sock.connect(control_sock_name)

    with contextlib.closing(control_sock):
        control_sock.sendall(
            (json.dumps({"command": "worker:ack", "pid": os.getpid()}) + "\n").encode("utf-8")
        )
