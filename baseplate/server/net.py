import fcntl
import socket

from baseplate.lib.config import EndpointConfiguration


def bind_socket(endpoint: EndpointConfiguration) -> socket.socket:
    sock = socket.socket(endpoint.family, socket.SOCK_STREAM)

    # configure the socket to be auto-closed if we exec() e.g. on reload
    flags = fcntl.fcntl(sock.fileno(), fcntl.F_GETFD)
    fcntl.fcntl(sock.fileno(), fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)

    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # on linux, SO_REUSEPORT is supported for IPv4 and IPv6 but not other
    # families. we prefer it when available because it more evenly spreads load
    # among multiple processes sharing a port. (though it does have some
    # downsides, specifically regarding behaviour during restarts)
    if endpoint.family in (socket.AF_INET, socket.AF_INET6):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

    sock.bind(endpoint.address)
    sock.listen(128)
    return sock
