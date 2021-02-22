from contextlib import contextmanager

from thrift.protocol.TProtocol import TProtocolException
from thrift.Thrift import TApplicationException
from thrift.Thrift import TException
from thrift.transport.TTransport import TTransportException

from graphql_api.lib.circuit_breaker.observer import BreakerObserver


@contextmanager
def thrift_circuit_breaker(context, breaker):
    breaker_observer = BreakerObserver(context, breaker)
    breaker_observer.check_state()

    success: bool = True
    try:
        yield

    except (TApplicationException, TTransportException, TProtocolException):
        # Unhealthy errors:
        #   * Unknown thrift failure
        #   * DNS, socket, connection error
        #   * serialization error
        success = False
        raise
    except TException:
        # Healthy errors: known thrift exception, defined in the IDL
        raise
    except Exception:
        # Any other
        success = False
        raise
    finally:
        breaker_observer.register_attempt(success)
