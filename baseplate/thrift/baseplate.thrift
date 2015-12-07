namespace py baseplate.thrift

/** The base for any baseplate-based service.

Your service should inherit from this one so that common tools can interact
with any expected interfaces.

*/
service BaseplateService {
    /** Return whether or not the service is healthy.

    baseplate-healthcheck expects this endpoint to exist to determine your
    service's health.

    This should return True if the service is healthy. If the service is
    unhealthy, it can return False or raise an exception.

    */
    bool is_healthy(),
}
