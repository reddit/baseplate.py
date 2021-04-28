namespace py baseplate.thrift
namespace go reddit.baseplate
namespace java com.reddit.baseplate

/** An integer measuring the number of milliseconds of UTC time since epoch.

*/
typedef i64 TimestampMilliseconds

/** The base for any baseplate-based service.

Your service should inherit from this one so that common tools can interact
with any expected interfaces.

DEPRECATED: Please migrate to BaseplateServiceV2.

*/
service BaseplateService {
    /** Return whether or not the service is healthy.

    The healthchecker (baseplate.server.healthcheck) expects this endpoint to
    exist so it can determine your service's health.

    This should return True if the service is healthy. If the service is
    unhealthy, it can return False or raise an exception.

    */
    bool is_healthy(),
}

/** The different types of probes supported by is_healthy endpoint.

Please refer to Kubernetes' documentation for the differences between them:
https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/

Your service should use Readiness probe as the fallback for unsupported probes.

Note that the HTTP health check could use the string names of the probes,
so changing the names, even without changing the numeric values,
is considered as breaking change and should be avoided.

*/
enum IsHealthyProbe {
    READINESS = 1,
    LIVENESS = 2,
    STARTUP = 3,
}

/** The arg struct for is_healthy endpoint.

*/
struct IsHealthyRequest {
    1: optional IsHealthyProbe probe;
}

/** The base for any baseplate-based service.

Your service should inherit from this one so that common tools can interact
with any expected interfaces.

*/
service BaseplateServiceV2 {
    /** Return whether or not the service is healthy.

    The healthchecker (baseplate.server.healthcheck) expects this endpoint to
    exist so it can determine your service's health.

    This should return True if the service is healthy. If the service is
    unhealthy, it can return False or raise an exception.

    */
    bool is_healthy(
        1: IsHealthyRequest request,
    ),
}

/** The integer values within this enum correspond to HTTP status codes.

HTTP layers can easily map errors to an appropriate status code.
*/
enum ErrorCode {
    /** This indicates that the request was invalid. More details may be
    present in the details map of the Error struct.
    */
    BAD_REQUEST = 400,
    /** This indicates that the request could not be authenticated. It may be
    appropriate to retry the request in some scenarios. For example, a retry is
    appropriate if an expired authentication credential was initially used.
    */
    UNAUTHORIZED = 401,
    /** This indicates that the request can not be completed until a payment is
    made.
    */
    PAYMENT_REQUIRED = 402,
    /** This indicates that the client does not have access to the requested
    resource. Unlike UNAUTHORIZED, refreshing an authentication resource and
    trying again will not make a difference.
    */
    FORBIDDEN = 403,
    /** This indicates that the request was made for a resource that does not
    exist.
    */
    NOT_FOUND = 404,
    /** This indicates that the request would cause a conflict with the current
    state of the server.
    */
    CONFLICT = 409,
    /** This indicates that the requested resource was once available but is no
    longer.
    */
    GONE = 410,
    /** This indicates that the server is not in a state required to complete
    the request. For example, the server may require that a resource's children
    must be deleted before the resource itself is deleted.
    */
    PRECONDITION_FAILED = 412,
    /** This indicates that the request is larger than the limits set by the
    server, such as when a file that is too big is uploaded.
    */
    PAYLOAD_TOO_LARGE = 413,
    /** This indicates that the server is a teapot rather than a coffee maker.
    */
    IM_A_TEAPOT = 418,
    /** This indicates that the request was directed at a server that is not
    able to produce a response. For example, because of connection reuse.
    */
    MISDIRECTED_REQUEST = 421,
    /** This indicates that the request was valid, but the server is unable to
    process the request instructions. The request should not be retried without
    modification.
    */
    UNPROCESSABLE_ENTITY = 422,
    /** This indicates that the request tried to operate on a resource that is
    locked.
    */
    LOCKED = 423,
    /** This indicates that the request failed because it depends on another
    request that has failed.
    */
    FAILED_DEPENDENCY = 424,
    /** This indicates that the server is concerned that the request may be
    replayed, resulting in a reply attack.
    */
    TOO_EARLY = 425,
    /** This indicates that the server requires a precondition to be specified
    in order to process the request. For example, the server may require
    clients to specify a version number for a resource that they are trying to
    update in order to avoid lost updates. If the client does not specify a
    version number then the server may respond with `PRECONDITION_REQUIRED`.
    */
    PRECONDITION_REQUIRED = 428,
    /** This indicates that the client has been rate limited by the server. It
    may be appropriate to retry the request after some time has passed.
    */
    TOO_MANY_REQUESTS = 429,
    /** This indicates that the request contained a header value that is too
    large.
    */
    REQUEST_HEADER_FIELDS_TOO_LARGE = 431,
    /** This indicates that the requested resource is unavailable for legal
    reasons such as when the content is censored in a country.
    */
    UNAVAILABLE_FOR_LEGAL_REASONS = 451,
    /** This indicates a generic, unhandled server errors.
    */
    INTERNAL_SERVER_ERROR = 500,
    /** This indicates that the request was made for a method that the server
    understands but does not support.
    */
    NOT_IMPLEMENTED = 501,
    /** This indicates that a downstream service returned a bad (unexpected
    error or malformed) response.
    */
    BAD_GATEWAY = 502,
    /** This indicates that the server is not ready to handle the request such
    as when it is down for maintenance or overloaded. Clients may retry the
    request with exponential backoff.
    */
    SERVICE_UNAVAILABLE = 503,
    /** This indicates that the server timed out.
    */
    TIMEOUT = 504,
    /** This indicates that the server does not have sufficient storage to
    complete the request.
    */
    INSUFFICIENT_STORAGE = 507,
    /** This indicates that the server detected that an infinite loop between
    services.
    */
    LOOP_DETECTED = 508,
    /** Developers should use a value higher than 1000
    when defining custom codes.
    */
    USER_DEFINED = 1000,
}

exception Error {
    /** A code describing the general nature of the error.
    This should be specified for all errors. This field uses
    the i32 type instead of the ErrorCode type in order to give
    developers an escape hatch to define their own error codes.
    Developers should do their best to avoid defining a custom
    error code. Developers should use a value higher than 1000
    when defining custom codes.
    */
    1: optional i32 code
    /** A human-readable error message. It should both explain the error
    and offer an actionable resolution to it, if applicable. It should
    be safe to desplay this message in a user-facing client.
    */
    2: optional string message
    /** A map of additional error information. This is most useful
    when there is a validation error. The server may use this map
    to return multiple errors. This should be safe for clients to
    display. Example:
        {
            "post.title": "This field is too long.",
            "post.kind": "This field is required."
        }
    */
    3: optional map<string, string> details
    /** Server could choose to set this field to true to explicitly indicate
    that client shall retry this request, and false to explicitly indicate that
    client shall not retry this request. Unset means that it's up to the client
    to decide (using other information, for example the code) whether to retry
    this request.
    */
    4: optional bool retryable
}
