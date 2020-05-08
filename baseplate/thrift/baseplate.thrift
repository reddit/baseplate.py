namespace py baseplate.thrift
namespace go reddit.baseplate
namespace java com.reddit.baseplate

/**A raw authentication token as returned by the authentication service.

*/
typedef string AuthenticationToken

/** A two-character ISO 3166-1 country code

*/
typedef string CountryCode

/** The base for any baseplate-based service.

Your service should inherit from this one so that common tools can interact
with any expected interfaces.

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


/** The components of the Reddit LoID cookie that we want to propagate between
services.

This model is a component of the "Edge-Request" header.  You should not need to
interact with this model directly, but rather through the EdgeRequestContext
interface provided by baseplate.

*/
struct Loid {
    /** The ID of the LoID cookie.

    */
    1: string id;
    /** The time when the LoID cookie was created in epoch milliseconds.

    */
    2: i64 created_ms;
}

/** The components of the Reddit Session tracker cookie that we want to
propagate between services.

This model is a component of the "Edge-Request" header.  You should not need to
interact with this model directly, but rather through the EdgeRequestContext
interface provided by baseplate.

*/
struct Session {
    /** The ID of the Session tracker cookie.

    */
    1: string id;
}

/** The components of the device making a request to our services that we want to
propogate between services.

This model is a component of the "Edge-Request" header.  You should not need to
interact with this model directly, but rather through the EdgeRequestContext
interface provided by baseplate.

*/
struct Device {
    /** The ID of the device.

    */
    1: string id;
}

/** Metadata about the origin service for a request.

The "origin" service is the service responsible for handling the request from
the client.

This model is a component of the "Edge-Request" header.  You should not need to
interact with this model directly, but rather through the EdgeRequestContext
interface provided by baseplate.
*/
struct OriginService {
    /** The name of the origin service.

    */
    1: string name
}

/** Geolocation data from a request to our services that we want to
propagate between services.

This model is a component of the "Edge-Request" header.  You should not need to
interact with this model directly, but rather through the EdgeRequestContext
interface provided by baseplate.

*/
struct Geolocation {
    /** The country code of the requesting client.
    */
    1: CountryCode country_code
}

/** Container model for the Edge-Request context header.

Baseplate will automatically parse this from the "Edge-Request" header and
provides an interface that wraps this Thrift model.  You should not need to
interact with this model directly, but rather through the EdgeRequestContext
interface provided by baseplate.

*/
struct Request {
    1: Loid loid;
    2: Session session;
    3: AuthenticationToken authentication_token;
    4: Device device;
    5: OriginService origin_service;
    6: Geolocation geolocation;
}

/** The integer values within this enum correspond to HTTP status codes.

HTTP layers can easily map errors to an appropriate status code.
*/
enum ErrorCode {
    /** This is appropriate if the client sent an invalid request.

    You can send the details using the details map in the Error struct.
    */
    BAD_REQUEST = 400,
    /** This is appropriate when you fail to authenticate the request.

    It may be appropriate for the client to retry this request in the event, for
    example, if they used an expired authentication credential, they can retry
    */
    UNAUTHORIZED = 401,
    /** This is appropriate if you need to communicate to the client that their
    request can not be completed until a payment is made.
    */
    PAYMENT_REQUIRED = 402,
    /** This is appropriate when you can authenticate a request but the client
    does not have access to the requested resource.

    Unlike Unauthorized, refreshing an authentication resource and trying again
    will not make a difference.
    */
    FORBIDDEN = 403,
    /** This is appropriate when the client tries to access something that does
    not exist.
    */
    NOT_FOUND = 404,
    /** This is appropriate when a client request would cause a conflict with
    the current state of the server.
    */
    CONFLICT = 409,
    /** This is appropriate when the resource requested was once available but is
    no longer.
    */
    GONE = 410,
    /** This is appropriate when the server is not in a state required to complete
    the clients request. For example, deleting an entity that has children which must
    by deleted first.
    */
    PRECONDITION_FAILED = 412,
    /** This is appropriate when the client sends a request that is larger than
    the limits set by the server, such as when they try to upload a file that is
    too big.
    */
    PAYLOAD_TOO_LARGE = 413,
    /** This is appropriate when the server is a teapot rather than a coffee maker.
    */
    IM_A_TEAPOT = 418,
    /** This is appropriate when the request was directed at a server that is not
    able to produce a response. For example, because of connection reuse.
    */
    MISDIRECTED_REQUEST = 421,
    /** This is appropriate when the request is valid but the server is unable to
    process the request instructions.

    The request should not be retried without modification.
    */
    UNPROCESSSABLE_ENTITY = 422,
    /** This is appropriate when the client tries to operate on a resource that is
    locked.
    */
    LOCKED = 423,
    /** This is appropriate when the request failed because it depended on another
    request and that request failed.
    */
    FAILED_DEPENDENCY = 424,
    /** This is appropriate when the server is concerned that the request may be
    replayed, resulting in a replay attack.
    */
    TOO_EARLY = 425,
    /** This is appropriate if the server requires a precondition to be specified in
    order to process a request. For example, a server may require clients to specify
    a version number for a resource that they are trying to update in order to avoid
    lost updates. If the client does not specify a version number then the server may
    respond with `PRECONDITION_REQUIRED`.
    */
    PRECONDITION_REQUIRED = 428,
    /** This is appropriate when the client has been rate limited by the server.

    It may be appropriate for the client to retry the request after some time has
    passed, it is encouraged to use this along with Retryable to communicate to
    the client when they are able to retry.
    */
    TOO_MANY_REQUESTS = 429,
    /** This is appropriate when the client sends a header value that is too large.
    */
    REQUEST_HEADER_FIELDS_TOO_LARGE = 431,
    /** This is appropriate when the requested resource is unavailable for
    legal reasons, such as when the content is censored in a country.
    */
    UNAVAILABLE_FOR_LEGAL_REASONS = 451,
    /** This is appropriate for generic, unhandled server errors.
    */
    INTERNAL_SERVER_ERROR = 500,
    /** This applies when a request is made for an HTTP method that the server
    understands but does not support.
    */
    NOT_IMPLEMENTED = 501,
    /** This is appropriate to use when your service is responsible for making
    requests to other services and one returns a bad (unexpected error or malformed)
    response.
    */
    BAD_GATEWAY = 502,
    /** This is appropriate when a server is not ready to handle a request such
    as when it is down for maintenance or overloaded.

    Clients may retry 503's with exponential backoff.
    */
    SERVICE_UNAVAILABLE = 503,
    /** This is appropriate to use when your service is responsible for making
    requests to other services and one times out.
    */
    TIMEOUT = 504,
    /** This is appropriate when a server does not have sufficient storage to complete
    the client's request.
    */
    INSUFFICIENT_STORAGE = 507,
    /** This is appropriate if the server is able to detect that an infinite loop has been
    detected between services.
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
    and offer an actionable resolution to it, if applicable. Displaying 
    this message to a user should not be dangerous.
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
}
