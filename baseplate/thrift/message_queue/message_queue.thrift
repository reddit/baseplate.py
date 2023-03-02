namespace go reddit.message_queue
namespace py message_queue.thrift
namespace java com.reddit.baseplate.message_queue

struct CreateResponse {}

struct PutResponse {}

struct GetResponse {
    1: binary value;
}

exception ThriftTimedOutError {}

service RemoteMessageQueueService {
    CreateResponse create_queue(
        1: string queue_name
        2: i64 max_messages
    );
    PutResponse put(
        1: string queue_name
        2: binary message
        3: double timeout
    ) throws (
        1: ThriftTimedOutError timed_out_error
    );
}
