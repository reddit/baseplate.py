namespace go reddit.message_queue
namespace py message_queue.thrift
namespace java com.reddit.baseplate.message_queue

struct PutResponse {}

struct GetResponse {
    1: binary value;
}

exception PutFailedError {}

exception GetFailedError {}

service RemoteMessageQueueService {
    PutResponse put(
        1: string queue_name
        2: i64 max_messages
        3: binary message
        4: double timeout
    ) throws (
        1: PutFailedError put_failed_error
    );
    GetResponse get(
    ) throws (
        1: GetFailedError get_failed_error
    );
}
