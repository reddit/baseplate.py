namespace go baseplate.remote_message_queue
namespace py baseplate_remote_message_queue
namespace java com.reddit.baseplate.remote_message_queue

struct PutRequest {
    1: binary data;
}

struct PutResponse {}


struct GetRequest {
    1: binary key;
}

struct GetResponse {
    1: binary value;
}

exception PutFailedError {}

exception GetFailedError {}

service RemoteMessageQueueService {
    PutResponse put(
        1: PutRequest request
    ) throws (
        1: PutFailedError put_failed_error
    );
    GetResponse get(
        1: GetRequest request
    ) throws (
        1: GetFailedError get_failed_error
    );
}
