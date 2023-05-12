namespace go reddit.message_queue
namespace py message_queue.thrift
namespace java com.reddit.baseplate.message_queue

struct PutResponse {}

exception ThriftTimedOutError {}

service RemoteMessageQueueService {
    PutResponse put(
        1: binary message
        2: double timeout
    ) throws (
        1: ThriftTimedOutError timed_out_error
    );
}
