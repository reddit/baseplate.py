namespace py test_local_namespace

include "include_target.thrift"

struct TestInclude {
  1: include_target.TestInclude,
}
