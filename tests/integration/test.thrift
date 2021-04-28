exception ExpectedException {

}

service TestService {
    bool example() throws (1: ExpectedException exc),
}

struct ExampleStruct {
    1: string string_field;
    2: i64 int_field;
}
