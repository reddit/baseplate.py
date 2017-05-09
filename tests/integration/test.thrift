exception ExpectedException {

}

service TestService {
    bool example_simple(),

    void example_throws(1: bool crash) throws (1: ExpectedException exc),
}
