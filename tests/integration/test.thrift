exception ExpectedException {

}

service TestService {
    bool example() throws (1: ExpectedException exc),
    bool sleep() throws (1: ExpectedException exc),
}
