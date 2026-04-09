# Tests for base server functionality
#
# Server assumed to be running on the default port

setup() {
    load "test_helper/bats-support/load"
    load "test_helper/bats-assert/load"
}

@test "RPUSH" {
    run redis-cli RPUSH list_key foo
    assert_output 1
    run redis-cli RPUSH list_key bar
    assert_output 2
}

