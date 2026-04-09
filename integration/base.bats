# Tests for base server functionality
#
# Server assumed to be running on the default port

setup() {
    load "test_helper/bats-support/load"
    load "test_helper/bats-assert/load"
}

@test "ping" {
    run redis-cli PING
    assert_output PONG
}

@test "multiple pings on same connection" {
    run bash -c "echo -e 'PING\nPING\n' | redis-cli"
    assert_line --index 0 'PONG'
    assert_line --index 1 'PONG'
}

@test "multiple pings on different connections" {
    run redis-cli PING
    assert_output PONG
    run redis-cli PING
    assert_output PONG
}

@test "echo" {
    run redis-cli ECHO ABC
    assert_output ABC
    run redis-cli echo DEF
    assert_output DEF
    run redis-cli eChO XYZ
    assert_output XYZ
}

@test "set get" {
    run redis-cli SET foo bar
    assert_output OK
    run redis-cli GET foo
    assert_output bar
    run redis-cli GET none
    assert_output ""
    run redis-cli SET foo baz
    assert_output OK
    run redis-cli GET foo
    assert_output baz
}

@test "set get with expiry" {
    run redis-cli SET a 10
    assert_output OK
    run redis-cli SET b 11 px 100
    assert_output OK
    run redis-cli SET c 12 px 1000
    assert_output OK
    run redis-cli GET a
    assert_output 10
    run redis-cli GET b
    assert_output 11
    run redis-cli GET c
    assert_output 12
    sleep 0.2
    run redis-cli GET a
    assert_output 10
    run redis-cli GET b
    assert_output ""
    run redis-cli GET c
    assert_output 12
}