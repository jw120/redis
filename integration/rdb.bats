# Tests server reads a sample rdb file
#
# Server assumed to be running on the default port having been given appropriate dir and dbfilename

setup() {
    load "test_helper/bats-support/load"
    load "test_helper/bats-assert/load"
}

@test "ping" {
    run redis-cli PING
    assert_output PONG
}

@test "config" {
    run redis-cli CONFIG GET dir
    assert_line --index 0 dir
    assert_line --index 1 ../integration
    run redis-cli CONFIG GET dbfilename
    assert_line --index 0 dbfilename
    assert_line --index 1 read_sample.rdb
}

@test "keys *" {
    run redis-cli KEYS "*"
    assert_output --partial mykey
    assert_output --partial anotherkey
}


@test "get values" {
    run redis-cli GET mykey
    assert_output myval
    run redis-cli GET anotherkey
    assert_output 42
}
