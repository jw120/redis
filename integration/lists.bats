# Tests for base server functionality
#
# Server assumed to be running on the default port

setup() {
    load "test_helper/bats-support/load"
    load "test_helper/bats-assert/load"
}

@test "RPUSH single value" {
    run redis-cli DEL list_key
    assert_output "OK"
    run redis-cli RPUSH list_key foo
    assert_output "1"
    run redis-cli RPUSH list_key bar
    assert_output "2"
    run redis-cli LRANGE list_key 0 1
    assert_line --index 0 foo
    assert_line --index 1 bar
    run redis-cli LLEN list_key
    assert_output 2
}

@test "RPUSH multiple values" {
    run redis-cli DEL list_key
    assert_output "OK"
    run redis-cli RPUSH list_key foo pqr
    assert_output 2
    run redis-cli RPUSH list_key bar cat car
    assert_output 5
    run redis-cli LRANGE list_key 0 4
    assert_line --index 0 foo
    assert_line --index 1 pqr
    assert_line --index 2 bar
    assert_line --index 3 cat
    assert_line --index 4 car
    run redis-cli LLEN list_key
    assert_output 5
}

@test "LPUSH single value" {
    run redis-cli DEL list_key
    assert_output "OK"
    run redis-cli LPUSH list_key foo
    assert_output "1"
    run redis-cli LPUSH list_key bar
    assert_output "2"
    run redis-cli LRANGE list_key 0 1
    assert_line --index 0 bar
    assert_line --index 1 foo
    run redis-cli LLEN list_key
    assert_output 2
}

@test "LPUSH multiple values" {
    run redis-cli DEL list_key
    assert_output "OK"
    run redis-cli LPUSH list_key foo pqr
    assert_output 2
    run redis-cli LPUSH list_key bar cat car
    assert_output 5
    run redis-cli LRANGE list_key 0 4
    assert_line --index 0 car
    assert_line --index 1 cat
    assert_line --index 2 bar
    assert_line --index 3 pqr
    assert_line --index 4 foo
    run redis-cli LLEN list_key
    assert_output 5
    
}

@test "LRANGE empty value" {
    run redis-cli LRANGE unknown 2 3
    refute_output
}

@test "LRANGE normal" {
    run redis-cli DEL list_key
    assert_output "OK"
    run redis-cli RPUSH list_key a b c d e f
    assert_output 6
    run redis-cli LRANGE list_key 1 4
    assert_line --index 0 b
    assert_line --index 1 c
    assert_line --index 2 d
    assert_line --index 3 e
}

@test "LRANGE start out of tange" {
    run redis-cli DEL list_key
    assert_output "OK"
    run redis-cli RPUSH list_key a b c d e f
    assert_output 6
    run redis-cli LRANGE list_key 7 10
    refute_output
}

@test "LRANGE stop out of range" {
    run redis-cli DEL list_key
    assert_output "OK"
    run redis-cli RPUSH list_key a b c d e f
    assert_output 6
    run redis-cli LRANGE list_key 2 9
    assert_line --index 0 c
    assert_line --index 1 d
    assert_line --index 2 e
    assert_line --index 3 f
}

@test "LRANGE start after stop" {
    run redis-cli DEL list_key
    assert_output "OK"
    run redis-cli RPUSH list_key a b c d e f
    assert_output 6
    run redis-cli LRANGE list_key 4 2
    refute_output
}

@test "LRANGE negatives" {
    run redis-cli DEL list_key
    assert_output "OK"
    run redis-cli RPUSH list_key a b c d e f
    assert_output 6
    run redis-cli LRANGE list_key -4 -2
    assert_line --index 0 c
    assert_line --index 1 d
    assert_line --index 2 e
}

@test "LRANGE negatives saturating" {
    run redis-cli DEL list_key
    assert_output "OK"
    run redis-cli RPUSH list_key a b c d e f
    assert_output 6
    run redis-cli LRANGE list_key -10 -3
    assert_line --index 0 a
    assert_line --index 1 b
    assert_line --index 2 c
    assert_line --index 3 d
}


@test "LLEN missings {
    run redis-cli DEL list_key
    assert_output "OK"
    run redis-cli LLEN list_key
    assert_output 0
    run redis-cli SET list_key abc
    assert_output "OK"
}

@test "LPOP single {
    run redis-cli DEL list_key
    assert_output "OK"
    run redis-cli RPUSH list_key a b c d e
    assert_output 5
    run redis-cli LPOP list_key
    assert_output a
    run redis-cli LPOP list_key
    assert_output b
    run redis-cli LRANGE list_key 0 -1
    assert_line --index 0 c
    assert_line --index 1 d
    assert_line --index 2 e
}

@test "LPOP multiple" {
    run redis-cli DEL list_key
    assert_output "OK"
    run redis-cli RPUSH list_key a b c d e
    assert_output 5
    run redis-cli LPOP list_key 3
    assert_line --index 0 a
    assert_line --index 1 b
    assert_line --index 2 c
    run redis-cli LRANGE list_key 0 -1
    assert_line --index 0 d
    assert_line --index 1 e
}

@test "LPOP multiple overflow" {
    run redis-cli DEL list_key
    assert_output "OK"
    run redis-cli RPUSH list_key a b c
    assert_output 3
    run redis-cli LPOP list_key 4
    assert_line --index 0 a
    assert_line --index 1 b
    assert_line --index 2 c
    run redis-cli LLEN list_key
    assert_output 0
}


