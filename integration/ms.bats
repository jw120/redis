# Tests master and slave behaviour
#
# Master server assumed to be running on default port with read_sample.rdb
# Replica server assumed to be running on 6380 as replica of the master

setup() {
    load "test_helper/bats-support/load"
    load "test_helper/bats-assert/load"
}

@test "ping master" {
    run redis-cli PING
    assert_output PONG
}

@test "ping replica" {
    run redis-cli -p 6380 PING
    assert_output PONG
}

@test "info master" {
   run redis-cli INFO replication
   assert_output --partial "role:master"
   assert_output --partial "connected_slaves:1"
   assert_output --partial "master_replid"
   assert_output --partial "master_repl_offset"
}

@test "info slave" {
   run redis-cli -p 6380 INFO replication
   assert_output --partial "role:slave"
}

@test "set on master" {
    run redis-cli SET foo 123
    assert_output OK
    run redis-cli -p 6380 GET foo
    assert_output 123
}
