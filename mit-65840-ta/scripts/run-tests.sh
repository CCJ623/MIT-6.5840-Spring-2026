#!/bin/bash

# Simple test runner for MIT 6.5840 labs.

if [ -z "$1" ]; then
    echo "Usage: $0 <lab-name> [test-regex]"
    echo "Example: $0 raft1 TestInitialElection"
    exit 1
fi

LAB=$1
TEST_REGEX=$2

cd src || exit 1

if [ -n "$TEST_REGEX" ]; then
    make RUN="-run $TEST_REGEX" "$LAB"
else
    make "$LAB"
fi
