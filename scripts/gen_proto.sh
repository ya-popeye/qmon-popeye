#!/bin/sh
myhome=$(dirname $0)
protoc --proto_path $myhome/../core/src/main/java/ --java_out $myhome/../core/src/main/java $myhome/../core/src/main/java/popeye/transport/proto/message.proto
