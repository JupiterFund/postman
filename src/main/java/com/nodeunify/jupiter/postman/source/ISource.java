package com.nodeunify.jupiter.postman.source;

import com.google.protobuf.GeneratedMessageV3;

import io.reactivex.Flowable;

public interface ISource {
    public Flowable<GeneratedMessageV3> readStream() throws Exception;
}
