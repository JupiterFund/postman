package com.nodeunify.jupiter.postman.source;

import com.google.protobuf.GeneratedMessageV3;
import com.gta.qts.c2j.adaptee.IGTAQTSCallbackBase;

import io.reactivex.FlowableEmitter;

public interface IGTAQTSCallbackExtension extends IGTAQTSCallbackBase {
    public void setEmitter(FlowableEmitter<GeneratedMessageV3> emitter);
}
