package com.mdg.gateway.producer;

/** Which stage of the pipeline produced a dead letter. */
public enum FailureStage {

    /** Raw frame could not be parsed or failed canonical validation. Deterministic. */
    TRANSFORM,

    /** Canonical event was valid but the broker would not accept it. Usually transient. */
    PUBLISH
}
