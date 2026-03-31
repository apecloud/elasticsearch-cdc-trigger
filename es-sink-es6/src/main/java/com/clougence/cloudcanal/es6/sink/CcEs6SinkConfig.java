package com.clougence.cloudcanal.es6.sink;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CcEs6SinkConfig {

    private String sourceHosts;

    private String sourceUser;

    private String sourcePassword;

    private String sourceIndexName = com.clougence.cloudcanal.es_base.EsTriggerConstant.ES_TRIGGER_IDX;

    private String startTime;

    private int pollIntervalMs = 1000;

    private int idlePollIntervalMs = 3000;

    private int errorBackoffMs = 10000;

    private int batchSize = 200;
}
