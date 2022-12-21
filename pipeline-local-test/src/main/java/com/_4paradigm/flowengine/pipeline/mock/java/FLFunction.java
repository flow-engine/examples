package com._4paradigm.flowengine.pipeline.mock.java;

import com._4paradigm.flowengine.pipeline.core.PipelineContext;

import java.util.Map;
import java.util.Random;

public class FLFunction {
    Map<String, Object> initConfig;
    Map<String, Object> runtimeConfig;
    public void onInit(Map<String, Object> initConfig) {
    }
    public Object run(PipelineContext pipelineContext, Object... args) {
        Integer rand = new Random().nextInt(100);
        System.out.println("random val is:" + rand);
        return rand;
    }
    public void onDestroy() {
    }
}
