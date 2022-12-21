package com._4paradigm.flowengine.pipeline.mock;

import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MockHookFunction extends FLFunction {

    private String hook;

    @Override
    public void onInit(Map<String, Object> initConfig) {
        this.hook = (String) initConfig.get("hook");
    }

    @Override
    public Object run(PipelineContext context, Object... args) {
        log.error("trace id: {}, hook: {}, func key: {}", context.getTraceId(), this.hook, this.getKey());
        return hook;
    }
}
