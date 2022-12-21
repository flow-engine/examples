package com._4paradigm.flowengine.pipeline.mock;

import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MockConditionFunction extends FLFunction {

    private int divide;
    private int target;

    @Override
    public void onInit(Map<String, Object> initConfig) {
        this.divide = (Integer) initConfig.get("divide");
        this.target = (Integer) initConfig.get("target");
    }

    @Override
    public Object run(PipelineContext context, Object... args) {
        int i = context.getTraceId().hashCode();
        int result = Math.abs(i) % divide;
        log.error("on condition, func key: {},  hash code: {}, result: {}", this.getKey(), i, result);
        return result == target;
    }
}
