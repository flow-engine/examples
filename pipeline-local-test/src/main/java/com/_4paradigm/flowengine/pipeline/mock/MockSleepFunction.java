package com._4paradigm.flowengine.pipeline.mock;

import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MockSleepFunction extends FLFunction {

    private int duration;

    @Override
    public void onInit(Map<String, Object> initConfig) {
        this.duration = (Integer) initConfig.getOrDefault("duration", 100);
    }

    @Override
    public Object run(PipelineContext context, Object... args) {
        log.error("func key: {}, start to sleep for {} ms", this.getKey(), duration);
        try {
            TimeUnit.MILLISECONDS.sleep(duration);
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.error("func key: {}, finish sleep for {} ms", this.getKey(), duration);
        return String.format("sleep %s ms", duration);
    }
}
