package com._4paradigm.flowengine.pipeline.sample;

import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import java.util.concurrent.ThreadLocalRandom;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class GenerateConfig extends FLFunction {

    @Override
    public Object run(PipelineContext pipelineContext, Object... objects) {
        StrategyConfig strategyConfig = new StrategyConfig(newDouble(), newDouble(), newDouble(),
                newDouble());
        log.info("generate strategy random number: {}", strategyConfig);
        return strategyConfig;
    }


    private Double newDouble() {
        return ThreadLocalRandom.current().nextDouble();
    }
}
