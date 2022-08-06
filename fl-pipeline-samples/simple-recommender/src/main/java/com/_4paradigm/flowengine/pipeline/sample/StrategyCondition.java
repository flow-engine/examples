package com._4paradigm.flowengine.pipeline.sample;


import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import java.util.Map;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class StrategyCondition extends FLFunction {

    /**
     * runtime config sample: { "strategy": "recall", "start": 0.0, "end": 0.7 }
     */
    @Override
    public Object run(PipelineContext pipelineContext, Object... objects) {
        Map<String, Object> runtimeConfig = this.getRuntimeConfig();
        StrategyConfig strategyConfig = (StrategyConfig) objects[0];
        Double start = (Double) runtimeConfig.get("start");
        Double end = (Double) runtimeConfig.get("end");
        Double number = 0.0d;
        String strategy = (String) runtimeConfig.get("strategy");
        switch (strategy) {
            case "recall":
                number = strategyConfig.getRecall();
                break;
            case "sort":
                number = strategyConfig.getSort();
                break;
            case "predict":
                number = strategyConfig.getPredict();
                break;
            case "reRank":
                number = strategyConfig.getReRank();
                break;
            default:
                break;
        }
        boolean result = start <= number && number < end;
        log.info("check strategy {} condition, result: {}", strategy, result);
        return result;
    }
}
