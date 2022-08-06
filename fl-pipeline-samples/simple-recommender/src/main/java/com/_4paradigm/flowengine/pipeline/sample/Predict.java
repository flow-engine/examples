package com._4paradigm.flowengine.pipeline.sample;

import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class Predict extends FLFunction {

    @Override
    public Object run(PipelineContext context, Object... params) {
        List<Item> items = (List<Item>) params[0];
        items.forEach(i -> i.setAlgoScore(ThreadLocalRandom.current().nextDouble()));
        items.sort(Comparator.comparingDouble(Item::getRecallScore).reversed());
        log.info("predict by algo score");
        return items;
    }
}
