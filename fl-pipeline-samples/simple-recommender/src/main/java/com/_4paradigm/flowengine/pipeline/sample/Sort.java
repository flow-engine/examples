package com._4paradigm.flowengine.pipeline.sample;

import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import java.util.Comparator;
import java.util.List;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class Sort extends FLFunction {

    @Override
    public Object run(PipelineContext context, Object... params) {
        List<Item> recall = (List<Item>) params[0];
        recall.sort(Comparator.comparingDouble(Item::getRecallScore).reversed());
        log.info("sort by recall score");
        return recall;
    }
}
