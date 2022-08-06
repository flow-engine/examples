package com._4paradigm.flowengine.pipeline.sample;

import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import java.util.List;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class ReRank extends FLFunction {

    @Override
    public Object run(PipelineContext context, Object... args) {
        log.info("do re-rank, take top 5 items");
        List<Item> items = (List<Item>) args[0];
        return items.subList(0, 5);
    }

}
