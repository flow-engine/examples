package com._4paradigm.flowengine.pipeline.sample;

import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class RecallNew extends BaseRecallFunction {

    @Override
    public Object run(PipelineContext context, Object... args) {
        log.info("recall new");
        return this.searchFromES("i2i", 10, 20);
    }
}
