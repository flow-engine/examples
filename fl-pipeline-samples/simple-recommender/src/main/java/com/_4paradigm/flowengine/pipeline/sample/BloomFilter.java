package com._4paradigm.flowengine.pipeline.sample;

import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class BloomFilter extends FLFunction {

    @Override
    public Object run(PipelineContext pipelineContext, Object... objects) {
        log.info("do bloom filter");
        return objects[0];
    }
}
