package com._4paradigm.flowengine.pipeline.sample;

import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
public class RecallMerge extends FLFunction {

    @Override
    public Object run(PipelineContext pipelineContext, Object... objects) {
        Map<String, Item> result = Maps.newHashMap();
        for (Object object : objects) {
            List<Item> items = (List<Item>) object;
            items.forEach(i -> {
                if (result.containsKey(i.getItemId())) {
                    if (result.get(i.getItemId()).getRecallScore() < i.getRecallScore()) {
                        result.put(i.getItemId(), i);
                    }
                } else {
                    result.put(i.getItemId(), i);
                }
            });
        }
        log.info("merge recall items, total size: {}", result.size());
        return Lists.newArrayList(result.values());
    }
}
