package com._4paradigm.flowengine.pipeline.sample;

import com._4paradigm.flowengine.pipeline.core.FLFunction;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseRecallFunction extends FLFunction {

    @Override
    public void onInit(Map<String, Object> initConfig) {
        Object es = (String) initConfig.get("es");
        log.info("Prepare es client {} on init", es);
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
    }

    public List<Item> searchFromES(String channel, int start, int end) {
        return IntStream.range(start, end).mapToObj(
                i -> Item.builder().itemId(String.valueOf(i)).channel(channel)
                        .recallScore(Math.random())
                        .algoScore(Math.random()).build())
                .collect(Collectors.toList());
    }
}
