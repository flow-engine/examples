package com._4paradigm.flowengine.pipeline.mock;

import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class MockLogFunction extends FLFunction {

    @Override
    public Object run(PipelineContext context, Object... args) {
        Map<String, Object> body = new HashMap<>();
        body.put("reqId", "demo_request_id");
        body.put("eventTime", Instant.now().toEpochMilli());
        Map<String, Object> reqFeas = new HashMap<>();
        reqFeas.put("userId", "1");
        reqFeas.put("serviceId", "1");
        body.put("reqFeas", reqFeas);
        List<Map<String, Object>> items = new LinkedList<>();
        for (int i = 0; i <= 30; i++) {
            Map<String, Object> item = new HashMap<>();
            item.put("itemId", String.valueOf(i));
            Map<String, Object> itemFeas = new HashMap<>();
            itemFeas.put("weight", ThreadLocalRandom.current().nextDouble());
            itemFeas.put("page", 0);
            itemFeas.put("rank", i);
            item.put("itemFeas", itemFeas);
            items.add(item);
        }
        body.put("items", items);
        System.out.println("sfdsfdfff");
        context.log(this.getKey(), "test-log", body);
        return body;
    }
}
