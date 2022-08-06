package com._4paradigm.flowengine.pipeline.sample;

import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import java.util.HashMap;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class PipelineTest {

    @Test
    public void testPipeline() {
        GenerateConfig generateConfig = new GenerateConfig();
        RecallNew recallNew = new RecallNew();
        RecallU2I recallU2I = new RecallU2I();
        RecallI2I recallI2I = new RecallI2I();
        BloomFilter bloomFilter = new BloomFilter();
        RecallMerge recallMerge = new RecallMerge();
        Sort sort = new Sort();
        Predict predict = new Predict();
        ReRank reRank = new ReRank();

        HashMap<String, Object> recallAConditionConfig = new HashMap<>();
        recallAConditionConfig.put("strategy", "recall");
        recallAConditionConfig.put("start", 0.0);
        recallAConditionConfig.put("end", 0.5);
        StrategyCondition recallACondition = new StrategyCondition();
        recallACondition.setRuntimeConfig(recallAConditionConfig);
        HashMap<String, Object> recallBConditionConfig = new HashMap<>();
        recallBConditionConfig.put("strategy", "recall");
        recallBConditionConfig.put("start", 0.5);
        recallBConditionConfig.put("end", 1.0);
        StrategyCondition recallBCondition = new StrategyCondition();
        recallBCondition.setRuntimeConfig(recallBConditionConfig);

        HashMap<String, Object> es = new HashMap<>();
        es.put("es", "127.0.0.1:9200");
        recallNew.onInit(es);
        recallI2I.onInit(es);
        recallU2I.onInit(es);

        HashMap<String, Object> request = new HashMap<>();
        PipelineContext context = PipelineContext.create("trace_id", request);
        request.put("user_id", "1");

        Object strategyConfig = generateConfig.run(context);
        Object recall = null;
        if ((Boolean) recallACondition.run(context, strategyConfig)) {
            Object newRecall = recallNew.run(context, request);
            Object i2iRecall = recallI2I.run(context, request);
            recall = recallMerge.run(context, newRecall, i2iRecall);
        } else if ((Boolean) recallBCondition.run(context, strategyConfig)) {
            Object i2iRecall = recallI2I.run(context, request);
            Object u2iRecall = recallU2I.run(context, request);
            recall = recallMerge.run(context, i2iRecall, u2iRecall);
        }
        Object sortResult = sort.run(context, recall);
        Object predictResult = predict.run(context, sortResult);
        Object reRankResult = reRank.run(context, predictResult);
        log.info("result: {}", reRankResult);
    }
}