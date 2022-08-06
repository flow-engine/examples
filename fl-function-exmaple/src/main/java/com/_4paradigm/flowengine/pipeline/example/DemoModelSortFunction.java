package com._4paradigm.flowengine.pipeline.example;

import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

public class DemoModelSortFunction extends FLFunction {

    private static final Logger LOG = LoggerFactory.getLogger(DemoModelSortFunction.class);

    private static RestTemplate restTemplate;

    private static String predictorUrl;

    public static final ObjectMapper jsonMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(JsonParser.Feature.ALLOW_COMMENTS, true)
            .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
            .setTimeZone(TimeZone.getTimeZone("GMT+8"));

    @Override
    public void onInit(Map<String, Object> initConfig) {
        int timeOut = Integer.valueOf(String.valueOf(initConfig.getOrDefault("http.timeout", 500)));
        restTemplate = new RestTemplate(HttpUtils.httpRequestFactoryTimeOut(timeOut));
        predictorUrl = (String) initConfig.getOrDefault("predictUrl", "");
    }

    public Object run(PipelineContext pipelineContext, Object... objects) {
        Map<String, Object> limits = (Map<String, Object>) objects[0];
//        TypeReference<List<HashMap<String, Object>>> typeRef
//                = new TypeReference<List<HashMap<String, Object>>>() {};
//        List<HashMap<String, Object>> candidates = new ArrayList<>();
//        try {
//            candidates = jsonMapper.readValue(jsonMapper.writeValueAsString(limits.get("list")), typeRef);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        List<HashMap<String, Object>> candidates = (List<HashMap<String, Object>>) limits.get("list");
//        Map<String, Object> runtimeConfig = this.getRuntimeConfig();
        if (StringUtils.isNotEmpty(predictorUrl) && candidates.size() > 0) {
            try {
                LOG.info("start predict, {}", candidates.size());
                Map<String, Object>  predictRequest = new HashMap<>();
                predictRequest.put("accessToken", "mib");
                predictRequest.put("requestId", UUID.randomUUID().toString());
                predictRequest.put("requestTime", 0);
                predictRequest.put("isDebug", false);
                predictRequest.put("isWarmupRequest", false);
                predictRequest.put("resultLimit", candidates.size());
                List<Map<String, Object>> rawInstances = new ArrayList<>();
                for (int i = 0; i < candidates.size(); i++) {
                    Map<String, Object> item = candidates.get(i);
                    Map<String, Object> instance = new HashMap<>();
                    instance.put("id", item.get("id"));
                    Map<String, Object> rawFeatures = new HashMap<>();
                    instance.put("rawFeatures", rawFeatures);
                    rawFeatures.put("cart_original_price", 8250);
                    rawFeatures.put("cart_present_price", 7550);
                    rawFeatures.put("cart_product_list", "101,102,104,205");
                    rawFeatures.put("cart_product_quantity", 4);
                    rawFeatures.put("cart_quantity", 7);
                    rawFeatures.put("currentTime", 1576644307000L);
                    rawFeatures.put("instance_id", "I500000000003890");
                    rawFeatures.put("member_id", "M500000000009753");
                    rawFeatures.put("product_id",(String) item.getOrDefault("product_id", ""));
                    rawFeatures.put("store_id","S50000000000025");
                    rawFeatures.put("trace_id","T5000000000028003");
                    rawInstances.add(instance);
                }
                predictRequest.put("rawInstances", rawInstances);
                LOG.info("predict request, {}", jsonMapper.writeValueAsString(predictRequest));

                HttpHeaders httpHeaders = new HttpHeaders();
                httpHeaders.add("Content-Type", "application/json; charset=UTF-8");
                HttpEntity<String> requestEntity = new HttpEntity<String>(jsonMapper.writeValueAsString(predictRequest), httpHeaders);
                ResponseEntity<String> response;
                try {
                    response = restTemplate
                            .exchange(predictorUrl, HttpMethod.POST, requestEntity, String.class);
                    if (!response.getStatusCode().is2xxSuccessful()) {
                        return candidates;
                    }
                } catch (Exception e) {
                    return candidates;
                }
                JsonNode resp = jsonMapper.readTree(response.getBody());
                if (!resp.get("status").asText().equalsIgnoreCase("ok")) {
                    return candidates;
                }

                LOG.info("predict response, {}", jsonMapper.writeValueAsString(resp));
                JsonNode array = resp.get("instances");
                if (array.isArray()) {
                    for (int i = 0; i < array.size(); i++) {
                        JsonNode resItem = array.get(i);
                        int index = resItem.get("id").asInt()-1;
                        Map<String, Object> item = candidates.get(index);
                        item.put("predictScore", resItem.get("scores").get(0).asDouble());
                    }
                }

                candidates.sort(new Comparator<Map<String, Object>>() {
                    @Override
                    public int compare(Map<String, Object> o1, Map<String, Object> o2) {
                        if (o1 == null && o2 == null) {
                            return 0;
                        }
                        if (o1 == null) {
                            return 1;
                        }
                        if (o2 == null) {
                            return -1;
                        }
                        double click1 = (double) o1.getOrDefault("predictScore", 0.0d);
                        double click2 = (double) o2.getOrDefault("predictScore", 0.0d);

                        if (click1 > click2) {
                            return -1;
                        } else if (click1 < click2) {
                            return 1;
                        } else {
                            return 0;
                        }
                    }
                });
            } catch (Exception e) {
                return candidates;
            }
        }

        return candidates;
    }
}
