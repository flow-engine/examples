package com._4paradigm.flowengine.pipeline.core.functions.java.easyrules;

import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import com._4paradigm.flowengine.pipeline.core.aware.Aware;
import com._4paradigm.flowengine.pipeline.core.enums.Dependency;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.jeasy.rules.api.Facts;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.api.RulesEngine;
import org.jeasy.rules.core.DefaultRulesEngine;
import org.jeasy.rules.mvel.MVELRule;
import org.jeasy.rules.support.composite.ActivationRuleGroup;

import java.util.HashMap;
import java.util.Map;

@Aware({Dependency.FL_DATA})
public class JavaFunction extends FLFunction {
    public static final ObjectMapper objectMapper = new ObjectMapper();
    private RulesEngine engine;
    private Rules rules;
    private ActivationRuleGroup ruleGroup;

    @Override
    public void onInit(Map<String, Object> initConfig) {
        this.engine = new DefaultRulesEngine();
        this.rules = new Rules();
        this.ruleGroup = new ActivationRuleGroup();
        String rulesStr =(String) initConfig.get("rules");
        JsonNode rootNode = null;
        try {
            rootNode = objectMapper.readTree(rulesStr);
        } catch (Exception e) {
            System.out.println("json process error:"+e.getMessage());
        }
        String functionKey = this.getKey();
        if(functionKey == null || functionKey == "") {
            functionKey = "general_easy_rules";
        }

        for(int i=0;i<rootNode.size();i++) {
            JsonNode ruleNode = rootNode.get(i);
            JsonNode conditionNode = ruleNode.get("condition");
            JsonNode actionNode = ruleNode.get("action");
            if(conditionNode == null || actionNode == null) {
                continue;
            }
            MVELRule mvelRule = new MVELRule()
                    .name(functionKey+"_"+i)
                    .priority(i)
                    .when(conditionNode.asText())
                    .then(actionNode.asText());
            ruleGroup.addRule(mvelRule);
        }
        rules.register(ruleGroup);
    }

    @Override
    public Object run(PipelineContext pipelineContext, Object... args) {
        if(args == null) {
            args = new Object[]{};
        }
        Facts facts = new Facts();
        facts.put("context", pipelineContext);
        facts.put("args", args);
        if(this.getFlDataClient() != null) {
            facts.put("flDataClient", this.getFlDataClient());
        }

        Map<String, Object> output = new HashMap<String, Object>();
        facts.put("output", output);

        engine.fire(rules, facts);
        return output;
    }
    public static void main(String[] args) {
        ArrayNode arrayNode = objectMapper.createArrayNode();
        ObjectNode ruleNode = objectMapper.createObjectNode();
        ruleNode.put("condition", "System.out.println(\"rule1\");args[0]<args[1]");
        ruleNode.put("action", "output.put(\"result\", true)");
        arrayNode.add(ruleNode);

        ObjectNode ruleNode2 = objectMapper.createObjectNode();
        ruleNode2.put("condition", "System.out.println(\"rule2\");args[0]>=args[1]");
        ruleNode2.put("action", "output.put(\"result\", false)");
        arrayNode.add(ruleNode2);

        ObjectNode ruleNode3 = objectMapper.createObjectNode();
        ruleNode3.put("condition", "System.out.println(\"rule3\");args[0]>=args[1]");
        ruleNode3.put("action", "output.put(\"result\", true)");

        Map<String, Object> initConfig = new HashMap<>();
        initConfig.put("rules", arrayNode.toString());
        Object[] arguments = new Object[] {51,50};

        JavaFunction javaFunction = new JavaFunction();
        javaFunction.onInit(initConfig);
        PipelineContext pipelineContext = PipelineContext.builder().build();
        Map<String, Object> result = (Map)javaFunction.run(pipelineContext, arguments);
        System.out.println(result.get("result"));
    }
}
