package com._4paradigm.flowengine.pipeline.core.functions.java.easyrules.condition;

import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import org.jeasy.rules.api.Facts;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.api.RulesEngine;
import org.jeasy.rules.core.DefaultRulesEngine;
import org.jeasy.rules.mvel.MVELRule;
import org.jeasy.rules.support.composite.ActivationRuleGroup;
import org.mvel2.ParserContext;
import shade.guava.collect.Maps;

import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

public class JavaFunction extends FLFunction {
    private RulesEngine engine;
    private Rules rules;
    private ActivationRuleGroup ruleGroup;
    private String PV_IF_COND_TEMPLATE = "rand=(Double)context.getInnerContextMap(\"rands\").get(\"%s\");rand>=%s&&rand<=%s";
    private String PV_ELSE_COND_TEMPLATE = "rand=(Double)context.getInnerContextMap(\"rands\").get(\"%s\");rand<%s||rand>%s";

    private String IF_ACTION = "output.put(\"result\", true);";
    private String ELSE_ACTION = "output.put(\"result\", false);";

    private String UV_BASE_COND_TEMPLATE = "userId=context.getRequest().get(\"userId\");content=userId+\"%s\";";
    private String UV_CRC_CODE = "crc=new CRC32();crc.reset();crc.update(content.getBytes());num=crc.getValue()%100L;";
    private String UV_IF_JUDGE_TEMPLATE ="num>=%s&&num<=%s";
    private String UV_ELSE_JUDGE_TEMPLATE = "num<%s||num>%s";

    @Override
    public void onInit(Map<String, Object> initConfig) {
        this.engine = new DefaultRulesEngine();
        this.rules = new Rules();
        this.ruleGroup = new ActivationRuleGroup();
        String abType = (String) initConfig.getOrDefault("abType", "PV");
        if(abType.equals("PV")) {
            initPVRules(initConfig);
        } else if (abType.equals("UV")){
            initUVRules(initConfig);
        }
    }

    private void initPVRules(Map<String, Object> initConfig) {
        Double start = Double.valueOf(String.valueOf(initConfig.get("start")));
        Double end = Double.valueOf(String.valueOf(initConfig.get("end")));
        String strategyType = (String) initConfig.get("strategyType");

        String ifCondition = String.format(PV_IF_COND_TEMPLATE, strategyType, start, end);
        String elseCondition = String.format(PV_ELSE_COND_TEMPLATE, strategyType, start, end);
        System.out.println(ifCondition);
        System.out.println(elseCondition);
        MVELRule ifRule = new MVELRule().name("pv_condition_if").priority(0).when(ifCondition).then(IF_ACTION);
        MVELRule elseRule = new MVELRule().name("pv_condition_else").priority(1).when(elseCondition).then(ELSE_ACTION);
        ruleGroup.addRule(ifRule);
        ruleGroup.addRule(elseRule);

        rules.register(ruleGroup);
    }

    private void initUVRules(Map<String, Object> initConfig) {
        Integer start = Integer.valueOf(String.valueOf(initConfig.get("start")));
        Integer end = Integer.valueOf(String.valueOf(initConfig.get("end")));
        String salt = (String) initConfig.getOrDefault("salt", "");

        String ifCondition = String.format(UV_BASE_COND_TEMPLATE, salt) + UV_CRC_CODE + String.format(UV_IF_JUDGE_TEMPLATE,start, end);
        String elseCondition = String.format(UV_BASE_COND_TEMPLATE, salt) + UV_CRC_CODE + String.format(UV_ELSE_JUDGE_TEMPLATE, start, end);
        System.out.println(ifCondition);
        System.out.println(elseCondition);
        ParserContext parserContext = new ParserContext();
        parserContext.addImport("CRC32", CRC32.class);
        MVELRule ifRule = new MVELRule(parserContext).name("uv_condition_if").priority(0).when(ifCondition).then(IF_ACTION);
        MVELRule elseRule = new MVELRule(parserContext).name("uv_condition_else").priority(1).when(elseCondition).then(ELSE_ACTION);
        ruleGroup.addRule(ifRule);
        ruleGroup.addRule(elseRule);

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
        return output.get("result");
    }
    public static void main(String[] args) {
        Map<String, Object> initConfig = new HashMap<>();
        initConfig.put("start", 0.5);
        initConfig.put("end", 0.7);
        initConfig.put("strategyType", "RECALL");
        initConfig.put("abType", "PV");

        JavaFunction javaFunction = new JavaFunction();
        javaFunction.onInit(initConfig);
        Map<String, Object> innerContextMap = Maps.newHashMap();
        Map<String, Double> randoms = Maps.newHashMap();
        randoms.put("RECALL", 0.1);
        innerContextMap.put("rands", randoms);
        Map<String, Object> request = Maps.newHashMap();
        request.put("userId", "wys");
        PipelineContext pipelineContext = PipelineContext.builder().request(request).innerContextMap(innerContextMap).build();
        Boolean result = (Boolean)javaFunction.run(pipelineContext, null);
        System.out.println(result);
    }
}
