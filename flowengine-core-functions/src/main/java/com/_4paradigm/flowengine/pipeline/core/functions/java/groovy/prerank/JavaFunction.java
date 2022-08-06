package com._4paradigm.flowengine.pipeline.core.functions.java.groovy.prerank;

import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;
import shade.guava.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JavaFunction extends FLFunction {
    private GroovyObject groovyExecute;
    private static String SIZE_FUNC_TEMPLATE = "def size() {\n" +
                                            "    %s\n" +
                                            "}\n";
    private static String ORDER_FUNC_TEMPLATE = "def order() {\n" +
                                            "    \"%s\"\n" +
                                            "}\n";
    private static String SCORE_FUNC_TEMPLATE = "def score(item) {\n" +
                                                "    %s\n" +
                                                "}\n";
    private static String EXEC_FUNC = "def execute(context, args) {\n" +
                                    "    def l=args[0]\n" +
                                    "    def s = size()\n" +
                                    "    l=l.sort {a,b-> if(order() == \"ASC\") {score(a)<=>score(b) } else {score(b)<=>score(a)}}\n" +
                                    "    if(s >= 0) {\n" +
                                    "        l=l.subList(0, Math.min(l.size(),s))\n" +
                                    "    }\n" +
                                    "    return l\n" +
                                    "}";
    @Override
    public void onInit(Map<String, Object> initConfig) {
        GroovyClassLoader groovyClassLoader = new GroovyClassLoader(ClassLoader.getSystemClassLoader());
        String scoreExpr = (String) initConfig.get("score");
        String order = (String) initConfig.getOrDefault("order", "DESC");
        Integer size = (Integer) initConfig.getOrDefault("size", -1);
        String script = String.format(SIZE_FUNC_TEMPLATE, size) +
                String.format(ORDER_FUNC_TEMPLATE, order) +
                String.format(SCORE_FUNC_TEMPLATE, scoreExpr) +
                EXEC_FUNC;
        System.out.println(script);
        Class groovyClass = groovyClassLoader.parseClass(script);
        try {
            this.groovyExecute = (GroovyObject) groovyClass.newInstance();
        } catch (Exception e) {
            System.out.println("init failed:" + e.getMessage());
        }
    }

    @Override
    public Object run(PipelineContext pipelineContext, Object... args) {
        Object[] objects = new Object[]{ pipelineContext, args };
        Object result = groovyExecute.invokeMethod("execute", objects);
        return result;
    }

    public static void main(String[] args) {
        Map<String, Object> initConfig = Maps.newHashMap();
        initConfig.put("score", "item");
        initConfig.put("order", "DESC");
        initConfig.put("size",-1);
        JavaFunction groovyFunction = new JavaFunction();
        groovyFunction.onInit(initConfig);

        List<Integer> nums = new ArrayList<>();
        nums.add(3);
        nums.add(1);
        nums.add(2);
        Object[] arguments = new Object[]{nums};
        List result = (List)groovyFunction.run(null, arguments);
        for(Object obj : result) {
            System.out.println(obj);
        }
    }
}
