package com._4paradigm.flowengine.pipeline.core.functions.java.groovy.merge;

import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JavaFunction extends FLFunction {
    private static ObjectMapper objectMapper = new ObjectMapper();
    private GroovyObject groovyExecute;
    private static String SIZE_FUNC_TEMPLATE = "def size() {\n" +
            "    %s\n" +
            "}\n";
    private static String ORDER_FUNC_TEMPLATE = "def order() {\n" +
            "    \"%s\"\n" +
            "}\n";
    private static String BASESCORE_FUNC_TEMPLATE = "def basescore(item) {\n" +
            "    %s\n" +
            "}\n";
    private static String WEIGHTS_FUNC_TEMPLATE = "def weights() {\n" +
            "    %s\n" +
            "}\n";
    private static String EXEC_FUNC = "def mergeItems(items) {\n" +
            "    items.inject([:], {cache, item-> if(cache) cache['merge_score']+=item['merge_score'] else cache = item;return cache})\n" +
            "}\n" +
            "def merge(lists) {\n" +
            "    def l = lists.inject([], {cache,list->cache+list})\n" +
            "    def groupedlist = l.groupBy {item->item['itemId']}\n" +
            "    return groupedlist.inject([], {cache, items->cache+= mergeItems(items.value)})\n" +
            "}\n" +
            "def sort(list) {\n" +
            "    list.sort {a,b-> if(order() == \"ASC\") {a['merge_score']<=>b['merge_score'] } else {b['merge_score']<=>a['merge_score']}}\n" +
            "}\n" +
            "def weightedscore(lists,weights) {\n" +
            "    return lists.eachWithIndex {list,index-> list.each{item->item['merge_score']=basescore(item)*weights[index]}}\n" +
            "}\n" +
            "def cut(list) {\n" +
            "    def s=size()\n" +
            "    if(s>=0) {\n" +
            "        list=list.subList(0, Math.min(list.size(), s))\n" +
            "    }\n" +
            "    return list\n" +
            "}\n" +
            "def execute(context, args) {\n" +
            "    cut(sort(merge(weightedscore(args, weights()))))\n" +
            "}";
    @Override
    public void onInit(Map<String, Object> initConfig) {
        GroovyClassLoader groovyClassLoader = new GroovyClassLoader(ClassLoader.getSystemClassLoader());
        String scoreExpr = (String) initConfig.get("basescore");
        String order = (String) initConfig.getOrDefault("order", "DESC");
        String weights = ((List) initConfig.get("weights")).toString();
        Integer size = (Integer)initConfig.getOrDefault("size", -1);
        String script = String.format(SIZE_FUNC_TEMPLATE, size)
                + String.format(WEIGHTS_FUNC_TEMPLATE, weights)
                + String.format(ORDER_FUNC_TEMPLATE, order)
                + String.format(BASESCORE_FUNC_TEMPLATE, scoreExpr)
                + EXEC_FUNC;
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

    public static void main(String[] args) throws IOException {
        Map<String, Object> initConfig = Maps.newHashMap();
        initConfig.put("basescore", "1");
        initConfig.put("order", "DESC");
        List<Double> weights = new ArrayList<>();
        weights.add(0.3);
        weights.add(0.7);
        initConfig.put("weights", weights);
        initConfig.put("size" , 3);
        JavaFunction groovyFunction = new JavaFunction();
        groovyFunction.onInit(initConfig);

        List<Map<String, Object>> l1 = Lists.newArrayList();
        l1.add(getItem(new Item("item1", 10.0)));
        l1.add(getItem(new Item("item2", 0.8)));
        List<Map<String, Object>> l2 = Lists.newArrayList();
        l2.add(getItem(new Item("item2", 0.5)));
        l2.add(getItem(new Item("item3", 1.0)));
        Object[] arguments = new Object[]{l1,l2};
        List result = (List) groovyFunction.run(null, arguments);
        System.out.println(result.toString());
    }

    private static Map<String, Object> getItem(Item item) throws IOException {
        return objectMapper.readValue(objectMapper.writeValueAsString(item), Map.class);
    }

    @AllArgsConstructor
    @Data
    public static class Item {
        private String itemId;
        private Double score;
    }
}
