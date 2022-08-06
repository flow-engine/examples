package com._4paradigm.flowengine.pipeline.core.functions.java.groovy;

import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;

import java.util.Map;

public class JavaFunction extends FLFunction {
    private GroovyObject groovyExecute;

    @Override
    public void onInit(Map<String, Object> initConfig) {
        GroovyClassLoader groovyClassLoader = new GroovyClassLoader(ClassLoader.getSystemClassLoader());

        Class groovyClass = groovyClassLoader.parseClass((String) initConfig.get("script"));
        try {
            this.groovyExecute = (GroovyObject) groovyClass.newInstance();
        } catch (Exception e) {
            System.out.println("init failed:" + e.getMessage());
        }
    }

    @Override
    public Object run(PipelineContext pipelineContext, Object... args) {
        Object[] objects = new Object[]{ pipelineContext, args };
        Object result = groovyExecute.invokeMethod("run", objects);
        return result;
    }
}

