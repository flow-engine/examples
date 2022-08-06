package com._4paradigm.flowengine.pipeline.core.functions.java.init;

import com._4paradigm.cess.sds.spi.bean.storage.EsEndPoint;
import com._4paradigm.flowengine.pipeline.common.data.FLDataClient;
import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import com._4paradigm.flowengine.pipeline.core.aware.Aware;
import com._4paradigm.flowengine.pipeline.core.enums.Dependency;
import org.codehaus.plexus.util.ExceptionUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import shade.guava.collect.Maps;

import java.util.HashMap;
import java.util.Map;

@Aware({Dependency.FL_DATA})
public class JavaFunction extends FLFunction {
    RestHighLevelClient client;
    EsEndPoint userEsEndPoint;

    @Override
    public void onInit(Map<String, Object> initConfig) {
        String projectKey = (String) initConfig.get("sdsProjectKey");
        String userTableName = (String) initConfig.get("sdsUserTableName");
//        this.setFlDataClient(new FLDataClient("http://172.27.128.146:40121/sds-workbench"));
        client=this.getFlDataClient().getEsService(projectKey, userTableName).getEsClient();
        userEsEndPoint = this.getFlDataClient().getFlowengineDataClient().getExtendsTableService().getExtendTableByName(projectKey, userTableName).getExtendDataStage().getEsEndPoint();
    }
    @Override
    public Object run(PipelineContext pipelineContext, Object... objects) {
        pipelineContext.setInnerContextMap("rands", genRandoms());
        pipelineContext.setInnerContextMap("USER", getUserInfo(pipelineContext));

        return objects;
    }

    private Map<String, Object> getUserInfo(PipelineContext pipelineContext) {
        String userId = String.valueOf( pipelineContext.getRequest().get("userId"));
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        QueryBuilder queryBuilder = QueryBuilders.termQuery("userId", userId);
        boolQueryBuilder.must(queryBuilder);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolQueryBuilder);
        SearchRequest request = new SearchRequest(userEsEndPoint.getIndex());
        request.source(sourceBuilder);
        System.out.println(request.toString());
        Map<String, Object> userInfo = Maps.newHashMap();
        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            SearchHit[] sh = response.getHits().getHits();
            if(sh.length>0) {
                userInfo=sh[0].getSourceAsMap();
            }
            return userInfo;
        } catch(Exception e) {
            System.out.println("error:"+ExceptionUtils.getFullStackTrace(e));
            pipelineContext.log(this.getKey(), "error", ExceptionUtils.getFullStackTrace(e));
            return userInfo;
        }
    }

    private Map<String, Double> genRandoms() {
        Map<String, Double> randoms = Maps.newHashMap();
        randoms.put("RECALL", Math.random());
        randoms.put("PRERANK", Math.random());
        randoms.put("RANK", Math.random());
        randoms.put("RERANK", Math.random());
        return randoms;
    }

    public static void main(String[] args) {

        Map<String, Object> initConfig = new HashMap<>();
        initConfig.put("sdsProjectKey", "piper-test" );
        initConfig.put("sdsUserTableName", "test_user_es_1");

        JavaFunction initFunction = new JavaFunction();
        initFunction.onInit(initConfig);

        Map<String, Object> request = new HashMap<>();
        request.put("userId", "1");

        Map<String, Object>  innerCotextMap = Maps.newHashMap();
        PipelineContext context = PipelineContext.builder().request(request).innerContextMap(innerCotextMap).build();
        initFunction.run(context, null);
        System.out.println(context.getInnerContextMap("rands"));
        System.out.println(context.getInnerContextMap("USER"));
    }
}

