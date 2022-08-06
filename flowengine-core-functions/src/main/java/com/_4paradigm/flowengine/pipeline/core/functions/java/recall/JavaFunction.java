package com._4paradigm.flowengine.pipeline.core.functions.java.recall;

import com._4paradigm.cess.sds.spi.bean.storage.EsEndPoint;
import com._4paradigm.flowengine.pipeline.common.data.FLDataClient;
import com._4paradigm.flowengine.pipeline.core.FLFunction;
import com._4paradigm.flowengine.pipeline.core.PipelineContext;
import com._4paradigm.flowengine.pipeline.core.aware.Aware;
import com._4paradigm.flowengine.pipeline.core.enums.Dependency;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import shade.guava.collect.Lists;
import shade.guava.collect.Maps;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Aware({Dependency.FL_DATA})
public class JavaFunction extends FLFunction {
    private static BigDecimal MAX_DOUBLE = new BigDecimal(Double.MAX_VALUE);
    private static BigDecimal MIN_DOUBLE = new BigDecimal(Double.MIN_VALUE);
    RestHighLevelClient client;
    private ObjectMapper objectMapper =new ObjectMapper();
    private Boolean sqlCase = false;
    private String translatedQuery;
    private String exprBuiltQuery;
    private EsEndPoint esEndPoint;

    @Override
    public void onInit(Map<String, Object> initConfig) {
        String projectKey =(String) initConfig.get("sdsProjectKey");
        String tableName = (String) initConfig.get("sdsItemTableName");
//        this.setFlDataClient(new FLDataClient("http://172.27.128.146:40121/sds-workbench"));
        if(StringUtils.isNotEmpty(projectKey)) {
            client = this.getFlDataClient().getEsService(projectKey, tableName).getEsClient();
        } else {
            Object projectId = initConfig.get("sdsProjectId");
            if(projectId!=null) {
                System.out.println("projectId:"+projectId);
                client = this.getFlDataClient().getEsService((int)projectId, tableName).getEsClient();
            }
        }
        esEndPoint = this.getFlDataClient().getFlowengineDataClient().getExtendsTableService().getExtendTableByName(projectKey, tableName).getExtendDataStage().getEsEndPoint();
        if(initConfig.get("sql")!=null) {
            sqlCase = true;
            String sql = (String) initConfig.get("sql");
            sql = sql.replace("${INDEX}", esEndPoint.getIndex());
            this.translatedQuery = translateSqlToESDsl(sql);
            if(translatedQuery == null) {
                throw new IllegalArgumentException("translate sql to es dsl failed");
            }
        } else if(initConfig.get("expr")!=null){
            try {
                JsonNode rule = objectMapper.readTree((String)initConfig.get("expr"));
                QueryBuilder queryBuilder = buildQuery(rule);
                exprBuiltQuery = queryBuilder.toString();
            } catch (IOException e) {
            }
        }

    }
    @Override
    public Object run(PipelineContext pipelineContext, Object... objects) {
        if(sqlCase) {
            return runSqlQuery(pipelineContext, objects);
        } else {
            return runExprQuery(pipelineContext, objects);
        }
    }

    @Override
    public void onDestroy() {
        try {
            client.close();
        } catch (IOException e) {
        }
    }

    private List<Map<String, Object>> runExprQuery(PipelineContext pipelineContext, Object... objects) {
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        if(exprBuiltQuery == null) {
            return result;
        }

        String query = parseValue(exprBuiltQuery, pipelineContext);
        QueryBuilder queryBuilder = QueryBuilders.wrapperQuery(query);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);
        SearchRequest request = new SearchRequest(esEndPoint.getIndex());
        request.source(sourceBuilder);

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            SearchHit[] sh = response.getHits().getHits();
            for(int i=0;i<sh.length;i++) {
                Map<String,Object> hit = sh[i].getSourceAsMap();
                result.add(hit);
            }

            return result;
        } catch(Exception e) {
            return result;
        }
    }

    private List<Map<String, Object>> runSqlQuery(PipelineContext pipelineContext, Object... objects) {
        if(translatedQuery == null) {
            return null;
        }

        String query = parseValue(translatedQuery, pipelineContext);
        Request sqlEsRequest = getSQLTranslatedRequest(esEndPoint.getIndex(), query);
        try {
            Response response = client.getLowLevelClient().performRequest(sqlEsRequest);
            String result = IOUtils.toString(response.getEntity().getContent());
            System.out.println("result:"+result);
            JsonNode resultNode =objectMapper.readTree(result);
            JsonNode hits = resultNode.get("hits").get("hits");
            List<Map<String, Object>> items = Lists.newArrayList();
            for (JsonNode node : hits){
                Map<String, Object> item =objectMapper.convertValue(node.get("_source"), Map.class);;
                items.add(item);
            }
            return items;
        } catch (Exception e) {
            return null;
        }

    }

    public QueryBuilder buildQuery(JsonNode rule) {
        BoolQueryBuilder base = QueryBuilders.boolQuery();
        if(rule.has("conditions")) {
            JsonNode conditions = rule.get("conditions");
            JsonNode connection = rule.get("connector");
            for(JsonNode condition : conditions) {
                if(connection.textValue().equals("AND")) {
                    base = base.must(buildQuery(condition));
                } else if (connection.textValue().equals("OR")){
                    base = base.should(buildQuery(condition));
                }
            }

            return base;
        } else {
            String field = rule.get("field").textValue();
            String operator = rule.get("operator").textValue();
            List<Object> values = objectMapper.convertValue(rule.get("values"), List.class);
            switch (operator) {
                case "IN":
                    return QueryBuilders.termsQuery(field, values.stream().map(obj->SystemType2ESType(obj)).collect(Collectors.toList()));
                case "BETWEEN":
                    return QueryBuilders.rangeQuery(field).gte(values.get(0)).lte(values.get(1));
                case "GTE":
                    return QueryBuilders.rangeQuery(field).gte(values.get(0));
                case "LTE":
                    return QueryBuilders.rangeQuery(field).lte(values.get(0));
                case "EQUAL":
                    return QueryBuilders.termQuery(field, values.get(0));
                case "NOT_EQUAL":
                    return QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(field, values.get(0)));
                case "GT":
                    return QueryBuilders.rangeQuery(field).gt(values.get(0));
                case "LT":
                    return QueryBuilders.rangeQuery(field).lt(values.get(0));
                case "CONTAINS":
                    return QueryBuilders.wildcardQuery(field, "*" + values.get(0) + "*");
                case "NOT_CONTAINS":
                    return QueryBuilders.boolQuery().mustNot(QueryBuilders.wildcardQuery(field, "*" + values.get(0) + "*"));
                case "NOT_NULL":
                    return QueryBuilders.existsQuery(field);
                case "NULL":
                    return QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(field));
                case "NOT_EMPTY":
                    return QueryBuilders.boolQuery()
                            .should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(field)))
                            .should(QueryBuilders.termQuery(field, ""));
                case "EMPTY":
                    return QueryBuilders.boolQuery()
                            .must(QueryBuilders.existsQuery(field))
                            .mustNot(QueryBuilders.termQuery(field, ""));
                case "REGEX_MATCH":
                    return QueryBuilders.regexpQuery(field, values.get(0).toString());
                case "REGEX_NOT_MATCH":
                    return QueryBuilders.boolQuery().mustNot(QueryBuilders.regexpQuery(field, values.get(0).toString()));
                default:
                    throw new RuntimeException(String.format("unknown operator type for [%s]", operator));
            }
        }
    }
    public Object SystemType2ESType(Object obj) {
        if (obj instanceof Date) {
            return ((Date) obj).getTime();
        } else if (obj instanceof BigDecimal) {
            return convertDecimal2Double((BigDecimal) obj);
        } else {
            return obj;
        }
    }

    public double convertDecimal2Double(BigDecimal decimal) {
        double v;
        if (decimal.compareTo(MAX_DOUBLE) > 0) {
            v = Double.MAX_VALUE;
        } else if (decimal.compareTo(MIN_DOUBLE) < 0) {
            v = Double.MIN_VALUE;
        } else {
            v = decimal.doubleValue();
        }
        return v;
    }

    private String parseValue(String expression, PipelineContext pipelineContext) {
        Pattern pattern = Pattern.compile("\\$\\{([^}])*\\}");
        Matcher matcher = pattern.matcher(expression);

        StringBuffer buffer = new StringBuffer();
        while(matcher.find()) {
            String matchStr = matcher.group();
            String value = parseValueFromContext(matchStr, pipelineContext);
            matcher.appendReplacement(buffer, value);
        }

        matcher.appendTail(buffer);
        return buffer.toString();
    }

    private String parseValueFromContext(String expr, PipelineContext pipelineContext) {
        expr = expr.substring(2, expr.length()-1);
        String entity = expr.split("\\.")[0];
        String field = expr.split("\\.")[1];
        Map<String, Object> entityInfo = (Map)pipelineContext.getInnerContextMap(entity);
        if(entityInfo.containsKey(field)) {
            return String.valueOf(entityInfo.get(field));
        } else {
            return null;
        }
    }


    private Request getSQLTranslatedRequest(String index, String esQuery) {
        Request request = new Request("POST", "/"+index+"/_search/");
        request.setJsonEntity(esQuery);
        return request;
    }

    private String translateSqlToESDsl(String sql) {
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("query", sql);

        Request request = new Request("POST", "/_sql/translate");
        request.setJsonEntity(objectNode.toString());
        try {
            Response response = client.getLowLevelClient().performRequest(request);
            String expr = IOUtils.toString(response.getEntity().getContent());
            objectNode = (ObjectNode)objectMapper.readTree(expr);
            objectNode.put("_source", true);
            objectNode.remove("stored_fields");
            objectNode.remove("docvalue_fields");
            System.out.println(objectNode.toString());
            return objectNode.toString();
        } catch (IOException e) {
            return null;
        }
    }


    public static void main(String[] args) throws IOException {
        Map<String, Object> initConfig = new HashMap<String, Object>();
        initConfig.put("sdsProjectKey", "piper-test");
//        initConfig.put("sdsProjectId", 1);
        initConfig.put("sdsItemTableName", "test_recall_es_1");
        initConfig.put("sql", "SELECT (click+price) as cprice,* FROM ${INDEX} where brand ='${USER.brand}' and click > '${USER.click}'");

        Map<String, Object> user = Maps.newHashMap();
        user.put("brand", "brand1");
        user.put("click", 100);

        PipelineContext pipelineContext = PipelineContext.builder().innerContextMap(Maps.<String, Object>newConcurrentMap()).build();
        pipelineContext.setInnerContextMap("USER", user);

        JavaFunction javaFunction = new JavaFunction();
        javaFunction.onInit(initConfig);
        List<Map<String, Object>> result = (List)javaFunction.run(pipelineContext, null);
        System.out.println("result:"+result.toString());
        javaFunction.client.close();

    }
}

