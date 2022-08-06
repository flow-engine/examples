package com._4paradigm.flowengine.pipeline.example;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;

public class HttpUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

    // keep连接持续时间
    public static final int DEFAULT_KEEP_ALIVE_TIME_MILLIS = 20 * 1000;
    // idel 空闲时间
    public static final int IDEL_TIME_S = 5;
    // 第一次间隔执行任务时间
    public static final int DELAY_TIME_S = 10;
    // 定时任务周期时间
    public static final int PERIOD_TIME_S = 10;


    public static HttpClient getHttpClientTime(int timeout) {
        Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", SSLConnectionSocketFactory.getSocketFactory())
                .build();
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(registry);
        connectionManager.setMaxTotal(200);
        //路由是对maxTotal的细分
        connectionManager.setDefaultMaxPerRoute(100);
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(timeout) //服务器返回数据(response)的时间，超过该时间抛出read timeout
                .setConnectTimeout(200)//连接上服务器(握手成功)的时间，超出该时间抛出connect timeout
                .setConnectionRequestTimeout(100)//从连接池中获取连接的超时时间，超过该时间未拿到可用连接，会抛出org.apache.http.conn.ConnectionPoolTimeoutException: Timeout waiting for connection from pool
                .build();

        HttpRequestRetryHandler retryHandler = new HttpRequestRetryHandler() {
            @Override
            public boolean retryRequest(IOException e, int i, HttpContext httpContext) {

                if (i > 0) {
                    LOG.error("retry has more than 0 time, give up request");
                    return false;
                }
                if (e instanceof NoHttpResponseException) {
                    LOG.error("receive no response from server, retry");
                    return true;
                }
                if (e instanceof SSLHandshakeException) {
                    LOG.error("SSL hand shake exception");
                    return false;
                }
                if (e instanceof InterruptedIOException) {
                    LOG.error("InterruptedIOException");
                    return false;
                }
                if (e instanceof UnknownHostException) {
                    LOG.error("server host unknown");
                    return false;
                }
                if (e instanceof ConnectTimeoutException) {
                    LOG.error("connection time out");
                    return false;
                }
                if (e instanceof SSLException) {
                    LOG.error("SSLException");
                    return false;
                }
                HttpClientContext context = HttpClientContext.adapt(httpContext);
                org.apache.http.HttpRequest httpRequest = context.getRequest();
                if (!(httpRequest instanceof HttpEntityEnclosingRequest)) {
                    return true;
                }
                return false;
            }
        };
        HttpClient httpClient = HttpClientBuilder.create()
                .setDefaultRequestConfig(requestConfig).setRetryHandler(retryHandler)
                .setConnectionManager(connectionManager).setKeepAliveStrategy(connectionKeepAliveStrategy())
                .build();
        ScheduledExecutorService monitorExecutor = Executors.newScheduledThreadPool(1);
        monitorExecutor.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                connectionManager.closeExpiredConnections();
                connectionManager.closeIdleConnections(IDEL_TIME_S, TimeUnit.SECONDS);
            }
        }, DELAY_TIME_S, PERIOD_TIME_S, TimeUnit.SECONDS);
        return httpClient;
    }

    public static ClientHttpRequestFactory httpRequestFactoryTimeOut(int timeOut) {
        return new HttpComponentsClientHttpRequestFactory(HttpUtils.getHttpClientTime(timeOut));
    }

    public static ConnectionKeepAliveStrategy connectionKeepAliveStrategy() {
        return (response, context) -> {
            HeaderElementIterator it = new BasicHeaderElementIterator
                    (response.headerIterator(HTTP.CONN_KEEP_ALIVE));
            while (it.hasNext()) {
                HeaderElement he = it.nextElement();
                String param = he.getName();
                String value = he.getValue();

                if (value != null && param.equalsIgnoreCase("timeout")) {
                    return Long.parseLong(value) * 1000;
                }
            }
            return DEFAULT_KEEP_ALIVE_TIME_MILLIS;
        };
    }

}
