package ru.mail.polis.common;

import one.nio.cluster.ServiceProvider;
import one.nio.cluster.ServiceUnavailableException;
import one.nio.http.HttpCluster;
import one.nio.http.HttpProvider;
import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static one.nio.http.Response.INTERNAL_ERROR;

public class CustomHttpCluster extends HttpCluster {

    @NotNull
    private String localhost;

    private static final Response ERROR_REPONSE = new Response(INTERNAL_ERROR, new byte[0]);

    public CustomHttpCluster(@NotNull String localhost) {
        super();
        this.localhost = localhost;
    }

    public void configure(Set<String> configuration) throws IOException {
        Map<HttpProvider, Integer> newProviders = createProviders(configuration);
        List<HttpProvider> oldProviders = replaceProviders(newProviders);
        for (HttpProvider provider : oldProviders) {
            provider.close();
        }
    }

    public int getCurrentProvidersSize() {
        return getProviders().size();
    }

    private Map<HttpProvider, Integer> createProviders(Set<String> configuration) throws IOException {
        HashMap<HttpProvider, Integer> providers = new HashMap<>();
        int weight = 1;
        for (String providerString : configuration) {
            if (localhost.equals(getHostAndPort(providerString))) {
                continue;
            }
            HttpProvider provider = createProvider(providerString);
            providers.put(provider, weight);
        }
        return providers;
    }

    private String getHostAndPort(@NotNull String providerString) {
        Pattern pattern = Pattern.compile(".*://(.+):(.+)");
        Matcher matcher = pattern.matcher(providerString);
        if (matcher.find()) {
            String host = matcher.group(1);
            String port = matcher.group(2);
            if ("localhost".equals(host)) host = "0.0.0.0";
            return host + ":" + port;
        }
        throw new RuntimeException();
    }


    public List<Response> invokeAll(Request request, int from) {
        if (from <= 0) return new ArrayList<>();
        if (log.isTraceEnabled()) {
            log.trace(request.toString());
        }

        final int retries = this.retries;
        List<HttpProvider> providers = getProviders();
        List<Response> responses = new ArrayList<>();
        int cnt = 0;
        for (HttpProvider provider : providers) {
            Response response = null;
            for (int i = 0; i < retries; i++) {
                try {
                    response = provider.invoke(request);
                    provider.getFailures().set(0);
                } catch (Exception e) {
                    if (provider.getFailures().incrementAndGet() >= maxFailures) {
                        disableProvider(provider);
                    }
                    if ((e instanceof SocketTimeoutException || e.getCause() instanceof SocketTimeoutException) && !log.isTraceEnabled()) {
                        log.debug(provider + " timed out");
                    } else {
                        log.warn(provider + " invocation failed", e);
                    }
                }
            }
            if (response == null) responses.add(ERROR_REPONSE);
            else responses.add(response);
            if (++cnt == from) break;
        }
        return responses;
    }

    @SuppressWarnings("unchecked")
    private List<HttpProvider> getProviders() {
        return (List<HttpProvider>) (List<?>) Arrays.<ServiceProvider>asList(providerSelector.providers);
    }

}
