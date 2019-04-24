package ru.mail.polis.impl.simple;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KVServiceImpl implements KVService {

    @NotNull
    private final HttpServer httpServer;
    @NotNull
    private final KVDao dao;

    private static String parseId(@Nullable String query) {
        if (query == null) throw new IllegalArgumentException();
        Pattern compile = Pattern.compile("^id=(.+)");
        Matcher matcher = compile.matcher(query);
        if (matcher.find())
            return matcher.group(1);

        else throw new IllegalArgumentException();
    }

    private static void handleRequest(@NotNull int code, @Nullable byte[] response, HttpExchange http) throws IOException {
        if (response != null) {
            http.sendResponseHeaders(code, response.length);
            http.getResponseBody().write(response);
        } else http.sendResponseHeaders(code, 0);
    }

    private static void handleRequest(@NotNull int code, HttpExchange http) throws IOException {
        handleRequest(code, null, http);
    }

    private static void handleRequestAndClose(@NotNull int code, @Nullable byte[] response, HttpExchange http) throws IOException {
        try {
            handleRequest(code, response, http);
        } finally {
            http.close();
        }
    }

    private static void handleRequestAndClose(@NotNull int code, HttpExchange http) throws IOException {
        handleRequestAndClose(code, null, http);
    }

    public KVServiceImpl(int port,
                         @NotNull final KVDao dao) throws IOException {
        this.httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;

        this.httpServer.createContext("/*", http -> handleRequest(400, http));

        this.httpServer.createContext("/v0/status",
                http -> {
                    String response = "hello";
                    handleRequestAndClose(200, response.getBytes(Charset.forName("UTF-8")), http);
                });

        this.httpServer.createContext("/v0/entity", new ErrorHandler(http -> {
            String id = parseId(http.getRequestURI().getQuery());
            switch (http.getRequestMethod()) {
                case "GET":
                    byte[] response = dao.get(id.getBytes());
                    handleRequestAndClose(200, response, http);
                    break;
                case "DELETE":
                    dao.remove(id.getBytes());
                    handleRequestAndClose(202, null, http);
                    break;
                case "PUT":
                    List<String> headers = http.getRequestHeaders().get("Content-Length");
                    if (headers != null && !headers.isEmpty()) {
                        Integer contentLength = Integer.valueOf(headers.get(0));
                        final byte[] request = new byte[contentLength];
                        if (contentLength != 0 && http.getRequestBody().read(request) != contentLength) {
                            throw new IOException();
                        }
                        dao.upsert(id.getBytes(), request);
                        handleRequestAndClose(201, null, http);
                    }
                    break;
                default:
                    handleRequestAndClose(405, null, http);
            }
        }));
    }

    @Override
    public void start() {
        this.httpServer.start();
    }

    @Override
    public void stop() {
        this.httpServer.stop(0);
    }

    private static class ErrorHandler implements HttpHandler {
        private final HttpHandler delegate;

        ErrorHandler(HttpHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                delegate.handle(exchange);
            } catch (NoSuchElementException ex) {
                handleRequestAndClose(404, exchange);
            } catch (IllegalArgumentException ex) {
                handleRequestAndClose(400, exchange);
            } catch (Exception ex) {

                handleRequestAndClose(500, exchange);
                ex.printStackTrace();
            }
        }
    }

}
