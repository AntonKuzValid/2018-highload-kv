package ru.mail.polis.impl.fluent;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import one.nio.util.Utf8;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import ru.mail.polis.common.CustomHttpCluster;
import ru.mail.polis.common.NotEnoughReplicaException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static one.nio.http.Request.*;
import static one.nio.http.Response.*;

public class KVServiceImpl extends HttpServer implements KVService {

    @NotNull
    private final KVDao dao;

    @NotNull
    private final CustomHttpCluster cluster;

    private static final Pattern REPLICA_PATTERN = Pattern.compile("(\\d+)/(\\d+)");
    private static final String SERVER_HEADER = "server: true";

    public KVServiceImpl(@NotNull KVDao dao, @NotNull Set<String> topology, HttpServerConfig config, Object... routers) throws IOException {
        super(config, routers);
        this.dao = dao;
        this.cluster = initCluster(topology, config.acceptors);
    }

    private CustomHttpCluster initCluster(@NotNull Set<String> topology, @NotNull AcceptorConfig[] acceptors) throws IOException {
        if (acceptors.length == 1) {
            String host = acceptors[0].address;
            int port = acceptors[0].port;
            CustomHttpCluster cluster = new CustomHttpCluster(host + ":" + port);
            cluster.configure(topology);
            return cluster;
        } else throw new IllegalArgumentException("No configured acceptors");
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        try {
            super.handleRequest(request, session);
        } catch (NoSuchElementException ex) {
            session.sendResponse(new Response(NOT_FOUND, new byte[0]));
        } catch (IllegalArgumentException ex) {
            session.sendResponse(new Response(BAD_REQUEST, new byte[0]));
        } catch (NotEnoughReplicaException ex) {
            session.sendResponse(new Response(GATEWAY_TIMEOUT, new byte[0]));
        } catch (Exception ex) {
            session.sendResponse(new Response(INTERNAL_ERROR, new byte[0]));
            ex.printStackTrace();
        }
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        Response response = new Response(NOT_FOUND);
        session.sendResponse(response);
    }

    @Override
    public void start() {
        System.out.println("Server started");
        super.start();
    }

    @Override
    public void stop() {
        System.out.println("Server stopped");
        super.stop();
    }

    @Path("/v0/status")
    public Response handleStatus() {
        return Response.ok(Utf8.toBytes("OK"));
    }

    @Path("/v0/entity")
    public Response handleEntity(Request request,
                                 @Param("id") String id,
                                 @Param("replicas") String replicas,
                                 @Header("server") Boolean isServer
    ) throws IOException {
        if (id == null || id.isEmpty()) throw new IllegalArgumentException();
        Response reponse;
        switch (request.getMethod()) {
            case METHOD_GET:
                byte[] response = dao.get(id.getBytes());
                reponse = Response.ok(response);
                break;
            case METHOD_DELETE:
                dao.remove(id.getBytes());
                reponse = new Response(ACCEPTED, new byte[0]);
                break;
            case METHOD_PUT:
                byte[] body = request.getBody();
                dao.upsert(id.getBytes(), body);
                reponse = new Response(CREATED, new byte[0]);
                break;
            default:
                reponse = new Response(METHOD_NOT_ALLOWED, new byte[0]);
        }
        if (isServer == null || !isServer) handleEntityOnCluster(request, replicas);
        return reponse;
    }

    private void handleEntityOnCluster(Request req, String replicas) {
        AdditionalInformation addInf = getAdditionalInformation(replicas);
        req.addHeader(SERVER_HEADER);
        List<Response> responses = cluster.invokeAll(req, addInf.from - 1);
        int success = 1, bad = 0, error = 0;
        for (Response res : responses) {
            int status = res.getStatus();
            if (status >= 200 && status < 400) success++;
            else if (status >= 400 && status < 500) bad++;
            else if (status >= 500) error++;
        }

        if (success + bad + error < addInf.from)
            throw new NotEnoughReplicaException("From is " + addInf.from + " but succedd + bad - " + success + bad);
        if (success < addInf.ack) throw new NoSuchElementException();
    }

    private AdditionalInformation getDefaultAdditionalInformation() {
        int from = cluster.getCurrentProvidersSize() + 1;
        int ack = from / 2 + 1;
        return new AdditionalInformation(ack, from);
    }


    private AdditionalInformation getAdditionalInformation(@Nullable String replicas) {
        if (replicas == null) return getDefaultAdditionalInformation();
        Matcher matcher = REPLICA_PATTERN.matcher(replicas);
        if (matcher.find()) {
            int ack = Integer.parseInt(matcher.group(1));
            int from = Integer.parseInt(matcher.group(2));
            return new AdditionalInformation(ack, from);
        }
        throw new IllegalArgumentException("Incorrect format of replicas");
    }

    private static class AdditionalInformation {
        private int ack;
        private int from;

        AdditionalInformation(int ack, int from) {
            this.ack = ack;
            this.from = from;
        }
    }


}
