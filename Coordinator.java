import java.io.IOException;
import java.util.HashMap;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Map;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {
    /*
     * The wrapper class of a request
     */
    class Request {
        String type;
        String val;
        long timestamp;
        HttpServerRequest req;

        public Request(String ty, String v, long t) {
            this.type = ty;
            this.val = v;
            this.timestamp = t;
        }
        public Request(String ty, long t, HttpServerRequest r) {
            this.type = ty;
            this.timestamp = t;
            this.req = r;
        }
    }

    // This integer variable tells you what region you are in
    // 1 for US-E, 2 for US-W, 3 for Singapore
    private static int region = KeyValueLib.region;

    // Default mode: Strongly consistent
    // Options: causal, eventual, strong
    private static String consistencyType = "strong";

    /**
     * Set the values of the following variables to the DNS names of your
     * three dataCenter instances. Be sure to match the regions with their DNS!
     * Do the same for the 3 Coordinators as well.
     */
    private static final String dataCenterUSE = "ec2-52-91-249-194.compute-1.amazonaws.com";
    private static final String dataCenterUSW = "ec2-54-152-190-176.compute-1.amazonaws.com";
    private static final String dataCenterSING = "ec2-54-208-224-22.compute-1.amazonaws.com";

    private static final String coordinatorUSE = "ec2-54-85-111-215.compute-1.amazonaws.com";
    private static final String coordinatorUSW = "ec2-54-165-89-60.compute-1.amazonaws.com";
    private static final String coordinatorSING = "ec2-52-91-191-121.compute-1.amazonaws.com";


    // Array of DNSs of coordinators and datacenters
    private static final String[] coordinatorDNSs = new String[3];
    private static final String[] datacenterDNSs = new String[3];

    private static final String GET = "GET";
    private static final String PUT = "PUT";

    private static HashMap<String, BlockingQueue<Request>> map = new HashMap<String, BlockingQueue<Request>>();


    /**
     * Helper function which send put request to datacenters
     */
    private static void putHandler(final String key, final Request r) {
        // This lock is for notification that all datacenters have received the requests.
        final CountDownLatch lock = new CountDownLatch(3);
        try {
            // Send Put to every datacenters.
            for (int i = 0 ; i < 3 ; i++) {
                final String dns = Coordinator.datacenterDNSs[i];
                Thread t = new Thread(new Runnable() {
                    public void run() {
                        try{
                            System.out.println(dns);
                            KeyValueLib.PUT(dns,
                                key, r.val, r.timestamp + "", Coordinator.consistencyType);
                            // Decrease the lock by one.
                            lock.countDown();
                        } catch (IOException e) {e.printStackTrace();}
                    }});
                t.start();
            }
            // Wait until all PUT returns.
            lock.await();
            // Complete all datastores
            KeyValueLib.COMPLETE(key, r.timestamp + "");
        } catch (Exception e) {e.printStackTrace();}
    }

    /**
     * Sum up all characters and get the index of data store
     */
    private static int getHash(String key) {
        int sum = 0;
        for (int i = 0 ; i < key.length() ; i++) {
            sum += (int)key.charAt(i);
        }
        if (sum >= 'a') sum -= 'a';
        return sum % 3 + 1;
    }


    // ==============================================
    /**
     * Add or remove (if r is null) a request to/from the corresponding priority queue.
     * If remove is called but queue is not empty, return false
     */
    private static synchronized boolean addOrRemove(final String key, Request r) {
        if (r != null) {
            if (Coordinator.map.containsKey(key)) {
                Coordinator.map.get(key).add(r);
            } else {
                Coordinator.map.put(key, new PriorityBlockingQueue<Request>(3000, new Comparator<Request>(){
                    @Override
                    public int compare(Request a, Request b) {
                        return (int)(a.timestamp - b.timestamp);
                    }
                }));
                Coordinator.map.get(key).add(r);
                Thread t = new Thread(new Runnable() {
                    public void run() {
                        while(true) {
                            while (Coordinator.map.get(key).size() > 0) {
                                try {
                                    final Request r = Coordinator.map.get(key).take();
                                    // GET request, call KeyValueLib.GET in a new thread
                                    if (r.type.equals(Coordinator.GET)) {
                                        Thread t = new Thread(new Runnable() {
                                            public void run() {
                                                try {
                                                    String output = KeyValueLib.GET(Coordinator.datacenterDNSs[KeyValueLib.region - 1],
                                                        key, r.timestamp + "", consistencyType);
                                                    r.req.response().end(output);
                                                } catch (IOException e) {
                                                    System.out.println("IOException");
                                                }
                                            }});
                                        t.start();
                                    // POST request, call putHandler in a new thread
                                    } else {
                                        Thread t = new Thread(new Runnable() {
                                            public void run() {
                                                String consistency = Coordinator.consistencyType;
                                                putHandler(key, r);
                                            }});
                                        t.start();
                                    }
                                } catch (InterruptedException e) {
                                    System.out.printf("take failed");
                                }
                            }
                            /*
                             *  This is a very important part to avoid race condition.
                             *  It is possible the queue is not empty when it wants to
                             *  remove a queue. So return value is important to determine
                             *  if it is right time to break the while loop.
                             */
                            if (Coordinator.addOrRemove(key, null)) break;
                        }
                    }
                });
                t.start();
            }
        } else {
            if (Coordinator.map.get(key).size() == 0) {
                Coordinator.map.remove(key);
                return true;
            }
            return false;
        }
        return true;
    }


    // ==============================================

    @Override
        public void start() {


            Coordinator.coordinatorDNSs[0] = coordinatorUSE;
            Coordinator.coordinatorDNSs[1] = coordinatorUSW;
            Coordinator.coordinatorDNSs[2] = coordinatorSING;
            Coordinator.datacenterDNSs[0] = dataCenterUSE;
            Coordinator.datacenterDNSs[1] = dataCenterUSW;
            Coordinator.datacenterDNSs[2] = dataCenterSING;

            KeyValueLib.dataCenters.put(dataCenterUSE, 1);
            KeyValueLib.dataCenters.put(dataCenterUSW, 2);
            KeyValueLib.dataCenters.put(dataCenterSING, 3);
            KeyValueLib.coordinators.put(coordinatorUSE, 1);
            KeyValueLib.coordinators.put(coordinatorUSW, 2);
            KeyValueLib.coordinators.put(coordinatorSING, 3);
            final RouteMatcher routeMatcher = new RouteMatcher();
            final HttpServer server = vertx.createHttpServer();
            server.setAcceptBacklog(32767);
            server.setUsePooledBuffers(true);
            server.setReceiveBufferSize(4 * 1024);

            routeMatcher.get("/put", new Handler<HttpServerRequest>() {
                @Override
                public void handle(final HttpServerRequest req) {
                    MultiMap map = req.params();
                    final String key = map.get("key");
                    final String value = map.get("value");
                    final Long timestamp = Long.parseLong(map.get("timestamp"));
                    final String forwarded = map.get("forward");
                    final String forwardedRegion = map.get("region");
                    Thread t = new Thread(new Runnable() {
                        public void run() {
                            /* Add code for PUT request handling here
                             * Each operation is handled in a new thread.
                             * Use of helper functions is highly recommended */

                            // Check if we need to forward the put request
                            int target_coordinator_idx = Coordinator.getHash(key);
                            if (target_coordinator_idx != KeyValueLib.region) {
                                try {
                                    System.out.println(Coordinator.coordinatorDNSs[target_coordinator_idx - 1]);
                                    KeyValueLib.AHEAD(key, timestamp + "");
                                    KeyValueLib.FORWARD(
                                        Coordinator.coordinatorDNSs[target_coordinator_idx - 1],
                                        key, value, timestamp + "");
                                } catch (IOException e) {e.printStackTrace();}
                            } else {
                                if (forwarded != null) {
                                    // If Put is forwared, update the timestamp
                                    final Long newTimestamp = Skews.handleSkew(timestamp, Integer.parseInt(forwardedRegion));
                                    // Add to the queue
                                    Coordinator.addOrRemove(key, new Request(Coordinator.PUT, value, newTimestamp));
                                } else {
                                    try {
                                        KeyValueLib.AHEAD(key, timestamp + "");
                                    } catch (IOException e) {e.printStackTrace();}
                                    // Add to the queue
                                    Coordinator.addOrRemove(key, new Request(Coordinator.PUT, value, timestamp));
                                }
                            }
                        }
                    });
                    t.start();
                    req.response().end(); // Do not remove this
                }
            });

            routeMatcher.get("/get", new Handler<HttpServerRequest>() {
                @Override
                public void handle(final HttpServerRequest req) {
                    MultiMap map = req.params();
                    final String key = map.get("key");
                    final Long timestamp = Long.parseLong(map.get("timestamp"));
                    Thread t = new Thread(new Runnable() {
                        public void run() {
                            /* Add code for GET requests handling here
                             * Each operation is handled in a new thread.
                             * Use of helper functions is highly recommended */

                            // If consistency type is eventual, send get directly.
                            if (consistencyType.equals("eventual")) {
                                try {
                                    String output = KeyValueLib.GET(
                                        Coordinator.datacenterDNSs[KeyValueLib.region - 1],
                                        key, timestamp + "", consistencyType);
                                    req.response().end(output);
                                } catch (IOException e) {
                                    System.out.println("IOException");
                                }
                            } else {
                                // Add to the queue, just like put.
                                Coordinator.addOrRemove(key, new Request(Coordinator.GET, timestamp, req));
                            }
                        }});
                    t.start();
                }
            });
            /* This endpoint is used by the grader to change the consistency level */
            routeMatcher.get("/consistency", new Handler<HttpServerRequest>() {
                @Override
                public void handle(final HttpServerRequest req) {
                    MultiMap map = req.params();
                    consistencyType = map.get("consistency");
                    req.response().end();
                }
            });
            /* BONUS HANDLERS BELOW */
            routeMatcher.get("/forwardcount", new Handler<HttpServerRequest>() {
                @Override
                public void handle(final HttpServerRequest req) {
                    req.response().end(KeyValueLib.COUNT());
                }
            });

            routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
                @Override
                public void handle(final HttpServerRequest req) {
                    System.out.println("========================");
                    KeyValueLib.RESET();
                    req.response().end();
                }
            });

            routeMatcher.noMatch(new Handler<HttpServerRequest>() {
                @Override
                public void handle(final HttpServerRequest req) {
                    req.response().putHeader("Content-Type", "text/html");
                    String response = "Not found.";
                    req.response().putHeader("Content-Length",
                        String.valueOf(response.length()));
                    req.response().end(response);
                    req.response().close();
                }
            });
            server.requestHandler(routeMatcher);
            server.listen(8080);
        }
}
