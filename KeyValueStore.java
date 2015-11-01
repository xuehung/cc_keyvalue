import java.util.Comparator;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.Collections;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class KeyValueStore extends Verticle {
	private HashMap<String, ArrayList<StoreValue>> store = null;
	private HashMap<String, Semaphore> keyLock = null;

	public KeyValueStore() {
		store = new HashMap<String, ArrayList<StoreValue>>();
		keyLock = new HashMap<String, Semaphore>();
	}

    private synchronized Semaphore getLock(String key) {
        if (!keyLock.containsKey(key)) {
            Semaphore lock = new Semaphore(1);
            keyLock.put(key, lock);
        }
        return keyLock.get(key);
    }

    private synchronized void addValue(String key, StoreValue val) {
        if (!store.containsKey(key)) {
            store.put(key, new ArrayList<StoreValue>());
        }
        store.get(key).add(val);
    }

	@Override
	public void start() {
		final KeyValueStore keyValueStore = new KeyValueStore();
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);
		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				String value = map.get("value");
				String consistency = map.get("consistency");
				Integer region = Integer.parseInt(map.get("region"));

				Long timestamp = Long.parseLong(map.get("timestamp"));

System.out.println(String.format("[put]%s/%s/%d\n", key, value, timestamp));


				/* TODO: You will need to adjust the timestamp here for some consistency levels */
				StoreValue sv = new StoreValue(timestamp, value);

				/* TODO: Add code to store the object here. You may need to adjust the timestamp */
                		addValue(key, sv);

System.out.println(String.format("[put.finish]%s/%s/%d\n", key, value, timestamp));

				String response = "stored";
				req.response().putHeader("Content-Type", "text/plain");
				req.response().putHeader("Content-Length",
						String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
			}
		});
		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				String consistency = map.get("consistency");
				final Long timestamp = Long.parseLong(map.get("timestamp"));

				Thread t = new Thread(new Runnable() {
					public void run() {

				/* TODO: Add code here to get the list of StoreValue associated with the key
				 * Remember that you may need to implement some locking on certain consistency levels */
				System.out.println(String.format("[get]%s/%d\n", key, timestamp));

		                Semaphore lock = getLock(key);
				try {
					System.out.println(String.format("[get.lock]%s/%d\n", key, timestamp));
			                lock.acquire();
				} catch (InterruptedException e) {e.printStackTrace();}
				ArrayList<StoreValue> values = store.get(key);
				Collections.sort(values, new Comparator<StoreValue>(){
					@Override
                    			public int compare(StoreValue a, StoreValue b) {
                        			return (int)(a.getTimestamp() - b.getTimestamp());
                    			}
				});

				/* Do NOT change the format the response. It will return a string of
				 * values separated by spaces */
				String response = "";
				if (values != null) {
					for (StoreValue val : values) {
						response = response + val.getValue() + " ";
					}
				}

                		lock.release();


				req.response().putHeader("Content-Type", "text/plain");
				if (response != null)
					req.response().putHeader("Content-Length",
							String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
					}
				});
				t.start();
			}
		});
		// Handler for when the AHEAD is called
		routeMatcher.get("/ahead", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));

System.out.println(String.format("[ahead]%s/%d\n", key, timestamp));

                Semaphore lock = getLock(key);
		try {
                lock.acquire();
		} catch (InterruptedException e) {e.printStackTrace();}
System.out.println(String.format("[ahead.lock]%s/%d\n", key, timestamp));


				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
			}
		});
		// Handler for when the COMPLETE is called
		routeMatcher.get("/complete", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				final Long timestamp = Long.parseLong(map.get("timestamp"));

System.out.println(String.format("[complete]%s/%d\n", key, timestamp));

                Semaphore lock = getLock(key);
                lock.release();
System.out.println(String.format("[complete.release]%s/%d\n", key, timestamp));


				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
			}
		});
		// Clears this stored keys. Do not change this
		routeMatcher.get("/reset", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				store.clear();
				keyLock.clear();
				req.response().putHeader("Content-Type", "text/plain");
				req.response().end();
				req.response().close();
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
