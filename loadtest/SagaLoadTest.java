/*
 * End-to-end load harness for the order fulfillment saga.
 *
 * Deliberately a single JDK-only source file: it runs with `java loadtest/SagaLoadTest.java`, so
 * measuring the platform never requires installing a load-testing tool or adding a module to the
 * build.
 *
 *   java loadtest/SagaLoadTest.java --orders 2000 --concurrency 64
 *
 * What it measures:
 *   submit  - how long POST /orders takes. This is the only latency a caller waits on, because
 *             the order is accepted as soon as it and its outbox row commit.
 *   settle  - how long until the order reaches a terminal state (SHIPPED or CANCELLED). This is
 *             the saga's real cost: five services and six topics after the response was returned.
 *
 * Settle time is sampled by polling GET /orders/{id}, so it is quantised to the poll interval and
 * is an upper bound. Reported numbers should be read as "no worse than".
 */

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class SagaLoadTest {

    private static final Duration POLL_INTERVAL = Duration.ofMillis(50);
    private static final Duration SETTLE_TIMEOUT = Duration.ofSeconds(120);

    public static void main(String[] args) throws Exception {
        Map<String, String> options = parseArgs(args);
        int orders = Integer.parseInt(options.getOrDefault("orders", "1000"));
        int concurrency = Integer.parseInt(options.getOrDefault("concurrency", "32"));
        String baseUrl = options.getOrDefault("base-url", "http://localhost:8081");
        String productId = options.getOrDefault("product", "SKU-1001");

        System.out.printf("Submitting %d orders at concurrency %d against %s%n",
                orders, concurrency, baseUrl);

        HttpClient http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        List<Long> submitNanos = java.util.Collections.synchronizedList(new ArrayList<>(orders));
        List<Long> settleNanos = java.util.Collections.synchronizedList(new ArrayList<>(orders));
        LongAdder submitFailures = new LongAdder();
        java.util.concurrent.atomic.AtomicReference<Exception> firstFailure =
                new java.util.concurrent.atomic.AtomicReference<>();
        LongAdder settleTimeouts = new LongAdder();
        Map<String, String> terminalStates = new java.util.concurrent.ConcurrentHashMap<>();

        // Phase one: submit as fast as the pool allows, and nothing else.
        //
        // Submitting and polling on the same thread would make every worker spend most of its time
        // waiting on GETs, and the resulting "throughput" would describe how fast the saga settles
        // rather than how many orders the service can accept. Separating the phases keeps the two
        // numbers meaning what their names say.
        Map<String, Long> startedAt = new java.util.concurrent.ConcurrentHashMap<>();
        ExecutorService submitters = Executors.newFixedThreadPool(concurrency);
        CountDownLatch submitted = new CountDownLatch(orders);

        long submitPhaseStart = System.nanoTime();
        for (int i = 0; i < orders; i++) {
            submitters.execute(() -> {
                try {
                    long start = System.nanoTime();
                    String orderId = submit(http, baseUrl, productId);
                    submitNanos.add(System.nanoTime() - start);
                    startedAt.put(orderId, start);
                } catch (Exception e) {
                    submitFailures.increment();
                    // Report the first failure in full. A harness that silently counts errors will
                    // happily report "0 samples" for a configuration problem and leave you
                    // guessing which end was broken.
                    if (firstFailure.compareAndSet(null, e)) {
                        System.err.println("First failure:");
                        e.printStackTrace();
                    }
                } finally {
                    submitted.countDown();
                }
            });
        }
        submitted.await();
        submitters.shutdown();
        submitters.awaitTermination(1, TimeUnit.MINUTES);
        double submitSeconds = (System.nanoTime() - submitPhaseStart) / 1e9;
        System.out.printf("Submitted %d orders in %.2f s; waiting for the saga to settle%n",
                startedAt.size(), submitSeconds);

        // Phase two: sweep every outstanding order once per round.
        //
        // Blocking on one order at a time would be much simpler and completely wrong: with 2000
        // orders across 64 threads, each thread owns about 30 orders and does not look at the
        // second until the first is terminal. Orders late in a thread's queue settle long before
        // anyone checks, and the measurement charges them all the waiting. Sweeping bounds the
        // observation error at roughly one round for every order.
        java.util.Set<String> outstanding = java.util.concurrent.ConcurrentHashMap.newKeySet();
        outstanding.addAll(startedAt.keySet());

        ExecutorService watchers = Executors.newFixedThreadPool(concurrency);
        long settleDeadline = System.nanoTime() + SETTLE_TIMEOUT.toNanos();
        int lastReported = 0;

        while (!outstanding.isEmpty() && System.nanoTime() < settleDeadline) {
            List<java.util.concurrent.Callable<Void>> sweep = new ArrayList<>(outstanding.size());
            for (String orderId : outstanding) {
                sweep.add(() -> {
                    try {
                        String state = pollStatus(http, baseUrl, orderId);
                        if ("SHIPPED".equals(state) || "CANCELLED".equals(state)) {
                            settleNanos.add(System.nanoTime() - startedAt.get(orderId));
                            terminalStates.merge(state, "1",
                                    (a, b) -> String.valueOf(Integer.parseInt(a) + 1));
                            outstanding.remove(orderId);
                        }
                    } catch (Exception ignored) {
                        // Transient read failure; the next sweep picks the order up again.
                    }
                    return null;
                });
            }
            watchers.invokeAll(sweep);

            int settledSoFar = orders - outstanding.size();
            if (settledSoFar - lastReported >= 200) {
                lastReported = settledSoFar;
                System.out.printf("  %d/%d settled%n", settledSoFar, orders);
            }
            if (!outstanding.isEmpty()) {
                Thread.sleep(POLL_INTERVAL.toMillis());
            }
        }
        outstanding.forEach(id -> settleTimeouts.increment());
        watchers.shutdown();
        watchers.awaitTermination(1, TimeUnit.MINUTES);
        double totalSeconds = (System.nanoTime() - submitPhaseStart) / 1e9;

        System.out.println();
        System.out.println("=== Results ===");
        System.out.printf("orders                %d%n", orders);
        System.out.printf("concurrency           %d%n", concurrency);
        System.out.printf("submit phase          %.2f s%n", submitSeconds);
        System.out.printf("submit throughput     %.1f orders/s accepted%n",
                startedAt.size() / submitSeconds);
        System.out.printf("total wall clock      %.2f s%n", totalSeconds);
        System.out.printf("saga throughput       %.1f orders/s fulfilled%n",
                settleNanos.size() / totalSeconds);
        System.out.printf("submit failures       %d%n", submitFailures.sum());
        System.out.printf("settle timeouts       %d (after %ds)%n",
                settleTimeouts.sum(), SETTLE_TIMEOUT.toSeconds());
        System.out.printf("terminal states       %s%n", terminalStates);
        System.out.println();
        report("submit (POST /orders)", submitNanos);
        report("settle (accepted -> terminal)", settleNanos);
    }

    private static String submit(HttpClient http, String baseUrl, String productId) throws Exception {
        String body = """
                {"userId":"load-user","productId":"%s","quantity":1,"totalAmount":19.99}
                """.formatted(productId);

        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/orders"))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException("submit failed: HTTP " + response.statusCode());
        }
        return extract(response.body(), "orderId");
    }

    private static String pollStatus(HttpClient http, String baseUrl, String orderId)
            throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/orders/" + orderId))
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        return extract(response.body(), "status");
    }

    /** Minimal field reader, so the harness stays dependency-free. */
    private static String extract(String json, String field) {
        String needle = "\"" + field + "\":\"";
        int start = json.indexOf(needle);
        if (start < 0) {
            return null;
        }
        start += needle.length();
        return json.substring(start, json.indexOf('"', start));
    }

    private static void report(String label, List<Long> samples) {
        if (samples.isEmpty()) {
            System.out.printf("%-30s no samples%n", label);
            return;
        }
        long[] sorted = samples.stream().mapToLong(Long::longValue).sorted().toArray();
        System.out.printf("%s  (n=%d)%n", label, sorted.length);
        System.out.printf("  p50 %8.1f ms%n", millis(percentile(sorted, 0.50)));
        System.out.printf("  p95 %8.1f ms%n", millis(percentile(sorted, 0.95)));
        System.out.printf("  p99 %8.1f ms%n", millis(percentile(sorted, 0.99)));
        System.out.printf("  max %8.1f ms%n", millis(sorted[sorted.length - 1]));
        System.out.println();
    }

    private static long percentile(long[] sorted, double q) {
        int index = (int) Math.ceil(q * sorted.length) - 1;
        return sorted[Math.min(Math.max(index, 0), sorted.length - 1)];
    }

    private static double millis(long nanos) {
        return nanos / 1e6;
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].startsWith("--")) {
                options.put(args[i].substring(2), args[i + 1]);
            }
        }
        if (Arrays.asList(args).contains("--help")) {
            System.out.println("Usage: java loadtest/SagaLoadTest.java "
                    + "[--orders N] [--concurrency N] [--base-url URL] [--product SKU]");
            System.exit(0);
        }
        return options;
    }
}
