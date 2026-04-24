import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ETL Pipeline — Quiz Leaderboard Aggregator
 *
 * Extracts scored events from a polling API, deduplicates by (roundId, participant),
 * aggregates total scores per participant, and submits the sorted leaderboard.
 *
 * Zero external dependencies — Java 11+ standard library only.
 *
 * @author Vimmy Roy
 */
public class Main {

    // ─── Constants ───────────────────────────────────────────────────────────────

    private static final String REG_NO       = "RA2311003020635";
    private static final String BASE_URL     = "https://devapigw.vidalhealthtpa.com/srm-quiz-task";
    private static final String POLL_PATH    = "/quiz/messages";
    private static final String SUBMIT_PATH  = "/quiz/submit";

    private static final int TOTAL_POLLS     = 10;          // polls 0..9
    private static final int POLL_DELAY_MS   = 5_000;       // mandatory 5-second gap
    private static final int HTTP_TIMEOUT_S  = 15;          // per-request timeout

    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    // Regex: captures roundId, participant, and score from each event object.
    // Handles optional whitespace and works on minified or pretty-printed JSON.
    private static final Pattern EVENT_PATTERN = Pattern.compile(
            "\"roundId\"\\s*:\\s*\"([^\"]+)\"\\s*,\\s*" +
            "\"participant\"\\s*:\\s*\"([^\"]+)\"\\s*,\\s*" +
            "\"score\"\\s*:\\s*(\\d+)"
    );

    // ─── Domain Model ────────────────────────────────────────────────────────────

    /**
     * Immutable value object representing a participant's aggregated score.
     */
    static final class PlayerScore implements Comparable<PlayerScore> {
        private final String participant;
        private final int totalScore;

        PlayerScore(String participant, int totalScore) {
            this.participant = participant;
            this.totalScore  = totalScore;
        }

        String getParticipant() { return participant; }
        int    getTotalScore()  { return totalScore;  }

        /** Natural ordering: descending by totalScore. */
        @Override
        public int compareTo(PlayerScore other) {
            return Integer.compare(other.totalScore, this.totalScore);
        }

        @Override
        public String toString() {
            return participant + " → " + totalScore;
        }
    }

    // ─── Entry Point ─────────────────────────────────────────────────────────────

    public static void main(String[] args) {

        LOG.info("ETL Pipeline started | regNo=" + REG_NO);

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(HTTP_TIMEOUT_S))
                .build();

        // Phase 1 — Extract & Deduplicate
        Set<String>          seen   = new HashSet<>();
        Map<String, Integer> scores = new HashMap<>();

        for (int poll = 0; poll < TOTAL_POLLS; poll++) {
            fetchPollData(client, poll, seen, scores);

            if (poll < TOTAL_POLLS - 1) {
                sleep(POLL_DELAY_MS);
            }
        }

        if (scores.isEmpty()) {
            LOG.warning("No scored events collected after " + TOTAL_POLLS + " polls. Aborting submission.");
            return;
        }

        // Phase 2 — Aggregate & Sort
        List<PlayerScore> leaderboard = buildSortedLeaderboard(scores);
        LOG.info("Leaderboard assembled | participants=" + leaderboard.size()
                + " | uniqueEvents=" + seen.size());

        // Phase 3 — Load (Submit)
        submitResults(client, leaderboard);

        LOG.info("ETL Pipeline completed.");
    }

    // ─── Extract ─────────────────────────────────────────────────────────────────

    /**
     * Fetches a single poll, extracts events via regex, deduplicates on the fly,
     * and accumulates scores into the shared map.
     *
     * @param client reusable HttpClient
     * @param poll   poll index (0–9)
     * @param seen   set of "roundId|participant" keys already processed
     * @param scores running participant → totalScore accumulator
     */
    private static void fetchPollData(HttpClient client, int poll,
                                       Set<String> seen, Map<String, Integer> scores) {
        String url = BASE_URL + POLL_PATH + "?regNo=" + REG_NO + "&poll=" + poll;

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(HTTP_TIMEOUT_S))
                    .GET()
                    .build();

            LOG.info("[POLL " + poll + "] → GET " + url);

            HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());

            int status = response.statusCode();
            if (status < 200 || status >= 300) {
                LOG.warning("[POLL " + poll + "] Non-2xx response: HTTP " + status);
                return;
            }

            String body = response.body();
            if (body == null || body.isBlank()) {
                LOG.warning("[POLL " + poll + "] Empty response body.");
                return;
            }

            processAndDeduplicate(body, poll, seen, scores);

        } catch (java.io.IOException e) {
            LOG.log(Level.SEVERE, "[POLL " + poll + "] I/O error: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.log(Level.SEVERE, "[POLL " + poll + "] Request interrupted.", e);
        }
    }

    // ─── Transform ───────────────────────────────────────────────────────────────

    /**
     * Parses the raw JSON response body, deduplicates events by composite key
     * (roundId + participant), and aggregates scores.
     */
    private static void processAndDeduplicate(String json, int poll,
                                               Set<String> seen,
                                               Map<String, Integer> scores) {
        Matcher matcher = EVENT_PATTERN.matcher(json);
        int extracted   = 0;
        int duplicates  = 0;

        while (matcher.find()) {
            String roundId     = matcher.group(1);
            String participant = matcher.group(2);
            int    score       = Integer.parseInt(matcher.group(3));

            // Composite key for O(1) deduplication
            String key = roundId + "|" + participant;

            if (seen.add(key)) {                       // returns false if already present
                scores.merge(participant, score, Integer::sum);
                extracted++;
            } else {
                duplicates++;
            }
        }

        LOG.info("[POLL " + poll + "] << new=" + extracted + "  dup=" + duplicates);
    }

    /**
     * Converts the score map into a sorted list of PlayerScore objects
     * (descending by totalScore).
     */
    private static List<PlayerScore> buildSortedLeaderboard(Map<String, Integer> scores) {
        List<PlayerScore> leaderboard = new ArrayList<>(scores.size());
        scores.forEach((p, s) -> leaderboard.add(new PlayerScore(p, s)));
        leaderboard.sort(null);   // uses PlayerScore.compareTo (descending)
        return leaderboard;
    }

    // ─── Load ────────────────────────────────────────────────────────────────────

    /**
     * Serialises the sorted leaderboard to JSON and POSTs it to the submission endpoint.
     */
    private static void submitResults(HttpClient client, List<PlayerScore> leaderboard) {
        String payload = buildSubmitJson(leaderboard);
        LOG.info("Submitting payload (" + payload.length() + " bytes) → POST " + BASE_URL + SUBMIT_PATH);
        LOG.info("Payload:\n" + payload);

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(BASE_URL + SUBMIT_PATH))
                    .timeout(Duration.ofSeconds(HTTP_TIMEOUT_S))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build();

            HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());

            LOG.info("Submission response [HTTP " + response.statusCode() + "]:\n" + response.body());

        } catch (java.io.IOException e) {
            LOG.log(Level.SEVERE, "Submission I/O error: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.log(Level.SEVERE, "Submission interrupted.", e);
        }
    }

    /**
     * Hand-builds a JSON string without any external serialisation library.
     *
     * Output format:
     * <pre>
     * {
     *   "regNo": "RA2311003020635",
     *   "leaderboard": [
     *     { "participant": "Alice", "totalScore": 120 },
     *     ...
     *   ]
     * }
     * </pre>
     */
    private static String buildSubmitJson(List<PlayerScore> leaderboard) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("{\n");
        sb.append("  \"regNo\": \"").append(REG_NO).append("\",\n");
        sb.append("  \"leaderboard\": [\n");

        for (int i = 0; i < leaderboard.size(); i++) {
            PlayerScore ps = leaderboard.get(i);
            sb.append("    { \"participant\": \"")
              .append(escapeJson(ps.getParticipant()))
              .append("\", \"totalScore\": ")
              .append(ps.getTotalScore())
              .append(" }");

            if (i < leaderboard.size() - 1) {
                sb.append(',');
            }
            sb.append('\n');
        }

        sb.append("  ]\n}");
        return sb.toString();
    }

    /**
     * Minimal JSON string escaper — handles the characters required by RFC 8259 §7.
     */
    private static String escapeJson(String raw) {
        if (raw == null) return "";
        StringBuilder out = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '"':  out.append("\\\""); break;
                case '\\': out.append("\\\\"); break;
                case '\n': out.append("\\n");  break;
                case '\r': out.append("\\r");  break;
                case '\t': out.append("\\t");  break;
                default:   out.append(c);
            }
        }
        return out.toString();
    }

    // ─── Utility ─────────────────────────────────────────────────────────────────

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warning("Sleep interrupted.");
        }
    }
}
