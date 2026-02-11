package io.github.evelynkk.orderplatform.messaging.dlq;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Operator surface for the dead-letter topic, available in any service that runs a web server.
 *
 * <p>Deliberately read-then-replay rather than a single "fix it" button: the point of dead
 * lettering is that a human looks at why the record failed before putting it back.
 */
@RestController
@RequestMapping("/admin/dlq")
@ConditionalOnWebApplication
@RequiredArgsConstructor
public class DeadLetterAdminController {

    private static final int MAX_BATCH = 500;

    private final DeadLetterAdminService deadLetters;

    @GetMapping
    public List<DeadLetter> peek(@RequestParam(defaultValue = "50") int limit) {
        return deadLetters.peek(clampBatch(limit));
    }

    @PostMapping("/replay")
    public Map<String, Integer> replay(@RequestParam(defaultValue = "50") int limit) {
        return Map.of("replayed", deadLetters.replay(clampBatch(limit)));
    }

    private static int clampBatch(int limit) {
        return Math.min(Math.max(limit, 1), MAX_BATCH);
    }
}
