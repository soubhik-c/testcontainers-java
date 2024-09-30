package org.testcontainers.containers;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class RangerContainerTest {

    private static final Logger logger = LoggerFactory.getLogger(RangerContainerTest.class);

    @Test
    public void rangerStandaloneTest() throws IOException, InterruptedException {
        BaseConsumer<?> logs = new Slf4jLogConsumer(logger);
        logger.info("Creating ranger container");
        try (RangerContainer container = new RangerContainer().withDefaults()) {
            container
                .withLogConsumer(logs)
                .withRangerDbContainerModifier(c -> c.withLogConsumer(logs))
                .withAuditStoreContainerModifier(c -> c.withLogConsumer(logs))
                .withCopyToContainer(
                    MountableFile.forClasspathResource("create-ranger-services.py"),
                    "/home/ranger/scripts/create-ranger-services.py"
                )
                .withWaitForServiceCreation("trino")
                .start();
            container.waitForDefaultAuditStoreContainerStarted();
            container.waitUntilContainerStarted();
            logger.info(
                String.format("ranger audit: http://%s:%s", container.getHost(), container.getDefaultAuditStorePort())
            );
            logger.info(String.format("ranger admin: http://%s:%s", container.getHost(), container.getRangerPort()));
            String response = getRangerAudit(container, "/count");
            int numAuditEntries = new ObjectMapper().readTree(response).path("value").asInt();
            assertThat(numAuditEntries).isGreaterThanOrEqualTo(0);
        }
    }

    public URI getRangerUri(RangerContainer ranger) {
        if (!ranger.isRunning()) {
            throw new IllegalStateException("Container is not running");
        }

        return URI.create(String.format("http://%s:%d/", ranger.getHost(), ranger.getRangerPort()));
    }

    public String getRangerAudit(RangerContainer rangerContainer, String operation)
        throws IOException, InterruptedException {
        HttpClient httpClient = HttpClient.newBuilder().build();
        HttpResponse<String> auditResponse = httpClient.send(
            HttpRequest
                .newBuilder(getRangerUri(rangerContainer).resolve("service/xaudit/access_audit" + operation))
                .GET()
                .header(
                    "Authorization",
                    "Basic " +
                    Base64.getEncoder().encodeToString("admin:Rangeradmin1".getBytes(Charset.defaultCharset()))
                )
                .header("accept", "application/json")
                .header("Content-Type", "text/plain")
                .build(),
            HttpResponse.BodyHandlers.ofString()
        );
        assertThat(auditResponse.statusCode())
            .withFailMessage("Failed to get audits: %s", auditResponse.body())
            .isEqualTo(200);
        return auditResponse.body();
    }
}
