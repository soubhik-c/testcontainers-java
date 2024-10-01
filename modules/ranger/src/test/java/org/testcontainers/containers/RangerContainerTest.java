package org.testcontainers.containers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

public class RangerContainerTest {

    private static final Logger logger = LoggerFactory.getLogger(RangerContainerTest.class);

    private static final DockerImageName RANGER_IMAGE = RangerContainer.DEFAULT_IMAGE_NAME.withTag(
        RangerContainer.DEFAULT_TAG
    );

    @Test
    public void rangerStandaloneTest() throws IOException, InterruptedException {
        BaseConsumer<?> logs = new Slf4jLogConsumer(logger);

        // rangerContainerUsage {
        // create ranger with default database and audit store
        try (RangerContainer container = new RangerContainer(RANGER_IMAGE).withDefaults()) {
            // override default database and audit store containers
            container
                .withLogConsumer(logs)
                .withRangerDbContainerModifier(c -> c.withLogConsumer(logs))
                .withAuditStoreContainerModifier(c -> c.withLogConsumer(logs))
                // create default services
                .withCopyToContainer(
                    MountableFile.forClasspathResource("create-ranger-services.py"),
                    "/home/ranger/scripts/create-ranger-services.py"
                )
                .withWaitForServiceCreation("trino")
                .start();
            container.waitUntilContainerStarted();
            logger.info(
                String.format("ranger audit: http://%s:%s", container.getHost(), container.getDefaultAuditStorePort())
            );
            logger.info(String.format("ranger admin: http://%s:%s", container.getHost(), container.getRangerPort()));

            // invoke audit apis
            String response = getRangerAudit(container, "/count");
            int numAuditEntries = new ObjectMapper().readTree(response).path("value").asInt();
            assertThat(numAuditEntries).isGreaterThanOrEqualTo(0);
        }
        // }
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
