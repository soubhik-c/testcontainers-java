package org.testcontainers.containers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.google.common.collect.ImmutableMap;
import lombok.SneakyThrows;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.net.URL;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.testcontainers.utility.MountableFile.forClasspathResource;

/**
 * Testcontainers implementation for Ranger.
 * <p>
 * Supported image: {@code ranger}
 * <p>
 * Exposed ports:
 * <ul>
 *     <li>Ranger: 6080</li>
 * </ul>
 */
public class RangerContainer extends GenericContainer<RangerContainer> {

    public static final String IMAGE = "apache/ranger";

    public static final String DEFAULT_TAG = "2.5.0";

    public static final String CONTAINER_INSTALL_PROPERTIES_PATH = "/opt/ranger/admin/install.properties";

    public static final int RANGER_PORT = 6080; // policymgr_external_url in install.properties

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse(IMAGE);

    private MountableFile installPropertiesFile = MountableFile.forClasspathResource("rangeradmin-install.properties");

    private final PostgreSQLContainer<?> defaultRangerDB;

    private final GenericContainer<?> defaultAuditStore;

    public RangerContainer() {
        this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
    }

    public RangerContainer(final DockerImageName dockerImageName) {
        this(
            dockerImageName,
            DockerImageName.parse(PostgreSQLContainer.IMAGE).withTag(PostgreSQLContainer.DEFAULT_TAG),
            DockerImageName.parse("solr").withTag(SolrContainer.DEFAULT_TAG)
        );
    }

    public RangerContainer(
        final DockerImageName dockerImageName,
        final DockerImageName rangerDB,
        final DockerImageName auditStore
    ) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

        this.defaultRangerDB = defaultRangerDb(rangerDB);
        this.defaultAuditStore = defaultAuditStore(auditStore);

        this.waitStrategy =
            new LogMessageWaitStrategy()
                .withRegEx(".*Apache Ranger Admin Service with pid [0-9]+ has started.*")
                .withStartupTimeout(Duration.of(120, ChronoUnit.SECONDS));
    }

    public RangerContainer withDefaults() {
        this.withNetwork(Network.newNetwork())
            .withCopyToContainer(installPropertiesFile, CONTAINER_INSTALL_PROPERTIES_PATH)
            .withEnv(Map.of("RANGER_DB_TYPE", "postgres"))
            .dependsOn(defaultRangerDB, defaultAuditStore);
        return self();
    }

    public RangerContainer withRangerDbContainerModifier(Consumer<Container<?>> modifier) {
        modifier.accept(defaultRangerDB);
        return self();
    }

    public RangerContainer withAuditStoreContainerModifier(Consumer<Container<?>> modifier) {
        modifier.accept(defaultAuditStore);
        return self();
    }

    public RangerContainer withWaitForServiceCreation(String service) {
        this.waitStrategy =
            new WaitAllStrategy(WaitAllStrategy.Mode.WITH_INDIVIDUAL_TIMEOUTS_ONLY)
                .withStrategy(
                    Wait
                        .forLogMessage(".*Apache Ranger Admin Service with pid [0-9]+ has started\\..*", 1)
                        .withStartupTimeout(Duration.ofSeconds(120))
                )
                .withStrategy(
                    Wait
                        .forLogMessage(".*" + service + ".*service created.*", 1)
                        .withStartupTimeout(Duration.ofSeconds(90))
                );
        return self();
    }

    public RangerContainer withInstallPropertiesFile(MountableFile installPropertiesFile) {
        if (installPropertiesFile == null) {
            throw new IllegalArgumentException();
        }
        this.installPropertiesFile = installPropertiesFile;
        return self();
    }

    public int getRangerPort() {
        return getMappedPort(RANGER_PORT);
    }

    public int getDefaultAuditStorePort() {
        return defaultAuditStore.getMappedPort(SolrContainer.SOLR_PORT);
    }

    public int getDefaultRangerDBPort() {
        return defaultRangerDB.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT);
    }

    @Override
    public RangerContainer withNetwork(Network network) {
        super.withNetwork(network);
        defaultRangerDB.withNetwork(network);
        defaultAuditStore.withNetwork(network);
        return self();
    }

    @Override
    public void stop() {
        super.stop();
        getDependencies().forEach(Startable::stop);
    }

    @Override
    @SneakyThrows
    protected void configure() {
        this.addExposedPort(RANGER_PORT);
    }

    @Override
    public Set<Integer> getLivenessCheckPortNumbers() {
        return new HashSet<>(getRangerPort());
    }

    @Override
    protected void waitUntilContainerStarted() {
        getWaitStrategy().waitUntilReady(this);
    }

    protected void waitForDefaultAuditStoreContainerStarted() {
        this.defaultAuditStore.waitUntilContainerStarted();
    }

    @Override
//    @SneakyThrows
    protected void containerIsStarted(InspectContainerResponse containerInfo) {

    }

    @SneakyThrows
    protected URL resourceToFileUrl(String resource) {
        return new File(MountableFile.forClasspathResource(resource).getFilesystemPath()).toURI().toURL();
    }

    private PostgreSQLContainer<?> defaultRangerDb(DockerImageName defaultImage) {
        return new PostgreSQLContainer<>(defaultImage)
            .withNetworkAliases("ranger-pg-db") // :5432 in rangeradmin-install.properties
            .withUsername("postgres")
            .withPassword("postgres");
    }

    private GenericContainer<?> defaultAuditStore(DockerImageName defaultImage) {
        return new GenericContainer<>(defaultImage)
            .withExposedPorts(SolrContainer.SOLR_PORT)
            .withNetworkAliases("ranger-solr-audit") // :8983 in rangeradmin-install.properties
            .withEnv(ImmutableMap.<String, String>builder()
                .put("SOLR_DEPLOYMENT", "standalone")
                .put("SOLR_SHARDS", "1")
                .put("SOLR_REPLICATION", "1")
                .put("xpack.security.enabled", "false")
                .put("ES_JAVA_OPTS", "-Xms500m -Xmx500m")
                .put("SOLR_PORT", Integer.toString(SolrContainer.SOLR_PORT))
                .put("SOLR_HEAP", "800m")
                .build())
            .withCommand("solr-precreate", "ranger_audits", "/opt/solr/server/solr/configsets/ranger_audits")
            .withCopyFileToContainer(
                forClasspathResource("solr-conf"),
                "/opt/solr/server/solr/configsets/ranger_audits/conf")
            .waitingFor(Wait.forListeningPort());
    }
}
