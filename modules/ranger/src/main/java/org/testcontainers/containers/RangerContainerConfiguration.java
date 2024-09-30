package org.testcontainers.containers;

import lombok.Data;
import org.testcontainers.utility.MountableFile;

import java.util.Optional;

@Data
public class RangerContainerConfiguration {

    private String rangerDBHostPort;

    private String rangerAuditHostPort;

    private Optional<MountableFile> installProperties = Optional.empty();
}
