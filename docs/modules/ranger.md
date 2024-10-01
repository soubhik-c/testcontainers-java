# Ranger Container

!!! note
    This module is INCUBATING. While it is ready for use and operational in the current version of Testcontainers, it is possible that it may receive breaking changes in the future. See [our contributing guidelines](/contributing/#incubating-modules) for more information on our incubating modules policy.


This module helps running [ranger](https://ranger.apache.org) using Testcontainers.

Note that it's based on the [official Docker image](https://hub.docker.com/r/apache/ranger).

## Usage example

You can start a solr container instance from any Java application by using:

<!--codeinclude-->
[Using a Ranger container](../../modules/ranger/src/test/java/org/testcontainers/containers/RangerContainerTest.java) inside_block:rangerContainerUsage
<!--/codeinclude-->

## Adding this module to your project dependencies

Add the following dependency to your `pom.xml`/`build.gradle` file:

=== "Gradle"
    ```groovy
    testImplementation "org.testcontainers:ranger:{{latest_version}}"
    ```

=== "Maven"
    ```xml
    <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>ranger</artifactId>
        <version>{{latest_version}}</version>
        <scope>test</scope>
    </dependency>
    ```
