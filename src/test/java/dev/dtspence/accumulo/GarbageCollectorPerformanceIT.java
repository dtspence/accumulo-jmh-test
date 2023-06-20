package dev.dtspence.accumulo;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.spi.balancer.HostRegexTableLoadBalancer;
import org.apache.accumulo.gc.GCRun;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerOpts;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.shaded.org.apache.http.client.utils.URIBuilder;
import org.apache.hadoop.yarn.webapp.RemoteExceptionData;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GarbageCollectorPerformanceIT {
    private final static Logger log = LoggerFactory.getLogger(GarbageCollectorPerformanceIT.class);

    private final static String ROOT_PASSWORD = "password";
    private final static int RFILE_COUNT = 10000;

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        static File rootPath;
        static File clusterPath;
        static File rfilePath;
        static MiniAccumuloConfigImpl config;
        static MiniAccumuloClusterImpl cluster;

        AccumuloClient client;
        File accumuloPropertiesFile;
        SiteConfiguration siteConfig;
        ServerContext serverContext;
        GCRun gc;

        @Setup(Level.Trial)
        public void setUp() throws Exception {
            rootPath = new File("/tmp/accumulo");
            clusterPath = new File("/tmp/accumulo/root");
            rfilePath = new File("/tmp/accumulo/rfile");

            FileUtils.deleteDirectory(rootPath);
            Files.createDirectories(clusterPath.toPath());
            Files.createDirectories(rfilePath.toPath());

            //clusterPath = tempPath.toFile();
            config = new MiniAccumuloConfigImpl(clusterPath, ROOT_PASSWORD)
                    .setNumTservers(3)
                    .setMemory(ServerType.TABLET_SERVER, 2, MemoryUnit.GIGABYTE);
            cluster = new MiniAccumuloClusterImpl(config);
            cluster.start();

            // disable mini-accumulo GC
            cluster.getClusterControl().stop(ServerType.GARBAGE_COLLECTOR);

            client = cluster.createAccumuloClient("root", new PasswordToken(ROOT_PASSWORD));

            // set max files to be high
            client.instanceOperations().setProperty("table.file.max", Integer.toString(RFILE_COUNT + 1));

            // unsure if this is the best way to suspend compactions
            client.instanceOperations().setProperty("tserver.compaction.major.service.default.planner.opts.maxOpen", "0");

            client.tableOperations().create("test");

            accumuloPropertiesFile = Paths.get(config.getDir().toString(), "conf", "accumulo.properties").toFile();
            siteConfig = SiteConfiguration.fromFile(accumuloPropertiesFile).build();
            serverContext = new ServerContext(siteConfig);
            gc = new GCRun(Ample.DataLevel.USER, serverContext);

            generateFilesAndImport();
        }

        void generateFilesAndImport() throws Exception {
            final var executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            final var paths = IntStream.range(1, RFILE_COUNT).mapToObj(x -> Paths.get(rfilePath.toString(), "rfile_" + x + ".rf")).collect(Collectors.toList());

            paths.forEach(p -> {
                executor.submit(() -> {
                    try (final var stream = Files.newOutputStream(p);
                         final var writer = RFile.newWriter().to(stream).build()) {
                        writer.append(new Key(UUID.randomUUID().toString()), new Value());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            });

            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.DAYS);

            log.info("importing");
            client.tableOperations().importDirectory(rfilePath.toString()).to("test").load();
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception{
            client.close();
            cluster.stop();
        }
    }

    @Benchmark
    public void benchmarkReferences(BenchmarkState state, Blackhole bh) {
        final var iter = state.gc.getReferences().iterator();
        while (iter.hasNext()) {
            bh.consume(iter.next());
        }
    }

    @Test
    public void testBenchmark() throws Exception {
        // @formatter:off
        Options opt = new OptionsBuilder()
                .include(this.getClass().getName() + ".*")
                // Set the following options as needed
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.MILLISECONDS)
                .warmupTime(TimeValue.seconds(1))
                .warmupIterations(1)
                .measurementIterations(3)
                .operationsPerInvocation(RFILE_COUNT)
                .threads(1)
                .forks(1)
                .shouldFailOnError(true)
                .shouldDoGC(true)
                .build();
        // @formatter:on
        new Runner(opt).run();
    }
}
