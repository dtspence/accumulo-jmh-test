package dev.dtspence.accumulo;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.gc.GCRun;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GarbageCollectorPerformanceIT {
    private final static Logger log = LoggerFactory.getLogger(GarbageCollectorPerformanceIT.class);

    private final static String ROOT_PASSWORD = "password";
    private final static String TEST_TABLE = "test";
    private final static int TSERVER_COUNT = 3;

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

        @Param({"1,100","10,100","100,100", "1000,100"})
        String splitsRfile;

        @Setup(Level.Trial)
        public void setUp() throws Exception {
            rootPath = new File("/tmp/accumulo");
            clusterPath = new File("/tmp/accumulo/root");
            rfilePath = new File("/tmp/accumulo/rfile");

            FileUtils.deleteDirectory(rootPath);
            Files.createDirectories(clusterPath.toPath());
            Files.createDirectories(rfilePath.toPath());

            config = new MiniAccumuloConfigImpl(clusterPath, ROOT_PASSWORD)
                    .setNumTservers(TSERVER_COUNT)
                    .setMemory(ServerType.TABLET_SERVER, 2, MemoryUnit.GIGABYTE);
            cluster = new MiniAccumuloClusterImpl(config);
            cluster.start();

            // disable mini-accumulo GC
            cluster.getClusterControl().stop(ServerType.GARBAGE_COLLECTOR);

            client = cluster.createAccumuloClient("root", new PasswordToken(ROOT_PASSWORD));

            client.tableOperations().create(TEST_TABLE);

            accumuloPropertiesFile = Paths.get(config.getDir().toString(), "conf", "accumulo.properties").toFile();
            siteConfig = SiteConfiguration.fromFile(accumuloPropertiesFile).build();
            serverContext = new ServerContext(siteConfig);
            gc = new GCRun(Ample.DataLevel.USER, serverContext);

            final var tid = client.tableOperations().tableIdMap().get(TEST_TABLE);
            final var splitRfileArray = splitsRfile.split(",");
            final var splitCount = Integer.parseInt(splitRfileArray[0]);
            final var rfileCount = Integer.parseInt(splitRfileArray[1]);
            final var executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            final var partitions = IntStream.range(0, splitCount).mapToObj(x -> "part_" + x).collect(Collectors.toList());
            final var paths = IntStream.range(0, rfileCount).mapToObj(x -> Paths.get(rfilePath.toString(), "rfile_" + x)).collect(Collectors.toList());
            final var splits = partitions.stream().map(Text::new).collect(Collectors.toCollection(TreeSet::new));
            final var metadataPartitionGroups = Math.max(1, (partitions.size() / TSERVER_COUNT) + 1);
            final var metadataSplits = (metadataPartitionGroups > 1) ?
                    Lists.partition(partitions, metadataPartitionGroups).stream()
                            .map(entry -> tid + ";" + entry.stream().sorted().skip(entry.size() / 2).findFirst().get())
                            .map(Text::new)
                            .collect(Collectors.toCollection(TreeSet::new)) :
                    new TreeSet<Text>();
            final var rfileTotalNumber = splitCount * rfileCount;

            // set max files to be high
            client.instanceOperations().setProperty("table.file.max", Integer.toString(rfileTotalNumber + 2));

            // unsure if this is the best way to suspend compactions
            client.instanceOperations().setProperty("tserver.compaction.major.service.default.planner.opts.maxOpen", "0");

            // add test table splits
            client.tableOperations().addSplits(TEST_TABLE, splits);

            partitions.forEach(part -> {
                paths.forEach(rf -> {
                    executor.submit(() -> {
                        try (final var stream = Files.newOutputStream(Paths.get(rf.toString() + "_" + part + ".rf"));
                             final var writer = RFile.newWriter().to(stream).build()) {
                            final var row = new Text(part + "_" + UUID.randomUUID());
                            final var key  = new Key(row);
                            final var val = new Value();
                            writer.append(key, val);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                });
            });

            executor.shutdown();
            executor.awaitTermination(3, TimeUnit.MINUTES);

            log.info("Importing files: {} (expected) / {} (actual)", rfileTotalNumber, Files.list(rfilePath.toPath()).count());
            client.tableOperations().importDirectory(rfilePath.toString()).to(TEST_TABLE).load();

            if (!metadataSplits.isEmpty()) {
                log.info("Setup metadata splits: {}", metadataSplits.size());
                client.tableOperations().addSplits("accumulo.metadata", metadataSplits);

                client.tableOperations().flush("accumulo.metadata");
                client.instanceOperations().waitForBalance();

                final var metadataSplitsActual = client.tableOperations().listSplits("accumulo.metadata");
                final var metadataDataActual = TabletsMetadata.builder(client).forLevel(Ample.DataLevel.USER)
                        .fetch(TabletMetadata.ColumnType.FILES)
                        .build()
                        .stream()
                        .map(tm -> tm.getEndRow().toString() + "[" + String.join(",", tm.getFiles().stream().map(StoredTabletFile::toString).limit(2).collect(Collectors.toList())) + " ...]")
                        .limit(5)
                        .collect(Collectors.toList());

                log.info("Metadata splits size: {} (computed) / {} (actual)", metadataSplits.size(), metadataSplitsActual.size());

                metadataSplitsActual.forEach(entry -> log.info("md.split - " + entry));
                metadataDataActual.forEach(entry -> log.info("md.data - " + entry));
            }
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
        final var opt = new OptionsBuilder()
                .include(this.getClass().getName() + ".*")
                .addProfiler(RFileImpact.class)
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.MILLISECONDS)
                .warmupTime(TimeValue.seconds(5))
                .warmupIterations(5)
                .measurementIterations(3)
                .threads(1)
                .forks(1)
                .shouldFailOnError(true)
                .shouldDoGC(true)
                .build();
        // @formatter:on
        new Runner(opt).run();
    }

}
