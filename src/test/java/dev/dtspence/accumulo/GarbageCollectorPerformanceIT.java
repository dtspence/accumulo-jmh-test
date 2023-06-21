package dev.dtspence.accumulo;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.util.TabletIterator;
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
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
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
        static File rfileErrorsPath;
        static MiniAccumuloConfigImpl config;
        static MiniAccumuloClusterImpl cluster;

        Connector client;
        File accumuloPropertiesFile;
        SiteConfiguration siteConfig;
        AccumuloServerContext serverContext;

        @Param({"1,100","10,100","100,100","1000,100"})
        String splitsRfile;

        @Setup(Level.Trial)
        public void setUp() throws Exception {
            rootPath = new File("/tmp/accumulo");
            clusterPath = new File("/tmp/accumulo/root");
            rfilePath = new File("/tmp/accumulo/rfile");
            rfileErrorsPath = new File("/tmp/accumulo/rfile-errors");

            FileUtils.deleteDirectory(rootPath);
            Files.createDirectories(clusterPath.toPath());
            Files.createDirectories(rfilePath.toPath());
            Files.createDirectories(rfileErrorsPath.toPath());

            config = new MiniAccumuloConfigImpl(clusterPath, ROOT_PASSWORD)
                    .setNumTservers(TSERVER_COUNT)
                    .setMemory(ServerType.TABLET_SERVER, 2, MemoryUnit.GIGABYTE);
            cluster = new MiniAccumuloClusterImpl(config);
            cluster.start();

            // disable mini-accumulo GC
            cluster.getClusterControl().stop(ServerType.GARBAGE_COLLECTOR);

            client = cluster.getConnector("root", new PasswordToken(ROOT_PASSWORD));

            client.tableOperations().create(TEST_TABLE);

            accumuloPropertiesFile = Paths.get(config.getDir().toString(), "conf", "accumulo.properties").toFile();

            final String tid = client.tableOperations().tableIdMap().get(TEST_TABLE);
            final String[] splitRfileArray = splitsRfile.split(",");
            final int splitCount = Integer.parseInt(splitRfileArray[0]);
            final int rfileCount = Integer.parseInt(splitRfileArray[1]);
            final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            final List<String> partitions = IntStream.range(0, splitCount).mapToObj(x -> "part_" + x).collect(Collectors.toList());
            final Collection<Path> paths = IntStream.range(0, rfileCount).mapToObj(x -> Paths.get(rfilePath.toString(), "rfile_" + x)).collect(Collectors.toList());
            final SortedSet<Text> splits = partitions.stream().map(Text::new).collect(Collectors.toCollection(TreeSet::new));
            final int metadataPartitionGroups = Math.max(1, (partitions.size() / TSERVER_COUNT) + 1);
            final SortedSet<Text> metadataSplits = (metadataPartitionGroups > 1) ?
                    Lists.partition(partitions, metadataPartitionGroups).stream()
                            .map(entry -> tid + ";" + entry.stream().sorted().skip(entry.size() / 2).findFirst().get())
                            .map(Text::new)
                            .collect(Collectors.toCollection(TreeSet::new)) :
                    new TreeSet<Text>();
            final int rfileTotalNumber = splitCount * rfileCount;

            // set max files to be high
            client.instanceOperations().setProperty("table.file.max", Integer.toString(rfileTotalNumber + 2));

            // unsure if this is the best way to suspend compactions
            client.instanceOperations().setProperty("tserver.compaction.major.service.default.planner.opts.maxOpen", "0");

            // add test table splits
            client.tableOperations().addSplits(TEST_TABLE, splits);

            partitions.forEach(part -> {
                paths.forEach(rf -> {
                    executor.submit(() -> {
                        try (final OutputStream stream = Files.newOutputStream(Paths.get(rf.toString() + "_" + part + ".rf"));
                             final RFileWriter writer = RFile.newWriter().to(stream).build()) {
                            final Text row = new Text(part + "_" + UUID.randomUUID());
                            final Key key  = new Key(row);
                            final Value val = new Value();
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

            client.tableOperations().importDirectory(TEST_TABLE, rfilePath.toString(), rfileErrorsPath.toString(), false);

            log.info("Setup metadata splits");

            if (!metadataSplits.isEmpty()) {
                client.tableOperations().addSplits("accumulo.metadata", metadataSplits);
            }
            client.tableOperations().flush("accumulo.metadata");
            client.instanceOperations().waitForBalance();

            log.info("Metadata splits size: {} (computed) / {} (actual)", metadataSplits.size(), client.tableOperations().listSplits("accumulo.metadata").size());
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception{
            cluster.stop();
        }
    }

    @Benchmark
    public void benchmarkReferences(BenchmarkState state, Blackhole bh) throws Exception {
        final Iterator<Map.Entry<Key,Value>> iter = getReferenceIterator(state.client);
        while (iter.hasNext()) {
            bh.consume(iter.next());
        }
    }

    @Test
    public void testBenchmark() throws Exception {
        // @formatter:off
        final Options opt = new OptionsBuilder()
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

    public Iterator<Map.Entry<Key, Value>> getReferenceIterator(Connector connector) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
        IsolatedScanner scanner = new IsolatedScanner(connector.createScanner(TEST_TABLE, Authorizations.EMPTY));
        scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
        scanner.fetchColumnFamily(MetadataSchema.TabletsSection.ScanFileColumnFamily.NAME);
        MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.fetch(scanner);
        TabletIterator tabletIterator = new TabletIterator(scanner, MetadataSchema.TabletsSection.getRange(), false, true);
        return Iterators.concat(Iterators.transform(tabletIterator, (input) -> {
            return input.entrySet().iterator();
        }));
    }

}
