package dev.dtspence.accumulo.benchmark;

import dev.dtspence.accumulo.support.RFileRandom;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.openjdk.jmh.annotations.Mode.AverageTime;

public class CompactionBenchmark {
    @State(value = Scope.Benchmark)
    public static class BenchmarkState {
        private final static String ROOT_PASSWORD = "password";
        private final static String TEST_TABLE = "test";

        private MiniAccumuloCluster cluster;
        private AccumuloClient client;
        private Path randomRFile;

        @Setup(Level.Trial)
        public void setUp() throws Exception {
//            var tempAccumuloDir = Files.createTempDirectory("rf-db");
//            var tempFileDir = Files.createTempDirectory("rf-data");

            var tempAccumuloDir = Paths.get("/tmp/rf-test/db");
            var tempFileDir = Paths.get("/tmp/rf-test/data");
            var clusterConf = new MiniAccumuloConfig(tempAccumuloDir.toFile(), ROOT_PASSWORD)
                    .setNumTservers(1)
                    .setJDWPEnabled(true);

            cluster = new MiniAccumuloCluster(clusterConf);
            cluster.start();
            client = Accumulo.newClient()
                    .from(cluster.getClientProperties())
                    .build();
            client.tableOperations().create(TEST_TABLE);

            randomRFile = new RFileRandom()
                    .rowMaximum(1000000L)
                    .writeToFile(tempFileDir);

            client.tableOperations().importDirectory(tempFileDir.toString())
                    .to(TEST_TABLE)
                    .load();
        }

        @TearDown
        public void tearDown() throws Exception {
            cluster.stop();
        }
    }

    @Benchmark
    @Fork(0)
    @BenchmarkMode(AverageTime)
    @Measurement(iterations = 5)
    @Warmup(iterations = 2)
    public void benchmarkCompaction(final BenchmarkState state) throws Exception {
        state.client.tableOperations().compact(BenchmarkState.TEST_TABLE, null, null, false, true);
    }
}
