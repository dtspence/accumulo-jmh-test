package dev.dtspence.accumulo.benchmark;

import dev.dtspence.accumulo.file.DisruptorSKVIterator;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

public class RFileReadBenchmark {
    private final static Logger LOG = LoggerFactory.getLogger(RFileReadBenchmark.class);

    @State(Scope.Thread)
    public static class BenchmarkState {
        private FileSKVIterator iterator;
        private DisruptorSKVIterator disruptorSKVIterator;

        @Setup(Level.Invocation)
        public void setUp() throws Exception {
            final var tempDir = Files.createTempDirectory("rf-test");
            final var tempPath = tempDir.toString() + "/rf-test.rf";
            var count = 0L;
            try (final var wr = RFile.newWriter().to(tempPath).build()) {
                for (var idx = 0; idx < 1000000; idx++) {
                    String row = String.format("%09x", idx);
                    String fam= Integer.toString(idx%1000,36);
                    String value = (idx*31)+"";
                    wr.append(new Key(row,fam), new Value(value));
                    count++;
                }
            }

            LOG.debug("Wrote k/v pairs: {}", count);
            LOG.debug("Temp path: {}", tempPath);

            final var accumuloConf = new ConfigurationCopy(Map.of());
            final var conf = new Configuration();
            iterator = RFileOperations.getInstance()
                    .newReaderBuilder()
                    .forFile(tempPath, FileSystem.getLocal(conf), conf,
                            NoCryptoServiceFactory.NONE)
                    .withTableConfiguration(accumuloConf)
                    //.seekToBeginning()
                    .build();

            disruptorSKVIterator = new DisruptorSKVIterator(iterator);
        }

        @TearDown(Level.Invocation)
        public void tearDown() throws Exception {
            disruptorSKVIterator.close();
        }
    }

    @Fork(value = 1)
    @Benchmark
    @BenchmarkMode(AverageTime)
    @Measurement(iterations = 5, time = 5, timeUnit = SECONDS)
    @Warmup(iterations = 3, time = 5, timeUnit = SECONDS)
    @OutputTimeUnit(MILLISECONDS)
    public void benchmarkBaselineRead(final BenchmarkState state, final Blackhole bh) throws IOException {
        state.iterator.seek(new Range(), Collections.emptyList(), false);
        while (state.iterator.hasTop()) {
            var copyKey = new Key(state.iterator.getTopKey());
            var copyValue = new Value(state.iterator.getTopValue());
            bh.consume(copyKey);
            bh.consume(copyValue);
            state.iterator.next();
        }
    }

    @Fork(value = 1)
    @Benchmark
    @BenchmarkMode(AverageTime)
    @Measurement(iterations = 5, time = 5, timeUnit = SECONDS)
    @Warmup(iterations = 3, time = 5, timeUnit = SECONDS)
    @OutputTimeUnit(MILLISECONDS)
    public void benchmarkDisruptorRead(final BenchmarkState state, final Blackhole bh) throws IOException {
        var count = 0L;
        state.disruptorSKVIterator.seek(new Range(), Collections.emptyList(), false);
        while (state.disruptorSKVIterator.hasTop()) {
            var copyKey = new Key(state.disruptorSKVIterator.getTopKey());
            var copyValue = new Value(state.disruptorSKVIterator.getTopValue());
            bh.consume(copyKey);
            bh.consume(copyValue);
            state.disruptorSKVIterator.next();
            count++;
        }
        bh.consume(count);
    }
}
