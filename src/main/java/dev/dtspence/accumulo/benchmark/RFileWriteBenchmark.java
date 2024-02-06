package dev.dtspence.accumulo.benchmark;

import dev.dtspence.accumulo.file.DisruptorSKVWriter;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.util.NullOutputStream;
import org.slf4j.simple.SimpleLogger;

import java.io.IOException;
import java.util.HashMap;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

public class RFileWriteBenchmark {
    @State(Scope.Thread)
    public static class BenchmarkState {
        FileSKVWriter writerDefault;
        DisruptorSKVWriter writerDisruptor;

        int idx;

        @Param({"none", "gz"})
        String codec;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            // disable logging
            System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "OFF");

            writerDefault = newWriter(codec);
            writerDisruptor = new DisruptorSKVWriter(newWriter(codec), 10_000_000);
            Stream.of(writerDefault, writerDisruptor).forEach(writer -> {
                try {
                    writer.startDefaultLocalityGroup();
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        @TearDown
        public void tearDown() {
            Stream.of(writerDefault, writerDisruptor).forEach(writer -> {
                try {
                    writer.close();
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        private static FileSKVWriter newWriter(final String codec) throws Exception {
            //noinspection resource
            var out = new NullOutputStream();
            var fileops = FileOperations.getInstance();
            var acuconf = DefaultConfiguration.getInstance();
            var userProps = new HashMap<String,String>();
            var conf = new Configuration();

            conf.set(Property.TABLE_FILE_COMPRESSION_TYPE.name(), codec);

            var cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE, userProps);

            var fsdo = new FSDataOutputStream(out, new FileSystem.Statistics("foo"));
            return fileops.newWriterBuilder().forOutputStream(".rf", fsdo, conf, cs)
                    .withTableConfiguration(acuconf).withStartDisabled().build();
        }
    }

    @Fork(value = 1)
    @Benchmark
    @BenchmarkMode(AverageTime)
    @Measurement(iterations = 5, batchSize = 1000000)
    @Warmup(iterations = 3, batchSize = 1000000)
    @OutputTimeUnit(MILLISECONDS)
    public void defaultWriter(final BenchmarkState state) throws Exception {
        runAppendOnWriter(state, state.writerDefault);
    }

    @Fork(value = 1)
    @Benchmark
    @BenchmarkMode(AverageTime)
    @Measurement(iterations = 5, batchSize = 1000000)
    @Warmup(iterations = 3, batchSize = 1000000)
    @OutputTimeUnit(MILLISECONDS)
    public void disruptorWriter(final BenchmarkState state) throws Exception {
        runAppendOnWriter(state, state.writerDisruptor);
    }

    void runAppendOnWriter(final BenchmarkState state, final FileSKVWriter testWriter) throws Exception {
        String row = String.format("%09x", state.idx++);
        String fam= Integer.toString(state.idx%1000,36);
        String value = (state.idx*31)+"";
        testWriter.append(new Key(row,fam), new Value(value));
    }
}
