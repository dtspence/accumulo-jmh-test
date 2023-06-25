package dev.dtspence.accumulo;

import org.apache.accumulo.core.metadata.ValidationUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ValidationUtilPerformanceIT {
    @State(Scope.Benchmark)
    public static class BenchmarkState {
        Collection<String> samples;

        @Setup(Level.Trial)
        public void setUp() {
            samples = IntStream.range(0, 10000)
                    .mapToObj(x -> RandomStringUtils.random(7, true, true) + ".rf")
                    .collect(Collectors.toList());
        }
    }

    @Benchmark
    public void benchmarkValidateFileName(BenchmarkState state, Blackhole bh) {
        for (final var file : state.samples) {
            ValidationUtil.validateFileName(file);
            bh.consume(file);
        }
    }

    @Test
    public void testBenchmark() throws Exception {
        // @formatter:off
        final var opt = new OptionsBuilder()
                .include(this.getClass().getName() + ".*")
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
