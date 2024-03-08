package dev.dtspence.accumulo.support;

import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RFileRandom {
    private List<String> cvSet = List.of("ALPHA&BRAVO", "ALPHA&CHARLIE", "ALPHA&BETA&BRAVO");

    private int cfSamples = 5;
    private int cqSamples = 5;
    private long rowMaximum = 100000;
    private long sizeMaximum = Long.MAX_VALUE;
    private boolean valueEnabled = true;

    public RFileRandom rowMaximum(final long value) {
        rowMaximum = value;
        return this;
    }

    public RFileRandom sizeMaximum(final long value) {
        sizeMaximum = value;
        return this;
    }

    public RFileRandom columnFamilySamples(final int count) {
        cfSamples = count;
        return this;
    }

    public RFileRandom columnQualifierSamples(final int count) {
        this.cqSamples = count;
        return this;
    }

    public RFileRandom valueEnabled(final boolean value) {
        valueEnabled = value;
        return this;
    }

    public Path writeToFile(final Path path) throws IOException {
        final var randomName = RandomStringUtils.randomAlphabetic(5).toLowerCase() + ".rf";
        final var randomPath = path.resolve(randomName);
        try (final var fsOut = new FileOutputStream(randomPath.toFile())) {
            writeTo(fsOut);
        }
        return randomPath;
    }

    public void writeTo(final OutputStream stream) throws IOException {
        final var cfSet = IntStream.range(0, cfSamples).mapToObj(x -> RandomStringUtils.randomAlphabetic(5)).sorted().collect(Collectors.toList());
        final var cqSet = IntStream.range(0, cqSamples).mapToObj(x -> RandomStringUtils.randomAlphabetic(5)).sorted().collect(Collectors.toList());

        final var ts = System.currentTimeMillis();
        final var rowName = RandomStringUtils.random(5);
        final var kvMap = new TreeMap<Key,Value>();
        final var rowDigits = Math.log10(rowMaximum) + 1;
        final var rowIdxPlaceholder = "%0" + (int)rowDigits + "d";
        var rowIdx = 0L;
        var kvSize = 0L;
        try (final var writer = RFile.newWriter().to(stream).build()) {
            while (rowIdx++ < rowMaximum) {
                final var rowNameIdx = "20240101_" + String.format(rowIdxPlaceholder, rowIdx);
                cfSet.forEach(cf -> {
                    cqSet.forEach(cq -> {
                        final var key = Key.builder()
                                .row(rowNameIdx)
                                .family(cf)
                                .qualifier(cq)
                                .visibility(cvSet.get(RandomUtils.nextInt(0, cvSet.size() - 1)))
                                .deleted(false)
                                .timestamp(ts)
                                .build();
                        final var value = valueEnabled ? new Value(RandomStringUtils.random(20)) : new Value();
                        kvMap.put(key, value);
                    });
                });
                kvSize += kvMap.entrySet().stream()
                        .map(kv -> kv.getKey().getSize() + kv.getValue().getSize())
                        .reduce(Integer::sum)
                        .orElseThrow();
                if (kvSize > sizeMaximum) {
                    break;
                }
                for (final var kv : kvMap.entrySet()) {
                    writer.append(kv.getKey(), kv.getValue());
                }
                kvMap.clear();
            }
        }
    }
}
