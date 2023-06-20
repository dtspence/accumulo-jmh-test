package dev.dtspence.accumulo;

import com.google.common.base.Strings;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

import java.util.Collection;
import java.util.Collections;

public class RFileImpact implements InternalProfiler {
    @Override
    public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams) {
        // no code
    }

    @Override
    public Collection<? extends Result> afterIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams, IterationResult result) {
        final var pr = result.getPrimaryResult();
        final var splitsRfileParam = benchmarkParams.getParam("splitsRfile");
        if (Strings.isNullOrEmpty(splitsRfileParam)) {
            return Collections.emptyList();
        }

        final var splitRFileArray = splitsRfileParam.split(",");
        final var splitCount = Long.parseLong(splitRFileArray[0]);
        final var rfilePerSplitCount = Long.parseLong(splitRFileArray[1]);
        final var rfileTotalNumber = splitCount * rfilePerSplitCount;
        final var rfilePerOperation = (pr.getScore() / rfileTotalNumber);
        final var rfileResult = new ScalarResult("rfile/op", rfilePerOperation, pr.getScoreUnit(), AggregationPolicy.AVG);
        return Collections.singletonList(rfileResult);
    }

    @Override
    public String getDescription() {
        return "R-File Cost";
    }
}
