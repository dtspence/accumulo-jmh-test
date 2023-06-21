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
        if (result.getRawPrimaryResults().isEmpty()) {
            return Collections.emptyList();
        }
        final Result pr = result.getPrimaryResult();
        final String splitsRfileParam = benchmarkParams.getParam("splitsRfile");
        if (Strings.isNullOrEmpty(splitsRfileParam)) {
            return Collections.emptyList();
        }

        final String[] splitRFileArray = splitsRfileParam.split(",");
        final long splitCount = Long.parseLong(splitRFileArray[0]);
        final long rfilePerSplitCount = Long.parseLong(splitRFileArray[1]);
        final long rfileTotalNumber = splitCount * rfilePerSplitCount;
        final double rfilePerOperation = (pr.getScore() / rfileTotalNumber);
        final ScalarResult rfileResult = new ScalarResult("rfile/op", rfilePerOperation, pr.getScoreUnit(), AggregationPolicy.AVG);
        return Collections.singletonList(rfileResult);
    }

    @Override
    public String getDescription() {
        return "R-File Cost";
    }
}
