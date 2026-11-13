package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.tsfile.file.metadata.statistics.*;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;

import java.io.IOException;
import java.util.*;

/** Failure protections and observations; no replacement Page selector. */
public final class PageSafety {
  final Map<String, String> parameters;
  final long started = System.nanoTime();
  int recovered = 0;
  long overlapCorrections = 0;

  PageSafety(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  Statistics readPage(IBatchDataIterator it, int start, int end, int length) throws IOException {
    Statistics s = null;
    while (it.hasNext()) {
      long t = it.currentTime();
      if (t >= start && t <= end) {
        if (s == null) s = new DoubleStatistics();
        s.update(t, ((Number) it.currentValue()).doubleValue());
      }
      it.next();
    }
    if (s == null) return null;
    int n = (int) Math.max(1000, s.getCount() / length + 1);
    s.idx = new int[n];
    s.edIdx = new int[n];
    s.updateStatistics();
    recovered++;
    return s;
  }

  void report(List<Statistics> units, List<Boolean> corrected) {
    int correctionCount = 0;
    for (boolean value : corrected) if (value) correctionCount++;
    System.out.println(
        "JOURNAL_NATIVE:{\"run_id\":\""
            + parameters.getOrDefault("run_id", "")
            + "\",\"entry\":\"original_page\",\"selected_chunk\":0,\"selected_page\":"
            + (units.size() - recovered)
            + ",\"selected_temp\":"
            + recovered
            + ",\"page_corrections\":"
            + (correctionCount + overlapCorrections)
            + ",\"query_build_chunk_calls\":0,\"raw_points\":-1,\"represented_subsequences\":-1,\"counter_scope\":\"legacy Page counters unavailable; no inferred coverage claims\",\"engine_query_seconds\":"
            + ((System.nanoTime() - started) / 1e9)
            + "}");
  }
}
