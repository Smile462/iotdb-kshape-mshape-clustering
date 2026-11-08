package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.series.*;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.*;
import org.apache.iotdb.tsfile.read.common.*;

import java.io.IOException;
import java.lang.management.*;
import java.util.*;

/** Controlled journal adapter. Storage and clustering remain in IoTDB. */
public final class JournalSelection {
  long chunks,
      pages,
      temp,
      raw,
      boundaryRaw,
      candidateRaw,
      buildCalls,
      payload,
      represented,
      outside;
  long correctedPages;
  long started = System.nanoTime(), selectionNanos, candidateNanos;
  long candidateHash = 0xcbf29ce484222325L;
  int candidateCount = 0;
  volatile boolean memoryStop = false;
  Thread memoryThread;
  volatile long heapPeak = 0, directPeak = 0;
  long heapBaseline = 0, directBaseline = 0;

  void sampleMemory() {
    long heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed(), direct = 0;
    for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class))
      if (pool.getName().equals("direct")) direct += pool.getMemoryUsed();
    heapPeak = Math.max(heapPeak, heap);
    directPeak = Math.max(directPeak, direct);
  }

  final Map<String, String> params;
  final int a, b, l, k;

  JournalSelection(Map<String, String> p, int start, int end, int length, int clusters) {
    params = p;
    a = start;
    b = end;
    l = length;
    k = clusters;
    if (p.getOrDefault("profile", "false").equals("true")) {
      sampleMemory();
      heapBaseline = heapPeak;
      directBaseline = directPeak;
      memoryThread =
          new Thread(
              () -> {
                while (!memoryStop) {
                  sampleMemory();
                  try {
                    Thread.sleep(10);
                  } catch (InterruptedException e) {
                    break;
                  }
                }
              });
      memoryThread.setDaemon(true);
      memoryThread.start();
    }
  }

  double ratio(Statistics s) {
    return Math.max(0L, Math.min(b, s.getEndTime()) - Math.max(a, s.getStartTime()) + 1)
        * 1.0
        / (s.getEndTime() - s.getStartTime() + 1);
  }

  void account(Statistics s) {
    payload +=
        8L * k * l * l
            + 8L * k * l
            + 8L * k
            + 8L * (k + 2) * l
            + 12L * (k + 2)
            + 4L * (s.idx.length + s.edIdx.length)
            + 16L * l;
    for (int n : s.edCounts) represented += n;
    outside += Math.max(0, a - s.getStartTime()) + Math.max(0, s.getEndTime() - b);
  }

  void addTemporaryPage(List<Statistics> units, BatchData batch) {
    // One reader page, one temporary object for its intersection with Q.
    // Do not accumulate raw observations from another page or split by size.
    Statistics current = null;
    IBatchDataIterator it = batch.getBatchDataIterator();
    while (it.hasNext()) {
      long t = it.currentTime();
      double v = ((Number) it.currentValue()).doubleValue();
      raw++;
      boundaryRaw++;
      if (t >= a && t <= b) {
        if (current == null) current = new DoubleStatistics();
        current.update(t, v);
      }
      it.next();
    }
    if (current == null) return;
    // Temporary labels stay in memory. Size their arrays for this page rather
    // than changing the clustering granularity to fit persisted 1000-slot arrays.
    int capacity = (int) Math.max(1000, current.getCount() / l + 1);
    current.idx = new int[capacity];
    current.edIdx = new int[capacity];
    current.updateStatistics();
    current = Statistics.journalChunkView(current);
    units.add(current);
    temp++;
    account(current);
  }

  interface PageCorrector {
    Statistics correct(BatchData batch, Statistics statistics);
  }

  void addCorrectedPage(
      List<Statistics> units, BatchData batch, Statistics stored, PageCorrector corrector)
      throws IOException {
    if (batch.length() == 0) throw new IOException("Usable Page metadata has no raw observations");
    Statistics working = stored.clone();
    working.idx = stored.idx.clone();
    working.edIdx = stored.edIdx.clone();
    Statistics corrected = corrector.correct(batch, working);
    raw += batch.length();
    boundaryRaw += batch.length();
    // The Page method rounds its metadata bounds. Restore the actual retained
    // raw span and adapt only fragments for the Chunk aggregator's grid layout.
    corrected.headExtraPoints = new double[l];
    corrected.tailExtraPoints = new double[l];
    batch.resetBatchData();
    IBatchDataIterator iterator = batch.getBatchDataIterator();
    long first = Long.MAX_VALUE, last = Long.MIN_VALUE;
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      if (time >= a && time <= b) {
        first = Math.min(first, time);
        last = Math.max(last, time);
      }
      iterator.next();
    }
    if (first == Long.MAX_VALUE) return;
    long firstComplete = -Math.floorDiv(-first, l) * l;
    long tailStart = Math.floorDiv(last + 1, l) * l;
    batch.resetBatchData();
    iterator = batch.getBatchDataIterator();
    while (iterator.hasNext()) {
      long time = iterator.currentTime();
      if (time >= first && time <= last) {
        int position = (int) Math.floorMod(time, (long) l);
        double value = ((Number) iterator.currentValue()).doubleValue();
        if (time < firstComplete) corrected.headExtraPoints[position] = value;
        if (time >= tailStart) corrected.tailExtraPoints[position] = value;
      }
      iterator.next();
    }
    corrected.setStartTime(first);
    corrected.setEndTime(last);
    corrected.journalGridFragments = true;
    units.add(corrected);
    correctedPages++;
    account(corrected);
  }

  static double[] boundarySequence(List<Statistics> units, int i, int l) {
    Statistics current = units.get(i);
    long start = current.getStartTime(), row = start / l * l, end = row + l - 1;
    if (start == row || current.getEndTime() < end) return null;
    double[] merged = new double[l];
    for (long t = start; t <= end; t++)
      merged[(int) (t - row)] = current.headExtraPoints[(int) (t - row)];
    long needed = start - 1;
    // A resolved overlap can yield pages shorter than l. Join only the one
    // boundary subsequence, traversing as many contiguous fragments as needed.
    for (int j = i - 1; j >= 0 && needed >= row; j--) {
      Statistics previous = units.get(j);
      if (previous.getEndTime() != needed) return null;
      long from = Math.max(row, previous.getStartTime());
      double[] fragment =
          previous.getStartTime() > row ? previous.headExtraPoints : previous.tailExtraPoints;
      for (long t = from; t <= needed; t++) merged[(int) (t - row)] = fragment[(int) (t - row)];
      needed = from - 1;
    }
    return needed < row ? merged : null;
  }

  List<Statistics> select(
      PartialPath path,
      Set<String> sensors,
      TSDataType type,
      QueryContext ctx,
      QueryDataSource data)
      throws IOException {
    return select(path, sensors, type, ctx, data, null);
  }

  List<Statistics> select(
      PartialPath path,
      Set<String> sensors,
      TSDataType type,
      QueryContext ctx,
      QueryDataSource data,
      PageCorrector corrector)
      throws IOException {
    double tp = Double.parseDouble(params.getOrDefault("theta_page", "0.8"));
    double tc = Double.parseDouble(params.getOrDefault("theta_chunk", "0.8"));
    boolean page = params.getOrDefault("level", "page").equals("page");
    String ablation = params.getOrDefault("ablation", "normal");
    IAggregateReader reader =
        new SeriesAggregateReader(path, sensors, type, ctx, data, null, null, null, true);
    List<Statistics> units = new ArrayList<>();
    while (reader.hasNextFile())
      while (reader.hasNextChunk()) {
        Statistics cs = reader.currentChunkStatistics();
        if (cs.idx[0] != Integer.MIN_VALUE) cs = Statistics.journalChunkView(cs);
        double cr = ratio(cs);
        if (cr <= 0) {
          reader.skipCurrentChunk();
          continue;
        }
        boolean use =
            !page
                && reader.canUseCurrentChunkStatistics()
                && (cr >= tc || ablation.equals("coarse"));
        if (use && !ablation.equals("dynamic")) {
          units.add(cs);
          chunks++;
          account(cs);
          reader.skipCurrentChunk();
          continue;
        }
        List<Statistics> dynamic = new ArrayList<>();
        while (reader.hasNextPage()) {
          Statistics storedPage = reader.currentPageStatistics();
          Statistics ps = storedPage == null ? null : Statistics.journalChunkView(storedPage);
          double pr = ps == null ? 1. : ratio(ps);
          boolean usablePage = storedPage != null && reader.canUseCurrentPageStatistics();
          if (pr <= 0) {
            reader.skipCurrentPage();
            continue;
          }
          if (usablePage && ((use && ablation.equals("dynamic")) || pr >= tp)) {
            if (use && ablation.equals("dynamic")) dynamic.add(ps);
            else {
              units.add(ps);
              pages++;
              account(ps);
            }
            reader.skipCurrentPage();
          } else if (corrector != null && usablePage && pr < tp) {
            addCorrectedPage(units, reader.nextPage(), storedPage, corrector);
          } else {
            addTemporaryPage(units, reader.nextPage());
          }
        }
        if (!dynamic.isEmpty()) {
          Statistics sum = new DoubleStatistics();
          sum.updateChunkStatistics(dynamic);
          sum.updateChunkMStatistics(dynamic);
          units.add(sum);
          chunks++;
          buildCalls++;
          account(sum);
        }
      }
    for (int i = 1; i < units.size(); i++) if (boundarySequence(units, i, l) != null) represented++;
    if (Boolean.getBoolean("journal.debug_units"))
      for (Statistics u : units)
        System.out.println(
            "JOURNAL_UNIT a="
                + u.getStartTime()
                + " b="
                + u.getEndTime()
                + " points="
                + u.getCount()
                + " m_weight="
                + Arrays.stream(u.edCounts).sum());
    selectionNanos = System.nanoTime() - started;
    return units;
  }

  void report() {
    if (memoryThread != null) {
      memoryStop = true;
      try {
        memoryThread.join(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      sampleMemory();
    }
    System.out.println(
        "JOURNAL_NATIVE:{\"run_id\":\""
            + params.getOrDefault("run_id", "")
            + "\",\"selected_chunk\":"
            + chunks
            + ",\"selected_page\":"
            + pages
            + ",\"selected_temp\":"
            + temp
            + ",\"page_corrections\":"
            + correctedPages
            + ",\"raw_points\":"
            + raw
            + ",\"boundary_raw_points\":"
            + boundaryRaw
            + ",\"candidate_raw_points\":"
            + candidateRaw
            + ",\"query_build_chunk_calls\":"
            + buildCalls
            + ",\"represented_subsequences\":"
            + represented
            + ",\"outside_unit_time_points\":"
            + outside
            + ",\"estimated_selected_payload_bytes\":"
            + payload
            + ",\"selection_seconds\":"
            + (selectionNanos / 1e9)
            + ",\"candidate_seconds\":"
            + (candidateNanos / 1e9)
            + ",\"candidate_count\":"
            + candidateCount
            + ",\"candidate_hash\":\""
            + String.format("%016x", candidateHash)
            + "\",\"heap_baseline_bytes\":"
            + heapBaseline
            + ",\"heap_peak_bytes\":"
            + heapPeak
            + ",\"direct_baseline_bytes\":"
            + directBaseline
            + ",\"direct_peak_bytes\":"
            + directPeak
            + ",\"jvm_memory_scope\":\"selection_and_solver_only; process probe covers full request\",\"engine_query_seconds\":"
            + ((System.nanoTime() - started) / 1e9)
            + "}");
  }
}
