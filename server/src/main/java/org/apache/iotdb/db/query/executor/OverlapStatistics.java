/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.chunk.MemChunkLoader;
import org.apache.iotdb.db.query.reader.series.SeriesReader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.util.CompleteSubsequences;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.*;

/** Reuse complete-window metadata across range overlaps with unique timestamps. */
final class OverlapStatistics {
  interface Corrector {
    Statistics correct(BatchData raw, Statistics stored, long start, long end, long nextStart);
  }

  static final class Page {
    final Statistics statistics;
    final IPageReader reader;
    final ChunkMetadata chunk;
    BatchData raw;

    Page(Statistics statistics, IPageReader reader, ChunkMetadata chunk) {
      this.statistics = statistics;
      this.reader = reader;
      this.chunk = chunk;
    }

    long start() {
      return statistics.getStartTime();
    }

    long end() {
      return statistics.getEndTime();
    }

    boolean sparse() {
      return statistics.getCount() < end() - start() + 1;
    }

    BatchData decode(Selection result) throws IOException {
      if (raw == null) {
        raw = reader.getAllSatisfiedPageData(true);
        result.rawPoints += raw.length();
      }
      return raw;
    }
  }

  static final class Selection {
    final List<Statistics> units = new ArrayList<>();
    final List<double[]> sequences = new ArrayList<>();
    long chunks, pages, corrections, rawPoints, overlapPages, reusedWindows, incompleteWindows;
  }

  static Selection select(
      PartialPath path,
      Set<String> sensors,
      TSDataType type,
      QueryContext context,
      QueryDataSource source,
      Map<String, String> parameters,
      int a,
      int b,
      int l,
      Corrector corrector)
      throws IOException {
    // Preserve the existing sequential-data path, including ordinary Page correction.
    if (source.getUnseqResources().isEmpty() || corrector == null) return null;
    if (!parameters.getOrDefault("ablation", "normal").equals("normal")) return null;
    List<ChunkMetadata> chunks =
        new SeriesReader(path, sensors, type, context, source, null, null, null, true)
            .getAllChunkMetadatas();
    List<ChunkMetadata> ordered = new ArrayList<>(chunks);
    ordered.sort(Comparator.comparingLong(ChunkMetadata::getStartTime));
    long right = Long.MIN_VALUE;
    boolean overlapping = false;
    for (ChunkMetadata chunk : ordered) {
      overlapping |= chunk.getStartTime() <= right;
      right = Math.max(right, chunk.getEndTime());
    }
    if (!overlapping) return null;
    List<Page> pages = new ArrayList<>();
    for (ChunkMetadata metadata : chunks) {
      if (metadata.getChunkLoader() instanceof MemChunkLoader
          || (metadata.getDeleteIntervalList() != null
              && !metadata.getDeleteIntervalList().isEmpty()))
        throw new IOException("Unique-overlap metadata requires sealed, undeleted Pages");
      Chunk chunk = metadata.getChunkLoader().loadChunk(metadata);
      chunk.setFromOldFile(metadata.isFromOldTsFile());
      ChunkReader reader = new ChunkReader(chunk, null);
      for (IPageReader page : reader.loadPageReaderList()) {
        if (page.isModified()
            || page.getStatistics() == null
            || !page.getStatistics().journalPayload())
          throw new IOException("Missing overlap Page payload; rebuild imported metadata");
        pages.add(new Page(page.getStatistics(), page, metadata));
      }
    }
    return selectPages(pages, parameters, a, b, l, corrector);
  }

  static Selection selectPages(
      List<Page> pages,
      Map<String, String> parameters,
      int a,
      int b,
      int l,
      Corrector unusedLegacyCorrector)
      throws IOException {
    pages.sort(Comparator.comparingLong(Page::start));
    long right = Long.MIN_VALUE;
    boolean overlapping = false;
    for (Page page : pages) {
      overlapping |= page.start() <= right;
      right = Math.max(right, page.end());
      if (page.sparse()
          && (page.statistics.idx[page.statistics.idx.length - 1] != -2
              || page.statistics.edIdx[page.statistics.edIdx.length - 1] != -2))
        throw new IOException(
            "Sparse Page has obsolete packed labels; rebuild metadata with current code");
    }
    if (!overlapping) return null;
    Selection result = new Selection();
    long first = ceil(Math.max((long) a, pages.get(0).start()), l);
    long last = Math.floorDiv(Math.min((long) b, right) + 1, l) * l - l;
    if (first > last) throw new IOException("Query contains no complete original-grid subsequence");
    Map<ChunkMetadata, List<Page>> groups = new IdentityHashMap<>();
    for (Page page : pages)
      if (page.chunk != null)
        groups.computeIfAbsent(page.chunk, key -> new ArrayList<>()).add(page);
    boolean pageLevel = parameters.getOrDefault("level", "page").equals("page");
    double tp = Double.parseDouble(parameters.getOrDefault("theta_page", "0.8"));
    double tc = Double.parseDouble(parameters.getOrDefault("theta_chunk", "0.8"));
    Set<ChunkMetadata> used = Collections.newSetFromMap(new IdentityHashMap<>());
    Set<Long> covered = new HashSet<>();
    for (Page page : pages) {
      if (page.end() < a || page.start() > b) continue;
      if (page.sparse()) result.overlapPages++;
      ChunkMetadata chunk = page.chunk;
      if (!pageLevel
          && chunk != null
          && chunk.getStatistics().journalPayload()
          && chunk.getStatistics().idx[0] == Integer.MIN_VALUE
          && ratio(chunk.getStartTime(), chunk.getEndTime(), a, b) >= tc) {
        if (used.add(chunk)) {
          result.units.add(Statistics.journalChunkView(chunk.getStatistics()));
          result.chunks++;
          List<Page> members = groups.get(chunk);
          boolean sparse = false;
          for (Page member : members) {
            mark(member.statistics, member.start(), l, first, last, covered);
            sparse |= member.sparse();
          }
          // Dense Chunk payloads also include their original complete Page-boundary windows.
          if (!sparse)
            for (int i = 1; i < members.size(); i++) {
              Page previous = members.get(i - 1), current = members.get(i);
              long row = Math.floorDiv(current.start(), l) * l;
              if (current.start() != row
                  && previous.end() + 1 == current.start()
                  && previous.start() <= row
                  && current.end() >= row + l - 1
                  && row >= first
                  && row <= last
                  && !covered.add(row))
                throw new IOException("Duplicate complete subsequence in Chunk payloads");
            }
        }
        continue;
      }
      Statistics unit = page.statistics;
      if (ratio(page.start(), page.end(), a, b) < tp) {
        unit = crop(page, a, b, l, result);
        result.corrections++;
      } else result.pages++;
      result.units.add(Statistics.journalChunkView(unit));
      mark(unit, page.start(), l, first, last, covered);
    }
    TreeMap<Long, PartialWindow> missing = new TreeMap<>();
    for (long row = first; row <= last; row += l)
      if (!covered.contains(row)) missing.put(row, new PartialWindow(l));
    result.reusedWindows = covered.size();
    for (Page page : pages) {
      long low = Math.floorDiv(Math.max(page.start(), first), l) * l;
      long high = Math.min(Math.floorDiv(page.end(), l) * l, last);
      if (low > high || missing.subMap(low, true, high, true).isEmpty()) continue;
      BatchData raw = page.decode(result);
      for (int i = 0; i < raw.length(); i++) {
        long t = raw.getTimeByIndex(i), row = Math.floorDiv(t, l) * l;
        PartialWindow window = missing.get(row);
        if (window == null) continue;
        int position = (int) Math.floorMod(t, (long) l);
        if (window.present.get(position))
          throw new IOException("Duplicate timestamp in unique-overlap input: " + t);
        double value = raw.getDoubleByIndex(i);
        if (!Double.isFinite(value)) throw new IOException("Nonfinite overlap observation");
        window.values[position] = value;
        window.present.set(position);
      }
    }
    for (PartialWindow window : missing.values()) {
      if (window.present.cardinality() == l) result.sequences.add(window.values);
      else result.incompleteWindows++;
    }
    if (result.units.isEmpty()) throw new IOException("No overlap metadata intersects query");
    result.units.sort(Comparator.comparingLong(Statistics::getStartTime));
    System.out.println(
        "JOURNAL_OVERLAP:{\"reused_windows\":"
            + result.reusedWindows
            + ",\"reconstructed_windows\":"
            + result.sequences.size()
            + ",\"incomplete_windows_skipped\":"
            + result.incompleteWindows
            + ",\"selected_chunks\":"
            + result.chunks
            + ",\"selected_pages\":"
            + result.pages
            + ",\"corrected_pages\":"
            + result.corrections
            + ",\"raw_points\":"
            + result.rawPoints
            + ",\"query_local_fit_calls\":0}");
    return result;
  }

  private static final class PartialWindow {
    final double[] values;
    final BitSet present;

    PartialWindow(int length) {
      values = new double[length];
      present = new BitSet(length);
    }
  }

  private static void mark(
      Statistics statistics, long originalStart, int l, long first, long last, Set<Long> covered)
      throws IOException {
    long base = ceil(originalStart, l);
    for (int slot = 0; slot < statistics.idx.length; slot++) {
      boolean shape = statistics.idx[slot] >= 0;
      int label = statistics.edIdx[slot];
      boolean medoid =
          label >= 0 && label < statistics.edCounts.length && statistics.edCounts[label] > 0;
      if (shape != medoid) throw new IOException("K/M Page label coverage differs");
      if (!shape) continue;
      long row = base + (long) slot * l;
      if (row >= first && row <= last && !covered.add(row))
        throw new IOException("Duplicate complete subsequence across unique Pages");
    }
  }

  static Statistics crop(Page page, long a, long b, int l, Selection result) throws IOException {
    Statistics copy = page.statistics.clone();
    copy.idx = page.statistics.idx.clone();
    copy.edIdx = page.statistics.edIdx.clone();
    long base = ceil(page.start(), l);
    Map<Long, double[]> windows = new HashMap<>();
    BatchData raw = page.decode(result);
    for (CompleteSubsequences.Window window : CompleteSubsequences.read(raw, l))
      windows.put(window.start, window.values);
    for (int slot = 0; slot < copy.idx.length; slot++) {
      long row = base + (long) slot * l;
      if (copy.idx[slot] < 0 || (row >= a && row + l - 1 <= b)) continue;
      double[] original = windows.get(row);
      if (original == null)
        throw new IOException("Stored complete-window label has no corresponding observations");
      double[] normalized = MathUtils._zscore(original);
      int shape = copy.idx[slot], medoid = copy.edIdx[slot];
      for (int u = 0; u < l; u++)
        for (int v = 0; v < l; v++) copy.sumMatrices[shape][u][v] -= normalized[u] * normalized[v];
      int count = copy.edCounts[medoid];
      if (count <= 0) throw new IOException("Invalid stored M count");
      for (int u = 0; u < l; u++)
        copy.edCentroids[medoid][u] =
            count == 1 ? 0 : (copy.edCentroids[medoid][u] * count - normalized[u]) / (count - 1);
      copy.edCounts[medoid]--;
      copy.idx[slot] = -1;
      copy.edIdx[slot] = -1;
    }
    for (int c = 0; c < copy.centroids.length; c++)
      if (MathUtils.clusterMemberNum(copy.idx, c) == 0) {
        Arrays.fill(copy.centroids[c], 0);
        for (double[] row : copy.sumMatrices[c]) Arrays.fill(row, 0);
        copy.deltas[c] = 0;
      }
    Arrays.fill(copy.headExtraPoints, 0);
    Arrays.fill(copy.tailExtraPoints, 0);
    copy.journalGridFragments = true;
    return copy;
  }

  static void mergeK(Selection selection, double[][] centers, double[][][] matrices, int[] counts) {
    if (selection == null) return;
    for (double[] raw : selection.sequences) {
      double[] value = MathUtils._zscore(raw);
      int cluster = nearest(value, centers, counts);
      if (counts[cluster] == 0) centers[cluster] = value.clone();
      for (int u = 0; u < value.length; u++)
        for (int v = 0; v < value.length; v++) matrices[cluster][u][v] += value[u] * value[v];
      counts[cluster]++;
    }
  }

  static void mergeM(Selection selection, double[][] centers, int[] counts) {
    if (selection == null) return;
    for (double[] raw : selection.sequences) {
      double[] value = MathUtils._zscore(raw);
      int cluster = nearest(value, centers, counts);
      for (int u = 0; u < value.length; u++)
        centers[cluster][u] =
            (centers[cluster][u] * counts[cluster] + value[u]) / (counts[cluster] + 1);
      counts[cluster]++;
    }
  }

  private static int nearest(double[] value, double[][] centers, int[] counts) {
    int best = -1;
    double distance = Double.POSITIVE_INFINITY;
    for (int c = 0; c < centers.length; c++)
      if (counts[c] > 0) {
        double d = MathUtils._sbd(value, centers[c]);
        if (d < distance) {
          best = c;
          distance = d;
        }
      }
    return best < 0 ? 0 : best;
  }

  private static long ceil(long value, int l) {
    return -Math.floorDiv(-value, (long) l) * l;
  }

  private static double ratio(long start, long end, long a, long b) {
    return Math.max(0L, Math.min(end, b) - Math.max(start, a) + 1) * 1.0 / (end - start + 1);
  }
}
