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
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.*;

/**
 * Original-statistics subtraction for identical, ordered overlapping Pages.
 *
 * <p>The legacy overlap rule retains a Page only up to the next Page's start minus one. Unlike
 * SeriesReader's merged batches, these records keep the original fitted payload and labels.
 * Nonidentical versions, deletions and noncontiguous inputs use the normal reader. No query-time
 * local clustering is performed here.
 */
final class OverlapStatistics {
  interface Corrector {
    Statistics correct(BatchData raw, Statistics stored, long start, long end, long nextStart);
  }

  static final class Page {
    final Statistics statistics;
    final IPageReader reader;
    final ChunkMetadata chunk;
    BatchData raw;
    double[] values;
    long retainedEnd;

    Page(Statistics statistics, IPageReader reader, ChunkMetadata chunk) {
      this.statistics = statistics;
      this.reader = reader;
      this.chunk = chunk;
      this.retainedEnd = statistics.getEndTime();
    }

    long start() {
      return statistics.getStartTime();
    }

    long end() {
      return statistics.getEndTime();
    }
  }

  static final class Selection {
    final List<Statistics> units = new ArrayList<>();
    long chunks, pages, corrections, rawPoints, overlapPages;
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
    // This O(1) gate leaves the section 8.2 sequential-data path unchanged.
    if (source.getUnseqResources().isEmpty() || corrector == null) return null;
    if (!parameters.getOrDefault("ablation", "normal").equals("normal")) return null;
    List<ChunkMetadata> chunks =
        new SeriesReader(path, sensors, type, context, source, null, null, null, true)
            .getAllChunkMetadatas();
    boolean overlap = false;
    List<ChunkMetadata> ordered = new ArrayList<>(chunks);
    ordered.sort(Comparator.comparingLong(ChunkMetadata::getStartTime));
    long right = Long.MIN_VALUE;
    for (ChunkMetadata chunk : ordered) {
      overlap |= chunk.getStartTime() <= right;
      right = Math.max(right, chunk.getEndTime());
    }
    if (!overlap) return null;
    List<Page> pages = new ArrayList<>();
    for (ChunkMetadata metadata : chunks) {
      if (metadata.getChunkLoader() instanceof MemChunkLoader
          || (metadata.getDeleteIntervalList() != null
              && !metadata.getDeleteIntervalList().isEmpty())) return null;
      Chunk chunk = metadata.getChunkLoader().loadChunk(metadata);
      chunk.setFromOldFile(metadata.isFromOldTsFile());
      ChunkReader reader = new ChunkReader(chunk, null);
      for (IPageReader page : reader.loadPageReaderList()) {
        if (page.isModified()
            || page.getStatistics() == null
            || !page.getStatistics().journalPayload()) return null;
        pages.add(new Page(page.getStatistics(), page, metadata));
      }
    }
    return selectPages(pages, parameters, a, b, l, corrector);
  }

  static Selection selectPages(
      List<Page> pages, Map<String, String> parameters, int a, int b, int l, Corrector corrector)
      throws IOException {
    pages.sort(Comparator.comparingLong(Page::start));
    boolean overlapping = false;
    for (int i = 1; i < pages.size(); i++) {
      Page previous = pages.get(i - 1), current = pages.get(i);
      // The old next-start clipping rule requires strictly increasing endpoints.
      if (current.start() <= previous.start() || current.end() <= previous.end()) return null;
      if (current.start() <= previous.end()) {
        previous.retainedEnd = current.start() - 1;
        overlapping = true;
      }
    }
    if (!overlapping) return null;
    // Verify the experimental duplicate semantics before reusing any payload.
    // Adjacent equal overlaps imply equality for every multiply covered timestamp.
    Page previous = null;
    for (Page current : pages) {
      long size = current.end() - current.start() + 1;
      if (size != current.statistics.getCount() || size > Integer.MAX_VALUE) return null;
      current.raw = current.reader.getAllSatisfiedPageData(true);
      if (current.raw.length() != size) return null;
      current.values = new double[(int) size];
      IBatchDataIterator it = current.raw.getBatchDataIterator();
      int index = 0;
      while (it.hasNext()) {
        if (it.currentTime() != current.start() + index) return null;
        double value = ((Number) it.currentValue()).doubleValue();
        if (!Double.isFinite(value)) return null;
        current.values[index++] = value;
        it.next();
      }
      if (previous != null) {
        for (long t = current.start(); t <= previous.end(); t++)
          if (Double.doubleToLongBits(current.values[(int) (t - current.start())])
              != Double.doubleToLongBits(previous.values[(int) (t - previous.start())]))
            return null;
        previous.values = null;
      }
      previous = current;
    }
    if (previous != null) previous.values = null;

    boolean pageLevel = parameters.getOrDefault("level", "page").equals("page");
    double tp = Double.parseDouble(parameters.getOrDefault("theta_page", "0.8"));
    double tc = Double.parseDouble(parameters.getOrDefault("theta_chunk", "0.8"));
    Set<ChunkMetadata> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Page p : pages) if (p.chunk != null) distinct.add(p.chunk);
    Set<ChunkMetadata> coarse = Collections.newSetFromMap(new IdentityHashMap<>());
    if (!pageLevel) {
      for (ChunkMetadata c : distinct) {
        boolean clear = true;
        for (ChunkMetadata other : distinct)
          if (c != other
              && c.getStartTime() <= other.getEndTime()
              && other.getStartTime() <= c.getEndTime()) {
            clear = false;
            break;
          }
        if (clear && ratio(c.getStartTime(), c.getEndTime(), a, b) >= tc) coarse.add(c);
      }
    }
    Set<ChunkMetadata> used = Collections.newSetFromMap(new IdentityHashMap<>());
    Selection result = new Selection();
    for (Page page : pages) {
      if (page.end() < a || page.start() > b) continue;
      if (page.retainedEnd != page.end()) result.overlapPages++;
      if (coarse.contains(page.chunk)) {
        if (used.add(page.chunk)) {
          result.units.add(Statistics.journalChunkView(page.chunk.getStatistics()));
          result.chunks++;
        }
      } else if (page.retainedEnd == page.end() && ratio(page.start(), page.end(), a, b) >= tp) {
        result.units.add(Statistics.journalChunkView(page.statistics));
        result.pages++;
      } else {
        long start = Math.max(a, page.start()), end = Math.min(b, page.retainedEnd);
        if (start > end) continue;
        Statistics corrected = subtract(page, start, end, l, corrector);
        result.units.add(corrected);
        result.corrections++;
        result.rawPoints += page.raw.length();
      }
      page.raw = null;
    }
    result.units.sort(Comparator.comparingLong(Statistics::getStartTime));
    return result;
  }

  static Statistics subtract(Page page, long start, long end, int l, Corrector corrector) {
    long completeStart = -Math.floorDiv(-start, l) * l;
    long completeEnd = Math.floorDiv(end + 1, l) * l - 1;
    Statistics corrected;
    if (completeStart <= completeEnd) {
      page.raw.resetBatchData();
      // Use the existing subtraction kernel, with complete-grid bounds. A subsequence
      // cut by overlap ownership is reconstructed once from adjacent fragments below.
      corrected =
          corrector.correct(page.raw, page.statistics.clone(), completeStart, completeEnd, -1);
    } else {
      corrected = page.statistics.clone();
      Arrays.fill(corrected.idx, -1);
      Arrays.fill(corrected.edIdx, -1);
      Arrays.fill(corrected.edCounts, 0);
      for (double[] c : corrected.edCentroids) Arrays.fill(c, 0);
      for (int c = 0; c < corrected.centroids.length; c++) clearK(corrected, c);
    }
    for (int c = 0; c < corrected.centroids.length; c++)
      if (MathUtils.clusterMemberNum(corrected.idx, c) == 0) clearK(corrected, c);
    corrected.headExtraPoints = new double[l];
    corrected.tailExtraPoints = new double[l];
    page.raw.resetBatchData();
    IBatchDataIterator it = page.raw.getBatchDataIterator();
    while (it.hasNext()) {
      long time = it.currentTime();
      if (time >= start && time <= end) {
        int position = (int) Math.floorMod(time, (long) l);
        double value = ((Number) it.currentValue()).doubleValue();
        if (time < completeStart) corrected.headExtraPoints[position] = value;
        if (time > completeEnd) corrected.tailExtraPoints[position] = value;
      }
      it.next();
    }
    corrected.setStartTime(start);
    corrected.setEndTime(end);
    corrected.setCount((int) (end - start + 1));
    corrected.journalGridFragments = true;
    return corrected;
  }

  private static void clearK(Statistics statistics, int cluster) {
    Arrays.fill(statistics.centroids[cluster], 0);
    statistics.deltas[cluster] = 0;
    for (double[] row : statistics.sumMatrices[cluster]) Arrays.fill(row, 0);
  }

  private static double ratio(long start, long end, int a, int b) {
    return Math.max(0L, Math.min(end, b) - Math.max(start, a) + 1) * 1.0 / (end - start + 1);
  }
}
