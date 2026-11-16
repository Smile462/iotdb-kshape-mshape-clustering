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
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.*;
import org.apache.iotdb.tsfile.file.metadata.statistics.util.CompleteSubsequences;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;

import org.junit.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;

public class OverlapStatisticsTest {
  static final int L = 16;

  @Before
  public void configure() {
    TSFileDescriptor.getInstance().getConfig().setClusterNum(3);
    TSFileDescriptor.getInstance().getConfig().setSeqLength(L);
  }

  static double value(long t) {
    return PageMCorrectionTest.value(t);
  }

  static final class Reader implements IPageReader {
    final Statistics statistics = new DoubleStatistics();
    final BatchData raw = new BatchData(TSDataType.DOUBLE);
    int decoded;

    Reader(Collection<Integer> timestamps) {
      for (int t : timestamps) {
        statistics.update(t, value(t));
        raw.putDouble(t, value(t));
      }
      raw.flip();
      statistics.updateStatistics();
    }

    public BatchData getAllSatisfiedPageData(boolean ascending) {
      decoded++;
      raw.resetBatchData();
      return raw;
    }

    public Statistics getStatistics() {
      return statistics;
    }

    public void setFilter(Filter filter) {}

    public boolean isModified() {
      return false;
    }
  }

  static List<Integer> range(int first, int end) {
    List<Integer> values = new ArrayList<>();
    for (int t = first; t < end; t++) values.add(t);
    return values;
  }

  static List<Reader> pair(int first, int size, int overlap) {
    List<Integer> left = new ArrayList<>(), right = new ArrayList<>();
    int a = first + size - overlap / 2, b = first + size + overlap / 2;
    left.addAll(range(first, a));
    right.addAll(range(b, first + 2 * size));
    for (int t = a; t < b; t++) ((t - a) % 2 == 1 ? left : right).add(t);
    Collections.sort(left);
    Collections.sort(right);
    return Arrays.asList(new Reader(left), new Reader(right));
  }

  static List<Reader> four() {
    List<Reader> readers = new ArrayList<>(pair(0, 256, 64));
    readers.addAll(pair(512, 256, 64));
    return readers;
  }

  static ChunkMetadata chunk(List<Reader> readers) {
    Statistics statistics = new DoubleStatistics();
    List<Statistics> pages = new ArrayList<>();
    for (Reader reader : readers) pages.add(reader.statistics);
    statistics.updateChunkStatistics(pages);
    statistics.updateChunkMStatistics(pages);
    return new ChunkMetadata("s0", TSDataType.DOUBLE, 0, statistics);
  }

  static List<OverlapStatistics.Page> pages(List<Reader> readers, boolean chunks) {
    List<OverlapStatistics.Page> result = new ArrayList<>();
    ChunkMetadata a = chunks ? chunk(Arrays.asList(readers.get(0), readers.get(2))) : null;
    ChunkMetadata b = chunks ? chunk(Arrays.asList(readers.get(1), readers.get(3))) : null;
    for (int i = 0; i < readers.size(); i++)
      result.add(
          new OverlapStatistics.Page(
              readers.get(i).statistics, readers.get(i), i % 2 == 0 ? a : b));
    return result;
  }

  static Map<String, String> params(String level) {
    Map<String, String> result = new HashMap<>();
    result.put("level", level);
    result.put("theta_page", "0.8");
    result.put("theta_chunk", "0.8");
    return result;
  }

  static OverlapStatistics.Selection select(
      List<OverlapStatistics.Page> pages, Map<String, String> p, int a, int b) throws Exception {
    return OverlapStatistics.selectPages(
        pages,
        p,
        a,
        b,
        L,
        (raw, stored, start, end, next) -> {
          throw new AssertionError("Legacy duplicate clipping used");
        });
  }

  static int represented(OverlapStatistics.Selection selection, boolean medoid) {
    int count = selection.sequences.size();
    for (Statistics s : selection.units) {
      if (medoid) for (int n : s.edCounts) count += n;
      else
        for (int c = 0; c < s.centroids.length; c++) count += MathUtils.clusterMemberNum(s.idx, c);
    }
    return count;
  }

  @Test
  public void sparsePrecomputeLabelsReferOnlyToCompleteTimeWindows() {
    for (Reader reader : four()) {
      List<CompleteSubsequences.Window> expected = CompleteSubsequences.read(reader.raw, L);
      long base = (reader.statistics.getStartTime() + L - 1) / L * L;
      Set<Long> actual = new HashSet<>();
      for (int i = 0; i < reader.statistics.idx.length; i++)
        if (reader.statistics.idx[i] >= 0) actual.add(base + (long) i * L);
      assertEquals(expected.size(), actual.size());
      for (CompleteSubsequences.Window w : expected) assertTrue(actual.contains(w.start));
      assertEquals(14, actual.size());
      assertEquals(-2, reader.statistics.idx[999]);
      assertEquals(14, Arrays.stream(reader.statistics.edCounts).sum());
    }
  }

  @Test
  public void pageMetadataAndReconstructedWindowsCoverQueryExactlyOnce() throws Exception {
    List<Reader> readers = four();
    OverlapStatistics.Selection result = select(pages(readers, false), params("page"), 0, 1023);
    assertEquals(56, result.reusedWindows);
    assertEquals(8, result.sequences.size());
    assertEquals(64, represented(result, false));
    assertEquals(64, represented(result, true));
    for (double[] sequence : result.sequences) {
      boolean exact = false;
      for (int t = 0; t < 1024; t += L) {
        double[] expected = new double[L];
        for (int j = 0; j < L; j++) expected[j] = value(t + j);
        exact |= Arrays.equals(expected, sequence);
      }
      assertTrue("No reordered, padded, or invented observations", exact);
    }
    for (Reader reader : readers) assertEquals(1, reader.decoded);
  }

  @Test
  public void rangeOverlapDoesNotForceChunkDescent() throws Exception {
    OverlapStatistics.Selection result = select(pages(four(), true), params("chunk"), 0, 1023);
    assertEquals(2, result.chunks);
    assertEquals(0, result.pages);
    assertEquals(64, represented(result, false));
    assertEquals(64, represented(result, true));
  }

  @Test
  public void thresholdsStillControlChunkAndPageSelection() throws Exception {
    Map<String, String> p = params("chunk");
    p.put("theta_chunk", "1.1");
    OverlapStatistics.Selection result = select(pages(four(), true), p, 0, 1023);
    assertEquals(0, result.chunks);
    assertEquals(4, result.pages);
    assertEquals(64, represented(result, true));
    p.put("theta_page", "1.1");
    result = select(pages(four(), true), p, 0, 1023);
    assertEquals(4, result.corrections);
    assertEquals(64, represented(result, false));
  }

  @Test
  public void shortQueryUsesStoredLabelsForSubtractionWithoutMutatingPages() throws Exception {
    List<Reader> readers = four();
    List<int[]> before = new ArrayList<>();
    for (Reader r : readers) before.add(r.statistics.edCounts.clone());
    Map<String, String> p = params("chunk");
    p.put("theta_chunk", ".99");
    p.put("theta_page", ".99");
    OverlapStatistics.Selection result = select(pages(readers, true), p, 83, 907);
    assertEquals(50, represented(result, false));
    assertEquals(50, represented(result, true));
    for (int i = 0; i < readers.size(); i++)
      assertArrayEquals(before.get(i), readers.get(i).statistics.edCounts);
  }

  @Test
  public void sparseChunkDoesNotInventDensePageBoundaryContribution() throws Exception {
    Reader first = new Reader(range(0, 200)), second = new Reader(range(200, 400));
    List<Reader> pair = pair(400, 256, 64);
    ChunkMetadata a = chunk(Arrays.asList(first, second, pair.get(0))),
        b = chunk(Collections.singletonList(pair.get(1)));
    List<OverlapStatistics.Page> pages = new ArrayList<>();
    for (Reader r : Arrays.asList(first, second, pair.get(0)))
      pages.add(new OverlapStatistics.Page(r.statistics, r, a));
    pages.add(new OverlapStatistics.Page(pair.get(1).statistics, pair.get(1), b));
    OverlapStatistics.Selection result = select(pages, params("chunk"), 0, 911);
    assertEquals(57, represented(result, false));
    assertEquals(57, represented(result, true));
    assertEquals(5, result.sequences.size());
  }

  @Test
  public void sparsePayloadSurvivesExistingSerializationLayout() throws Exception {
    Statistics original = four().get(0).statistics;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    original.serialize(out);
    Statistics restored =
        Statistics.deserialize(ByteBuffer.wrap(out.toByteArray()), TSDataType.DOUBLE);
    assertArrayEquals(original.idx, restored.idx);
    assertArrayEquals(original.edIdx, restored.edIdx);
    assertArrayEquals(original.edCounts, restored.edCounts);
    assertEquals(original.getCount(), restored.getCount());
    assertEquals(original.getSerializedSize(), out.size());
  }

  @Test
  public void sparseMedoidCandidatesContainOnlyRealConsecutiveWindows() {
    Reader reader = four().get(1);
    List<double[]> candidates = NonEmptyMedoidCandidates.sequences(reader.raw, L);
    assertEquals(14, candidates.size());
    List<CompleteSubsequences.Window> windows = CompleteSubsequences.read(reader.raw, L);
    for (int i = 0; i < candidates.size(); i++)
      assertArrayEquals(windows.get(i).values, candidates.get(i), 0);
  }

  @Test
  public void nonoverlappingPageSelectionIsNotReplaced() throws Exception {
    Reader a = new Reader(range(0, 256)), b = new Reader(range(256, 512));
    assertNull(
        select(
            Arrays.asList(
                new OverlapStatistics.Page(a.statistics, a, null),
                new OverlapStatistics.Page(b.statistics, b, null)),
            params("page"),
            0,
            511));
    assertEquals(0, a.decoded);
    assertEquals(0, b.decoded);
    QueryDataSource data = new QueryDataSource(Collections.emptyList(), Collections.emptyList());
    assertNull(
        OverlapStatistics.select(
            null,
            null,
            null,
            null,
            data,
            params("chunk"),
            0,
            511,
            L,
            (raw, stored, start, end, next) -> stored));
  }

  @Test
  public void obsoleteSparsePayloadFailsRatherThanRunningLocalClustering() throws Exception {
    List<Reader> readers = four();
    readers.get(0).statistics.idx[999] = -1;
    try {
      select(pages(readers, false), params("page"), 0, 1023);
      fail();
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("obsolete"));
    }
  }

  @Test
  public void missingObservationIsSkippedInsteadOfZeroPadded() throws Exception {
    List<Reader> readers = four();
    List<Integer> times = new ArrayList<>();
    for (int i = 0; i < readers.get(0).raw.length(); i++) {
      int t = (int) readers.get(0).raw.getTimeByIndex(i);
      if (t != 225) times.add(t);
    }
    readers.set(0, new Reader(times));
    OverlapStatistics.Selection result = select(pages(readers, false), params("page"), 0, 1023);
    assertEquals(1, result.incompleteWindows);
    assertEquals(63, represented(result, false));
    assertEquals(63, represented(result, true));
  }

  @Test
  public void duplicateObservationIsRejectedInsteadOfClipped() throws Exception {
    List<Reader> readers = four();
    List<Integer> times = new ArrayList<>();
    for (int i = 0; i < readers.get(0).raw.length(); i++)
      times.add((int) readers.get(0).raw.getTimeByIndex(i));
    times.add(224);
    Collections.sort(times);
    readers.set(0, new Reader(times));
    try {
      select(pages(readers, false), params("page"), 0, 1023);
      fail();
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("Duplicate timestamp"));
    }
  }
}
