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
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.*;
import org.apache.iotdb.tsfile.read.common.*;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;

import org.junit.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

import static org.junit.Assert.*;

public class OverlapStatisticsTest {
  static final int L = 16;

  @Before
  public void configure() {
    TSFileDescriptor.getInstance().getConfig().setClusterNum(3);
    TSFileDescriptor.getInstance().getConfig().setSeqLength(L);
  }

  static final class Reader implements IPageReader {
    final Statistics statistics;
    final BatchData raw;

    Reader(int start, int end, double offset) {
      statistics = new DoubleStatistics();
      raw = new BatchData(TSDataType.DOUBLE);
      for (int t = start; t <= end; t++) {
        double v = PageMCorrectionTest.value(t) + offset;
        statistics.update(t, v);
        raw.putDouble(t, v);
      }
      raw.flip();
      statistics.updateStatistics();
    }

    public BatchData getAllSatisfiedPageData(boolean ascending) {
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

  static OverlapStatistics.Corrector corrector(boolean medoid) throws Exception {
    Class<?> type = medoid ? KshapeMExecutor.class : KshapeExecutor.class;
    AggregationPlan plan = new AggregationPlan();
    plan.setParameters(Collections.singletonList(Collections.emptyMap()));
    Object executor = medoid ? new KshapeMExecutor(null, plan) : new KshapeExecutor(null, plan);
    for (String name : Arrays.asList("k", "l")) {
      java.lang.reflect.Field field = type.getDeclaredField(name);
      field.setAccessible(true);
      field.setInt(executor, name.equals("k") ? 3 : L);
    }
    Method kernel =
        type.getDeclaredMethod(
            "updateByIter",
            IBatchDataIterator.class,
            Statistics.class,
            long.class,
            long.class,
            long.class);
    kernel.setAccessible(true);
    return (raw, stored, a, b, next) -> {
      raw.resetBatchData();
      try {
        return (Statistics) kernel.invoke(executor, raw.getBatchDataIterator(), stored, a, b, next);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    };
  }

  static List<OverlapStatistics.Page> pages(int count, int width, int stride) {
    List<OverlapStatistics.Page> result = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Reader reader = new Reader(i * stride, i * stride + width - 1, 0);
      result.add(new OverlapStatistics.Page(reader.statistics, reader, null));
    }
    return result;
  }

  static Map<String, String> parameters(String level) {
    Map<String, String> p = new HashMap<>();
    p.put("level", level);
    p.put("theta_page", "1.0");
    p.put("theta_chunk", "0.8");
    return p;
  }

  static long weight(List<Statistics> units, boolean medoid) {
    long n = 0;
    for (Statistics s : units) {
      if (medoid)
        for (int count : s.edCounts) {
          assertTrue(count >= 0);
          n += count;
        }
      else for (int label : s.idx) if (label >= 0) n++;
    }
    for (int i = 1; i < units.size(); i++)
      if (JournalSelection.boundarySequence(units, i, L) != null) n++;
    return n;
  }

  @Test
  public void ordinarySequenceGateDoesNotReadOrChangeMetadata() throws Exception {
    QueryDataSource source = new QueryDataSource(Collections.emptyList(), Collections.emptyList());
    assertNull(
        OverlapStatistics.select(
            null, null, null, null, source, parameters("page"), 0, 999, L, corrector(true)));
  }

  @Test
  public void overlapsReuseMStatisticsWithNoFitAndNoDuplicateWeight() throws Exception {
    List<OverlapStatistics.Page> pages = pages(8, 320, 239);
    List<int[]> original = new ArrayList<>();
    for (OverlapStatistics.Page p : pages) original.add(p.statistics.edCounts.clone());
    PrintStream old = System.out;
    ByteArrayOutputStream log = new ByteArrayOutputStream();
    OverlapStatistics.Selection selection;
    try {
      System.setOut(new PrintStream(log, true, "UTF-8"));
      selection =
          OverlapStatistics.selectPages(pages, parameters("chunk"), 0, 1992, L, corrector(true));
    } finally {
      System.setOut(old);
    }
    assertNotNull(selection);
    assertEquals(7, selection.corrections);
    assertEquals(0, selection.chunks);
    assertFalse(log.toString("UTF-8").contains("JOURNAL_BUILD_PAGE_NS"));
    assertEquals((7 * 239 + 320) / L, weight(selection.units, true));
    for (int i = 0; i < pages.size(); i++)
      assertArrayEquals(original.get(i), pages.get(i).statistics.edCounts);
  }

  @Test
  public void kAndMUseTheSameNextStartOwnershipAndKeepOriginalPayloads() throws Exception {
    for (boolean medoid : new boolean[] {false, true}) {
      List<OverlapStatistics.Page> pages = pages(5, 320, 240);
      OverlapStatistics.Selection s =
          OverlapStatistics.selectPages(pages, parameters("page"), 0, 1279, L, corrector(medoid));
      assertNotNull(s);
      assertEquals(5, s.units.size());
      for (int i = 0; i < 4; i++) {
        assertEquals(i * 240, s.units.get(i).getStartTime());
        assertEquals((i + 1) * 240 - 1, s.units.get(i).getEndTime());
      }
      assertEquals(80, weight(s.units, medoid));
    }
  }

  @Test
  public void multipleShortFragmentsReconstructEachBoundaryOnlyOnce() throws Exception {
    List<OverlapStatistics.Page> pages = pages(20, 320, 7);
    OverlapStatistics.Selection s =
        OverlapStatistics.selectPages(pages, parameters("page"), 0, 452, L, corrector(true));
    assertNotNull(s);
    assertEquals(28, weight(s.units, true));
    for (int i = 1; i < s.units.size(); i++)
      assertTrue(s.units.get(i - 1).getEndTime() < s.units.get(i).getStartTime());
  }

  @Test
  public void clippedQueryDoesNotRestoreDataOutsideItsGrid() throws Exception {
    List<OverlapStatistics.Page> pages = pages(8, 320, 239);
    OverlapStatistics.Selection s =
        OverlapStatistics.selectPages(pages, parameters("page"), 35, 988, L, corrector(true));
    assertNotNull(s);
    assertEquals((976 - 48) / L, weight(s.units, true));
    assertEquals(35, s.units.get(0).getStartTime());
    assertEquals(988, s.units.get(s.units.size() - 1).getEndTime());
  }

  @Test
  public void changedDuplicateValuesFallBackInsteadOfSubtractingWrongVersion() throws Exception {
    List<OverlapStatistics.Page> pages = pages(2, 320, 240);
    Reader changed = new Reader(240, 559, 1);
    pages.set(1, new OverlapStatistics.Page(changed.statistics, changed, null));
    assertNull(
        OverlapStatistics.selectPages(pages, parameters("page"), 0, 559, L, corrector(true)));
  }

  @Test
  public void noOverlapKeepsTheExistingPagePath() throws Exception {
    assertNull(
        OverlapStatistics.selectPages(
            pages(3, 320, 320), parameters("page"), 0, 959, L, corrector(true)));
  }

  @Test
  public void legacyBinaryNextStartSubtractionMatchesRetainedKPayload() throws Exception {
    String file = System.getProperty("legacy.overlap.jar");
    Assume.assumeTrue("Set legacy.overlap.jar to verify the first rerun binary", file != null);
    final String name = "org.apache.iotdb.db.query.executor.KshapeExecutor";
    try (java.net.URLClassLoader loader =
        new java.net.URLClassLoader(
            new java.net.URL[] {new File(file).toURI().toURL()}, getClass().getClassLoader()) {
          @Override
          protected Class<?> loadClass(String n, boolean resolve) throws ClassNotFoundException {
            if (!n.equals(name)) return super.loadClass(n, resolve);
            Class<?> type = findLoadedClass(n);
            if (type == null) type = findClass(n);
            if (resolve) resolveClass(type);
            return type;
          }
        }) {
      Class<?> oldType = loader.loadClass(name);
      java.lang.reflect.Constructor<?> ctor =
          oldType.getDeclaredConstructor(
              org.apache.iotdb.db.query.context.QueryContext.class, AggregationPlan.class);
      ctor.setAccessible(true);
      AggregationPlan plan = new AggregationPlan();
      plan.setParameters(Collections.singletonList(Collections.emptyMap()));
      Object legacy = ctor.newInstance(null, plan);
      for (String fieldName : Arrays.asList("k", "l")) {
        java.lang.reflect.Field field = oldType.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setInt(legacy, fieldName.equals("k") ? 3 : L);
      }
      Method kernel =
          oldType.getDeclaredMethod(
              "updateByIter",
              IBatchDataIterator.class,
              Statistics.class,
              long.class,
              long.class,
              long.class);
      kernel.setAccessible(true);
      Reader page = new Reader(0, 639, 0);
      for (long next : new long[] {160, 320, 480}) {
        page.raw.resetBatchData();
        Statistics expected =
            (Statistics)
                kernel.invoke(
                    legacy,
                    page.raw.getBatchDataIterator(),
                    page.statistics.clone(),
                    0L,
                    639L,
                    next);
        OverlapStatistics.Page physical = new OverlapStatistics.Page(page.statistics, page, null);
        physical.raw = page.raw;
        Statistics actual = OverlapStatistics.subtract(physical, 0, next - 1, L, corrector(false));
        assertArrayEquals(expected.idx, actual.idx);
        for (int c = 0; c < 3; c++) {
          if (MathUtils.clusterMemberNum(actual.idx, c) == 0) continue;
          for (int row = 0; row < L; row++)
            assertArrayEquals(expected.sumMatrices[c][row], actual.sumMatrices[c][row], 0);
          assertArrayEquals(expected.centroids[c], actual.centroids[c], 0);
        }
      }
    }
  }
}
