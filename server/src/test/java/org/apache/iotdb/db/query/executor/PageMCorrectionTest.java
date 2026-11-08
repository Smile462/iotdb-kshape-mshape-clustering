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

import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.*;
import org.apache.iotdb.tsfile.read.common.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

public final class PageMCorrectionTest {
  private final int originalClusters = TSFileDescriptor.getInstance().getConfig().getClusterNum();
  private final int originalLength = TSFileDescriptor.getInstance().getConfig().getSeqLength();

  @org.junit.After
  public void restoreConfiguration() {
    TSFileDescriptor.getInstance().getConfig().setClusterNum(originalClusters);
    TSFileDescriptor.getInstance().getConfig().setSeqLength(originalLength);
  }

  static final int LENGTH = 16;

  static double value(long time) {
    return Math.sin(2 * Math.PI * time / LENGTH) + .02 * Math.cos(time);
  }

  static BatchData page(int start, int end) {
    BatchData batch = new BatchData(TSDataType.DOUBLE);
    for (int time = start; time <= end; time++) batch.putDouble(time, value(time));
    return batch.flip();
  }

  static void equal(double[] expected, double[] actual) {
    if (!Arrays.equals(expected, actual))
      throw new AssertionError("Page correction numerical drift");
  }

  static void oracle(Statistics stored, Statistics corrected, int start, int end) {
    int[] counts = stored.edCounts.clone();
    double[][] sums = new double[counts.length][LENGTH];
    for (int cluster = 0; cluster < counts.length; cluster++)
      for (int j = 0; j < LENGTH; j++)
        sums[cluster][j] = stored.edCentroids[cluster][j] * counts[cluster];
    for (int sequence = 0; sequence < 40; sequence++) {
      int first = sequence * LENGTH;
      if (first + LENGTH - 1 >= start && first <= end) continue;
      int cluster = stored.edIdx[sequence];
      double[] values = new double[LENGTH];
      for (int j = 0; j < LENGTH; j++) values[j] = value(first + j);
      values = MathUtils._zscore(values);
      counts[cluster]--;
      for (int j = 0; j < LENGTH; j++) sums[cluster][j] -= values[j];
    }
    if (!Arrays.equals(counts, corrected.edCounts))
      throw new AssertionError("Wrong M cluster counts");
    for (int cluster = 0; cluster < counts.length; cluster++)
      for (int j = 0; j < LENGTH; j++) {
        double expected = counts[cluster] == 0 ? 0 : sums[cluster][j] / counts[cluster];
        if (!Double.isFinite(corrected.edCentroids[cluster][j])
            || Math.abs(expected - corrected.edCentroids[cluster][j]) > 1e-10)
          throw new AssertionError(
              "Subtraction does not match original M labels and normalization");
      }
  }

  @org.junit.Test
  public void testSharedCorrection() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setClusterNum(3);
    TSFileDescriptor.getInstance().getConfig().setSeqLength(LENGTH);
    AggregationPlan plan = new AggregationPlan();
    plan.setParameters(Collections.singletonList(Collections.emptyMap()));
    KshapeMExecutor executor = new KshapeMExecutor(null, plan);
    Field length = KshapeMExecutor.class.getDeclaredField("l");
    length.setAccessible(true);
    length.setInt(executor, LENGTH);
    Method correction =
        KshapeMExecutor.class.getDeclaredMethod(
            "updateByIter",
            IBatchDataIterator.class,
            Statistics.class,
            long.class,
            long.class,
            long.class);
    correction.setAccessible(true);
    Statistics stored = new DoubleStatistics();
    for (int time = 0; time < 640; time++) stored.update(time, value(time));
    stored.updateStatistics();
    // Select an interval that only removes complete subsequences from the
    // Page. Both invocations must receive the same persisted statistics.
    for (int[] query : new int[][] {{16, 639}, {0, 623}, {17, 622}, {48, 127}, {48, 63}}) {
      int start = query[0], end = query[1];
      Statistics untouched = stored.clone();
      Statistics expected =
          (Statistics)
              correction.invoke(
                  executor,
                  page(0, 639).getBatchDataIterator(),
                  stored,
                  (long) start,
                  (long) end,
                  -1L);
      oracle(stored, expected, start, end);
      Statistics changedK = stored.clone();
      for (double[] center : changedK.centroids) Arrays.fill(center, 123456.0);
      Statistics independent =
          (Statistics)
              correction.invoke(
                  executor,
                  page(0, 639).getBatchDataIterator(),
                  changedK,
                  (long) start,
                  (long) end,
                  -1L);
      if (!Arrays.equals(expected.edCounts, independent.edCounts))
        throw new AssertionError("M uses K cluster identities");
      for (int c = 0; c < expected.edCentroids.length; c++)
        equal(expected.edCentroids[c], independent.edCentroids[c]);
      JournalSelection selector =
          new JournalSelection(Collections.emptyMap(), start, end, LENGTH, 3);
      List<Statistics> units = new ArrayList<>();
      selector.addCorrectedPage(
          units,
          page(0, 639),
          stored,
          (batch, copy) -> {
            try {
              return (Statistics)
                  correction.invoke(
                      executor, batch.getBatchDataIterator(), copy, (long) start, (long) end, -1L);
            } catch (ReflectiveOperationException error) {
              throw new RuntimeException(error);
            }
          });
      if (units.size() != 1 || selector.correctedPages != 1 || selector.temp != 0)
        throw new AssertionError("Boundary did not use Page correction");
      Statistics actual = units.get(0);
      if (!Arrays.equals(expected.edCounts, actual.edCounts))
        throw new AssertionError("Counts changed");
      for (int c = 0; c < actual.edCentroids.length; c++)
        equal(expected.edCentroids[c], actual.edCentroids[c]);
      if (actual.getStartTime() != start
          || actual.getEndTime() != end
          || !actual.journalGridFragments)
        throw new AssertionError("Incorrect Chunk boundary layout");
      equal(untouched.tailExtraPoints, stored.tailExtraPoints);
      equal(untouched.headExtraPoints, stored.headExtraPoints);
      for (int c = 0; c < stored.edCentroids.length; c++)
        equal(untouched.edCentroids[c], stored.edCentroids[c]);
    }
    System.out.println(
        "PASS: Chunk-M shares original Page correction, preserves stored input and adapts fragments");
    double[] singleton = {1., 2., 3.};
    MathUtils.updateEdCentroid(singleton, 1, Arrays.asList(1., 2., 3.));
    equal(new double[3], singleton);
    double[] normal = {3., 5., 7.};
    MathUtils.updateEdCentroid(normal, 2, Arrays.asList(4., 6., 8.));
    equal(new double[] {2., 4., 6.}, normal);
    try {
      MathUtils.updateEdCentroid(singleton, 0, Arrays.asList(1., 2., 3.));
      throw new AssertionError("Removing from an empty cluster was accepted");
    } catch (IllegalStateException expected) {
      System.out.println(
          "PASS: singleton becomes empty, normal subtraction is unchanged, empty removal is rejected");
    }
  }
}
