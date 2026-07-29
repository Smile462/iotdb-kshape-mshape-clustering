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
package org.apache.iotdb.tsfile.file.metadata.statistics;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.filter.StatisticsClassException;
import org.apache.iotdb.tsfile.exception.write.UnknownColumnTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.util.Complex;
import org.apache.iotdb.tsfile.file.metadata.statistics.util.FFT;
import org.apache.iotdb.tsfile.file.metadata.statistics.util.KMeans;
import org.apache.iotdb.tsfile.file.metadata.statistics.util.KShape;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * This class is used for recording statistic information of each measurement in a delta file. While
 * writing processing, the processor records the statistics information. Statistics includes
 * maximum, minimum and null value count up to version 0.0.1.<br>
 * Each data type extends this Statistic as super class.<br>
 * <br>
 * For the statistics in the Unseq file TimeSeriesMetadata, only firstValue, lastValue, startTime
 * and endTime can be used.</br>
 */
public abstract class Statistics<T extends Serializable> {

  private static final TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();

  private static final Logger LOG = LoggerFactory.getLogger(Statistics.class);
  /**
   * isEmpty being false means this statistic has been initialized and the max and min is not null;
   */
  protected boolean isEmpty = true;

  public boolean isChunkStatistics = false;

  /** number of time-value points */
  private int count = 0;

  private long startTime = Long.MAX_VALUE;
  private long endTime = Long.MIN_VALUE;
  private List<Long> timeWindow = new ArrayList<>();
  private List<Double> valueWindow = new ArrayList<>();
  private static final int k = tsFileConfig.getClusterNum(); // cluster num
  private static final int l = tsFileConfig.getSeqLength(); // subsequence length
  private static final int edK = k + 2; // cluster num for euclidean
  public double[][][] sumMatrices = new double[k][l][l];
  public double[][] centroids = new double[k][l];
  public double[] deltas = new double[k];
  public int[] idx = new int[1000];
  public double[] headExtraPoints = new double[l];
  public double[] tailExtraPoints = new double[l];

  // pre-computed metadata for K-Shape-M
  public double[][] edCentroids = new double[edK][l];
  public double[] edDeltas = new double[edK];
  public int[] edCounts = new int[edK];
  public int[] edIdx = new int[1000];

  public List<double[]> chunkCentroidList = new ArrayList<>();

  static final String STATS_UNSUPPORTED_MSG = "%s statistics does not support: %s";

  /**
   * static method providing statistic instance for respective data type.
   *
   * @param type - data type
   * @return Statistics
   */
  public static Statistics<? extends Serializable> getStatsByType(TSDataType type) {
    switch (type) {
      case INT32:
        return new IntegerStatistics();
      case INT64:
        return new LongStatistics();
      case TEXT:
        return new BinaryStatistics();
      case BOOLEAN:
        return new BooleanStatistics();
      case DOUBLE:
        return new DoubleStatistics();
      case FLOAT:
        return new FloatStatistics();
      case VECTOR:
        return new TimeStatistics();
      default:
        throw new UnknownColumnTypeException(type.toString());
    }
  }

  public Statistics clone() {
    Statistics newInstance = new DoubleStatistics();
    newInstance.setCount((int) this.count);
    newInstance.setStartTime(this.startTime);
    newInstance.setEndTime(this.endTime);
    for (int i = 0; i < k; i++)
      for (int j = 0; j < l; j++)
        for (int p = 0; p < l; p++) newInstance.setSumMatrices(this.sumMatrices[i][j][p], i, j, p);
    for (int i = 0; i < k; i++)
      for (int j = 0; j < l; j++) newInstance.setCentroids(this.centroids[i][j], i, j);
    for (int i = 0; i < k; i++) newInstance.setDeltas(this.deltas[i], i);
    for (int i = 0; i < 1000; i++) newInstance.setIdx(this.idx[i], i);
    for (int i = 0; i < l; i++) newInstance.setHeadExtraPoints(this.headExtraPoints[i], i);
    for (int i = 0; i < l; i++) newInstance.setTailExtraPoints(this.tailExtraPoints[i], i);

    for (int i = 0; i < edK; i++)
      for (int j = 0; j < l; j++) newInstance.setEdCentroids(this.edCentroids[i][j], i, j);
    for (int i = 0; i < edK; i++) newInstance.setEdDelta(this.edDeltas[i], i);
    for (int i = 0; i < edK; i++) newInstance.setEdCounts(this.edCounts[i], i);
    for (int i = 0; i < 1000; i++) newInstance.setEdIdx(this.edIdx[i], i);
    return newInstance;
  }

  public static int getSizeByType(TSDataType type) {
    switch (type) {
      case INT32:
        return IntegerStatistics.INTEGER_STATISTICS_FIXED_RAM_SIZE;
      case INT64:
        return LongStatistics.LONG_STATISTICS_FIXED_RAM_SIZE;
      case TEXT:
        return BinaryStatistics.BINARY_STATISTICS_FIXED_RAM_SIZE;
      case BOOLEAN:
        return BooleanStatistics.BOOLEAN_STATISTICS_FIXED_RAM_SIZE;
      case DOUBLE:
        return DoubleStatistics.DOUBLE_STATISTICS_FIXED_RAM_SIZE;
      case FLOAT:
        return FloatStatistics.FLOAT_STATISTICS_FIXED_RAM_SIZE;
      case VECTOR:
        return TimeStatistics.TIME_STATISTICS_FIXED_RAM_SIZE;
      default:
        throw new UnknownColumnTypeException(type.toString());
    }
  }

  public abstract TSDataType getType();

  public int getSerializedSize() {
    return ReadWriteForEncodingUtils.uVarIntSize(count) // count
        + 16 // startTime, endTime
        + 8 * k * l * l // sumMatrices
        + 8 * k * l // centroids
        + 8 * k // deltas
        + 4 * 1000 // idx
        + 8 * edK * l // edCentroids
        + 8 * edK // edDeltas
        + 4 * edK // edCounts
        + 4 * 1000 // edIdx
        + 8 * l * 2 // headExtraPoints, tailExtraPoints
        + getStatsSize();
  }

  public abstract int getStatsSize();

  public int serialize(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    if (!isChunkStatistics) {
      updateStatistics();
    }
    System.out.println("========================");
    System.out.println("k=" + k + ", l=" + l);
    System.out.println("Start time = " + startTime + ", end time = " + endTime);
    System.out.println("========================");

    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(count, outputStream);
    byteLen += ReadWriteIOUtils.write(startTime, outputStream);
    byteLen += ReadWriteIOUtils.write(endTime, outputStream);

    for (int i = 0; i < k; i++) {
      double[][] _matrix = sumMatrices[i];
      for (int j = 0; j < l; j++)
        for (int p = 0; p < l; p++) byteLen += ReadWriteIOUtils.write(_matrix[j][p], outputStream);
    }
    for (int i = 0; i < k; i++)
      for (int j = 0; j < l; j++) byteLen += ReadWriteIOUtils.write(centroids[i][j], outputStream);
    for (int i = 0; i < k; i++) byteLen += ReadWriteIOUtils.write(deltas[i], outputStream);
    for (int i = 0; i < 1000; i++) byteLen += ReadWriteIOUtils.write(idx[i], outputStream);

    // metadata for K-Shape-M
    for (int i = 0; i < edK; i++)
      for (int j = 0; j < l; j++)
        byteLen += ReadWriteIOUtils.write(edCentroids[i][j], outputStream);
    for (int i = 0; i < edK; i++) byteLen += ReadWriteIOUtils.write(edDeltas[i], outputStream);
    for (int i = 0; i < edK; i++) byteLen += ReadWriteIOUtils.write(edCounts[i], outputStream);
    for (int i = 0; i < 1000; i++) byteLen += ReadWriteIOUtils.write(edIdx[i], outputStream);

    for (int i = 0; i < l; i++) byteLen += ReadWriteIOUtils.write(headExtraPoints[i], outputStream);
    for (int i = 0; i < l; i++) byteLen += ReadWriteIOUtils.write(tailExtraPoints[i], outputStream);

    // value statistics of different data type
    byteLen += serializeStats(outputStream);
    return byteLen;
  }

  abstract int serializeStats(OutputStream outputStream) throws IOException;

  /** read data from the inputStream. */
  public abstract void deserialize(InputStream inputStream) throws IOException;

  public abstract void deserialize(ByteBuffer byteBuffer);

  public abstract T getMinValue();

  public abstract T getMaxValue();

  public abstract T getFirstValue();

  public abstract T getLastValue();

  public abstract double getSumDoubleValue();

  public abstract long getSumLongValue();

  /**
   * merge parameter to this statistic
   *
   * @throws StatisticsClassException cannot merge statistics
   */
  @SuppressWarnings("unchecked")
  public void mergeStatistics(Statistics<? extends Serializable> stats) {
    if (this.getClass() == stats.getClass()) {
      if (!stats.isEmpty) {
        this.timeWindow = stats.timeWindow;
        this.valueWindow = stats.valueWindow;

        this.sumMatrices = stats.sumMatrices;
        this.centroids = stats.centroids;
        this.deltas = stats.deltas;
        this.idx = stats.idx;

        this.edCentroids = stats.edCentroids;
        this.edDeltas = stats.edDeltas;
        this.edCounts = stats.edCounts;
        this.edIdx = stats.edIdx;

        this.headExtraPoints = stats.headExtraPoints;
        this.tailExtraPoints = stats.tailExtraPoints;

        if (stats.startTime < this.startTime) {
          this.startTime = stats.startTime;
        }
        if (stats.endTime > this.endTime) {
          this.endTime = stats.endTime;
        }

        // must be sure no overlap between two statistics
        this.count += stats.count;
        mergeStatisticsValue((Statistics<T>) stats);
        isEmpty = false;
      }
    } else {
      Class<?> thisClass = this.getClass();
      Class<?> statsClass = stats.getClass();
      LOG.warn("Statistics classes mismatched,no merge: {} v.s. {}", thisClass, statsClass);

      throw new StatisticsClassException(thisClass, statsClass);
    }
  }

  public void update(long time, boolean value) {
    update(time);
    updateStats(value);
  }

  public void update(long time, int value) {
    update(time);
    updateStats(value);
  }

  public void update(long time, long value) {
    update(time);
    updateStats(value);
  }

  public void update(long time, float value) {
    update(time);
    updateStats(value);
  }

  public void update(long time, double value) {
    update(time);
    updateStats(value);
    this.timeWindow.add(time);
    this.valueWindow.add(value);
  }

  public void update(long time, Binary value) {
    update(time);
    updateStats(value);
  }

  public void update(long time) {
    if (time < startTime) {
      startTime = time;
    }
    if (time > endTime) {
      endTime = time;
    }
    count++;
  }

  public void update(long[] time, boolean[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, int[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, long[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, float[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, double[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, Binary[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, int batchSize) {
    if (time[0] < startTime) {
      startTime = time[0];
    }
    if (time[batchSize - 1] > this.endTime) {
      endTime = time[batchSize - 1];
    }
    count += batchSize;
  }

  protected abstract void mergeStatisticsValue(Statistics<T> stats);

  public boolean isEmpty() {
    return isEmpty;
  }

  public void setEmpty(boolean empty) {
    isEmpty = empty;
  }

  void updateStats(boolean value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(int value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(long value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(float value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(double value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(Binary value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(boolean[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(int[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(long[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(float[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(double[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(Binary[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  /**
   * This method with two parameters is only used by {@code unsequence} which
   * updates/inserts/deletes timestamp.
   *
   * @param min min timestamp
   * @param max max timestamp
   */
  public void updateStats(long min, long max) {
    throw new UnsupportedOperationException();
  }

  public static Statistics<? extends Serializable> deserialize(
      InputStream inputStream, TSDataType dataType) throws IOException {
    Statistics<? extends Serializable> statistics = getStatsByType(dataType);
    statistics.setCount(ReadWriteForEncodingUtils.readUnsignedVarInt(inputStream));
    statistics.setStartTime(ReadWriteIOUtils.readLong(inputStream));
    statistics.setEndTime(ReadWriteIOUtils.readLong(inputStream));
    // metadata for K-Shape
    for (int i = 0; i < k; i++)
      for (int j = 0; j < l; j++)
        for (int p = 0; p < l; p++)
          statistics.setSumMatrices(ReadWriteIOUtils.readDouble(inputStream), i, j, p);
    for (int i = 0; i < k; i++)
      for (int j = 0; j < l; j++)
        statistics.setCentroids(ReadWriteIOUtils.readDouble(inputStream), i, j);
    for (int i = 0; i < k; i++) statistics.setDeltas(ReadWriteIOUtils.readDouble(inputStream), i);
    for (int i = 0; i < 1000; i++) statistics.setIdx(ReadWriteIOUtils.readInt(inputStream), i);

    // metadata for K-Shape-M
    for (int i = 0; i < edK; i++)
      for (int j = 0; j < l; j++)
        statistics.setEdCentroids(ReadWriteIOUtils.readDouble(inputStream), i, j);
    for (int i = 0; i < edK; i++)
      statistics.setEdDelta(ReadWriteIOUtils.readDouble(inputStream), i);
    for (int i = 0; i < edK; i++) statistics.setEdCounts(ReadWriteIOUtils.readInt(inputStream), i);
    for (int i = 0; i < 1000; i++) statistics.setEdIdx(ReadWriteIOUtils.readInt(inputStream), i);

    for (int i = 0; i < l; i++)
      statistics.setHeadExtraPoints(ReadWriteIOUtils.readDouble(inputStream), i);
    for (int i = 0; i < l; i++)
      statistics.setTailExtraPoints(ReadWriteIOUtils.readDouble(inputStream), i);
    statistics.deserialize(inputStream);
    statistics.isEmpty = false;
    return statistics;
  }

  public static Statistics<? extends Serializable> deserialize(
      ByteBuffer buffer, TSDataType dataType) {
    Statistics<? extends Serializable> statistics = getStatsByType(dataType);
    statistics.setCount(ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
    statistics.setStartTime(ReadWriteIOUtils.readLong(buffer));
    statistics.setEndTime(ReadWriteIOUtils.readLong(buffer));
    // metadata for K-Shape
    for (int i = 0; i < k; i++)
      for (int j = 0; j < l; j++)
        for (int p = 0; p < l; p++)
          statistics.setSumMatrices(ReadWriteIOUtils.readDouble(buffer), i, j, p);
    for (int i = 0; i < k; i++)
      for (int j = 0; j < l; j++)
        statistics.setCentroids(ReadWriteIOUtils.readDouble(buffer), i, j);
    for (int i = 0; i < k; i++) statistics.setDeltas(ReadWriteIOUtils.readDouble(buffer), i);
    for (int i = 0; i < 1000; i++) statistics.setIdx(ReadWriteIOUtils.readInt(buffer), i);

    // metadata for K-Shape-M
    for (int i = 0; i < edK; i++)
      for (int j = 0; j < l; j++)
        statistics.setEdCentroids(ReadWriteIOUtils.readDouble(buffer), i, j);
    for (int i = 0; i < edK; i++) statistics.setEdDelta(ReadWriteIOUtils.readDouble(buffer), i);
    for (int i = 0; i < edK; i++) statistics.setEdCounts(ReadWriteIOUtils.readInt(buffer), i);
    for (int i = 0; i < 1000; i++) statistics.setEdIdx(ReadWriteIOUtils.readInt(buffer), i);

    for (int i = 0; i < l; i++)
      statistics.setHeadExtraPoints(ReadWriteIOUtils.readDouble(buffer), i);
    for (int i = 0; i < l; i++)
      statistics.setTailExtraPoints(ReadWriteIOUtils.readDouble(buffer), i);

    statistics.deserialize(buffer);
    statistics.isEmpty = false;
    return statistics;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public long getCount() {
    return count;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public void setSumMatrices(double v, int i, int j, int p) {
    this.sumMatrices[i][j][p] = v;
  }

  public void setCentroids(double v, int i, int j) {
    this.centroids[i][j] = v;
  }

  public void setDeltas(double v, int i) {
    this.deltas[i] = v;
  }

  public void setIdx(int v, int i) {
    this.idx[i] = v;
  }

  public void setHeadExtraPoints(double v, int i) {
    this.headExtraPoints[i] = v;
  }

  public void setTailExtraPoints(double v, int i) {
    this.tailExtraPoints[i] = v;
  }

  public void setEdCentroids(double v, int i, int j) {
    this.edCentroids[i][j] = v;
  }

  public void setEdDelta(double v, int i) {
    this.edDeltas[i] = v;
  }

  public void setEdIdx(int v, int i) {
    this.edIdx[i] = v;
  }

  public void setEdCounts(int v, int i) {
    this.edCounts[i] = v;
  }

  public abstract long calculateRamSize();

  @Override
  public String toString() {
    return "startTime: " + startTime + " endTime: " + endTime + " count: " + count;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    return o != null && getClass() == o.getClass();
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), count, startTime, endTime);
  }

  public void updateStatistics() {
    if (this.timeWindow.size() < l) {
      return;
    }
    int n = this.timeWindow.size(); // all points

    long seqStartTime = (long) (Math.ceil(this.startTime * 1.0 / l) * l);
    long seqEndTime = (long) (Math.floor(this.endTime * 1.0 / l) * l);
    Arrays.fill(this.idx, -1);

    List<List<Double>> tmpSeqs = new ArrayList<>();
    List<Double> tmpSeq = new ArrayList<>();
    int _i = 0;
    while (_i < n) {
      if (this.timeWindow.get(_i) < seqStartTime) {
        this.headExtraPoints[_i] = this.valueWindow.get(_i);
        _i++;
        continue;
      }
      tmpSeq.add(this.valueWindow.get(_i));
      if (tmpSeq.size() == l) {
        tmpSeqs.add(tmpSeq);
        tmpSeq = new ArrayList<>();
      }
      _i++;
    }
    if (tmpSeq.size() > 0)
      for (int i = 0; i < tmpSeq.size(); i++)
        this.tailExtraPoints[l - tmpSeq.size() + i] = tmpSeq.get(i);

    double[][] X = new double[tmpSeqs.size()][l];
    for (int i = 0; i < tmpSeqs.size(); i++) {
      X[i] = tmpSeqs.get(i).stream().mapToDouble(Double::doubleValue).toArray();
      X[i] = normalize(X[i]);
    }

    int seqNum = X.length;
    int l = X[0].length;

    // pre-compute metadata for K-Shape
    KShape kshape = new KShape(k, 30);
    kshape.fit(X);
    for (int i = 0; i < k; i++)
      this.sumMatrices[i] = kshape.getSumMatrices()[i].getArray(); // k * l * l
    this.centroids = kshape.getCentroids(); // k * l
    this.deltas = kshape.getDeltas(); // k
    for (int i = 0; i < kshape.getIdx().length; i++) this.idx[i] = kshape.getIdx()[i];

    // pre-compute metadata for K-Shape-M
    Arrays.fill(this.edIdx, -1);
    Arrays.fill(this.edCounts, 0);
    KMeans clustering = new KMeans.Builder(edK, X).iterations(30).ifPlusInitial(true).build();
    this.edCentroids = clustering.getCentroids();
    this.edDeltas = clustering.getDeltas();
    for (int i = 0; i < clustering.getIdx().length; i++) this.edIdx[i] = clustering.getIdx()[i];
    for (int idx : this.edIdx) {
      if (idx == -1) break;
      this.edCounts[idx]++;
    }
  }

  private long calTimeInterval() {
    int count = 0;
    long maxFreqInterval = timeWindow.get(1) - timeWindow.get(0);
    for (int i = 2; i < timeWindow.size(); i++) {
      if (maxFreqInterval == timeWindow.get(i) - timeWindow.get(i - 1)) count++;
      else {
        count--;
        if (count == 0) {
          maxFreqInterval = timeWindow.get(i) - timeWindow.get(i - 1);
          count++;
        }
      }
    }
    return maxFreqInterval;
  }

  private double[] normalize(double[] x) {
    double sum = 0.0;
    for (double v : x) sum += v;
    double mean = sum / x.length;
    double std = 0.0;
    for (double v : x) std += (v - mean) * (v - mean);
    std = Math.sqrt(std / (x.length - 1));
    double[] res = new double[x.length];
    for (int i = 0; i < x.length; i++) res[i] = (x[i] - mean) / std;
    return res;
  }

  /**
   * 为 Chunk 级别生成精确的元数据。 该方法在 Chunk 刷盘时被调用，输入是该 Chunk 内所有 Page 的 Statistics 列表。 它对这些 Page 元数据执行一次完整的
   * KShape 聚类合并，得到 Chunk 级别的精确元数据。
   */
  public void updateChunkStatistics(List<Statistics> pageStatisticsList) {
    if (pageStatisticsList == null || pageStatisticsList.isEmpty()) {
      return;
    }

    // 合并时间范围和计数
    for (Statistics pageStat : pageStatisticsList) {
      if (pageStat.getStartTime() < this.startTime) {
        this.startTime = pageStat.getStartTime();
      }
      if (pageStat.getEndTime() > this.endTime) {
        this.endTime = pageStat.getEndTime();
      }
      this.count += pageStat.getCount();
    }

    // 执行与 executePageKshape 完全相同的合并逻辑
    double[][][] sumMatrices = pageStatisticsList.get(0).sumMatrices.clone();
    double[][] centroids = pageStatisticsList.get(0).centroids.clone();
    double[] deltas = pageStatisticsList.get(0).deltas.clone();
    int[] counts = new int[k];
    for (int i = 0; i < k; i++) {
      counts[i] = clusterMemberNum(pageStatisticsList.get(0).idx, i);
    }
    for (int i = 1; i < pageStatisticsList.size(); i++) {
      Statistics curStat = pageStatisticsList.get(i);

      for (int j = 0; j < curStat.centroids.length; j++) {
        int nearestIdx = findNearestCentroid(curStat.centroids[j], centroids);
        if (nearestIdx != -1) {
          for (int u = 0; u < l; ++u) {
            for (int v = 0; v < l; ++v) {
              sumMatrices[nearestIdx][u][v] += curStat.sumMatrices[j][u][v];
            }
          }
          int curCnt = clusterMemberNum(curStat.idx, j);
          for (int u = 0; u < l; ++u) {
            centroids[nearestIdx][u] =
                (centroids[nearestIdx][u] * counts[nearestIdx] + curStat.centroids[j][u] * curCnt)
                    / (counts[nearestIdx] + curCnt);
          }
          counts[nearestIdx] += curCnt;
        }
      }

      // 处理跨 Page 边界的补充点
      double[] curHeadExtraPoints = curStat.headExtraPoints;
      boolean complementaryFlag = false;
      for (double v : curHeadExtraPoints) {
        if (v != 0) {
          complementaryFlag = true;
          break;
        }
      }

      if (complementaryFlag) {
        double[] merged = new double[l];
        for (int j = 0; j < l; j++) {
          merged[j] = curHeadExtraPoints[j] + pageStatisticsList.get(i - 1).tailExtraPoints[j];
        }
        merged = zscore(merged);
        int nearestIdx = findNearestCentroid(merged, centroids);
        if (nearestIdx != -1) {
          for (int u = 0; u < l; ++u) {
            for (int v = 0; v < l; ++v) {
              sumMatrices[nearestIdx][u][v] += merged[u] * merged[v];
            }
          }
          counts[nearestIdx] += 1;
        }
      }
    }

    // 提取形状并保存到 this 的字段中
    for (int i = 0; i < k; i++) {
      if (counts[i] > 0) {
        for (int j = 0; j < l; j++) {
          this.centroids[i][j] = centroids[i][j];
        }
      }
    }
    this.sumMatrices = sumMatrices;
    this.deltas = deltas;

    // 同时填充 chunkCentroidList
    this.chunkCentroidList.clear();
    for (int i = 0; i < k; i++) {
      this.chunkCentroidList.add(this.centroids[i].clone());
    }

    this.headExtraPoints = pageStatisticsList.get(0).headExtraPoints.clone();
    this.tailExtraPoints =
        pageStatisticsList.get(pageStatisticsList.size() - 1).tailExtraPoints.clone();

    this.isChunkStatistics = true;
    this.isEmpty = false;
  }

  // Mshape更新方法
  public void updateChunkMStatistics(List<Statistics> pageStatisticsList) {
    if (pageStatisticsList == null || pageStatisticsList.isEmpty()) {
      return;
    }

    // 合并时间范围和计数
    for (Statistics pageStat : pageStatisticsList) {
      if (pageStat.getStartTime() < this.startTime) this.startTime = pageStat.getStartTime();
      if (pageStat.getEndTime() > this.endTime) this.endTime = pageStat.getEndTime();
      this.count += pageStat.getCount();
    }

    // 取第一个 Page 的 MShape 元数据作为初始值
    double[][] edCentroids = pageStatisticsList.get(0).edCentroids.clone();
    int[] edCounts = pageStatisticsList.get(0).edCounts.clone();

    // 逐个合并剩余 Page
    for (int i = 1; i < pageStatisticsList.size(); i++) {
      Statistics curStat = pageStatisticsList.get(i);

      for (int j = 0; j < curStat.edCentroids.length; j++) {
        if (curStat.edCounts[j] == 0) continue;
        int nearestIdx = findNearestCentroid(curStat.edCentroids[j], edCentroids);
        if (nearestIdx != -1) {
          // 加权平均合并 edCentroids
          for (int u = 0; u < l; u++) {
            edCentroids[nearestIdx][u] =
                (edCentroids[nearestIdx][u] * edCounts[nearestIdx]
                        + curStat.edCentroids[j][u] * curStat.edCounts[j])
                    / (edCounts[nearestIdx] + curStat.edCounts[j]);
          }
          // 累加成员数
          edCounts[nearestIdx] += curStat.edCounts[j];
        }
      }

      // 处理跨 Page 边界的补充点
      double[] curHeadExtraPoints = curStat.headExtraPoints;
      boolean complementaryFlag = false;
      for (double v : curHeadExtraPoints) {
        if (v != 0) {
          complementaryFlag = true;
          break;
        }
      }
      if (complementaryFlag) {
        double[] merged = new double[l];
        for (int j = 0; j < l; j++) {
          merged[j] = curHeadExtraPoints[j] + pageStatisticsList.get(i - 1).tailExtraPoints[j];
        }
        merged = zscore(merged);
        int nearestIdx = findNearestCentroid(merged, edCentroids);
        if (nearestIdx != -1) {
          for (int u = 0; u < l; u++) {
            edCentroids[nearestIdx][u] =
                (edCentroids[nearestIdx][u] * edCounts[nearestIdx] + merged[u])
                    / (edCounts[nearestIdx] + 1);
          }
          edCounts[nearestIdx] += 1;
        }
      }
    }

    // 保存到 this
    this.edCentroids = edCentroids;
    this.edCounts = edCounts;

    // 保存边界点
    this.headExtraPoints = pageStatisticsList.get(0).headExtraPoints.clone();
    this.tailExtraPoints =
        pageStatisticsList.get(pageStatisticsList.size() - 1).tailExtraPoints.clone();

    this.isChunkStatistics = true;
  }

  // 辅助方法
  private int findNearestCentroid(double[] centroid, double[][] centroids) {
    int idx = -1;
    double minDis = Double.MAX_VALUE;
    for (int i = 0; i < centroids.length; i++) {
      double dis = sbd(centroid, centroids[i]); // 使用 SBD 距离
      if (dis < minDis) {
        minDis = dis;
        idx = i;
      }
    }
    return idx;
  }

  /** 统计 idx 数组中值为 cluster 的成员数量。 */
  private int clusterMemberNum(int[] idx, int cluster) {
    int count = 0;
    for (int i : idx) {
      if (i == cluster) {
        count++;
      }
    }
    return count;
  }

  /** Z-Score 标准化 */
  private double[] zscore(double[] x) {
    double sum = 0.0;
    for (double v : x) sum += v;
    double mean = sum / x.length;
    double std = 0.0;
    for (double v : x) std += (v - mean) * (v - mean);
    std = Math.sqrt(std / (x.length - 1));
    double[] res = new double[x.length];
    for (int i = 0; i < x.length; i++) res[i] = (x[i] - mean) / std;
    return res;
  }

  private double sbd(double[] x, double[] y) {
    double maxv = Double.MIN_VALUE;
    for (double v : ncc(x, y)) {
      if (v > maxv) maxv = v;
    }
    return 1 - maxv / norm(x) / norm(y);
  }

  private double[] ncc(double[] x, double[] y) {
    double den = norm(x) * norm(y);
    if (den < 1e-9) den = Double.MAX_VALUE;
    int x_len = x.length;
    int fft_size = (int) Math.pow(2, Integer.toBinaryString(2 * x_len - 1).length());
    double[] cc =
        FFT.ifft(Complex.multiply(FFT.fft(x, fft_size), Complex.conjugate(FFT.fft(y, fft_size))));
    double[] ncc = new double[fft_size - 1];
    for (int i = 0; i < fft_size - 1; i++)
      if (i < x_len - 1) ncc[i] = cc[cc.length - x_len + 1 + i] / den;
      else ncc[i] = cc[i - x_len + 1] / den;
    return ncc;
  }

  private double norm(double[] x) {
    double res = 0.0;
    for (double v : x) res += v * v;
    return Math.sqrt(res);
  }
}
