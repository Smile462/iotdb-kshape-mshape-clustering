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

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class PageStatisticsReuseTest {
  private final int length = TSFileDescriptor.getInstance().getConfig().getSeqLength();
  private final int sequences =
      Math.max(8, TSFileDescriptor.getInstance().getConfig().getClusterNum() + 3);

  private double value(int time) {
    return Math.sin(time * 2 * Math.PI / length) + .07 * Math.cos(time * .193);
  }

  private void append(Statistics stats, int start, int end) {
    for (int i = start; i < end; i++) stats.update(i, value(i));
  }

  private byte[] serialize(Statistics stats) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    stats.serialize(out);
    return out.toByteArray();
  }

  private void payloadEquals(Statistics expected, Statistics actual) {
    assertArrayEquals(expected.idx, actual.idx);
    assertArrayEquals(expected.edIdx, actual.edIdx);
    assertArrayEquals(expected.edCounts, actual.edCounts);
    assertArrayEquals(expected.deltas, actual.deltas, 0);
    assertArrayEquals(expected.edDeltas, actual.edDeltas, 0);
    assertArrayEquals(expected.headExtraPoints, actual.headExtraPoints, 0);
    assertArrayEquals(expected.tailExtraPoints, actual.tailExtraPoints, 0);
    for (int c = 0; c < expected.centroids.length; c++) {
      assertArrayEquals(expected.centroids[c], actual.centroids[c], 0);
      for (int j = 0; j < length; j++)
        assertArrayEquals(expected.sumMatrices[c][j], actual.sumMatrices[c][j], 0);
    }
    for (int c = 0; c < expected.edCentroids.length; c++)
      assertArrayEquals(expected.edCentroids[c], actual.edCentroids[c], 0);
  }

  private int builds(ByteArrayOutputStream log) throws Exception {
    return log.toString("UTF-8").split("JOURNAL_BUILD_PAGE_NS=", -1).length - 1;
  }

  @Test
  public void reusesPayloadAcrossSerializationCloneAndBothReadPaths() throws Exception {
    PrintStream original = System.out;
    ByteArrayOutputStream log = new ByteArrayOutputStream();
    try {
      System.setOut(new PrintStream(log, true, "UTF-8"));
      Statistics stats = new DoubleStatistics();
      append(stats, 0, length * sequences);
      stats.updateStatistics();
      byte[] stored = serialize(stats);
      stats.updateStatistics();
      assertArrayEquals(stored, serialize(stats));
      Statistics copy = stats.clone();
      serialize(copy);
      payloadEquals(stats, copy);
      Statistics stream =
          Statistics.deserialize(new ByteArrayInputStream(stored), TSDataType.DOUBLE);
      Statistics buffer = Statistics.deserialize(ByteBuffer.wrap(stored), TSDataType.DOUBLE);
      assertArrayEquals(stored, serialize(stream));
      assertArrayEquals(stored, serialize(buffer));
      assertEquals(1, builds(log));
      // New points must invalidate the fitted payload.
      append(stats, length * sequences, length * (sequences + 1));
      stats.updateStatistics();
      serialize(stats);
      assertEquals(2, builds(log));
      assertEquals(sequences + 1, Arrays.stream(stats.edCounts).sum());
    } finally {
      System.setOut(original);
    }
  }

  @Test
  public void emptySubsequencePageIsCachedButNewPointsInvalidateIt() throws Exception {
    PrintStream original = System.out;
    ByteArrayOutputStream log = new ByteArrayOutputStream();
    try {
      System.setOut(new PrintStream(log, true, "UTF-8"));
      Statistics stats = new DoubleStatistics();
      append(stats, 0, length / 2);
      stats.updateStatistics();
      serialize(stats);
      stats.updateStatistics();
      assertEquals(1, builds(log));
      append(stats, length / 2, length * sequences);
      stats.updateStatistics();
      serialize(stats);
      assertEquals(2, builds(log));
      assertEquals(sequences, Arrays.stream(stats.edCounts).sum());
      assertArrayEquals(new double[length], stats.tailExtraPoints, 0);
    } finally {
      System.setOut(original);
    }
  }

  @Test
  public void eightSealedPagesBuildEightTimesAndPersistTheChunkInputs() throws Exception {
    PrintStream original = System.out;
    ByteArrayOutputStream log = new ByteArrayOutputStream();
    try {
      System.setOut(new PrintStream(log, true, "UTF-8"));
      ChunkWriterImpl writer =
          new ChunkWriterImpl(
              new MeasurementSchema(
                  "s0", TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
      int width = length * sequences;
      for (int page = 0; page < 8; page++) {
        for (int i = page * width; i < (page + 1) * width; i++) writer.write(i, value(i));
        writer.sealCurrentPage();
      }
      assertEquals(8, builds(log));
      Field snapshots = ChunkWriterImpl.class.getDeclaredField("pageStatisticsList");
      snapshots.setAccessible(true);
      List<Statistics> inputs = (List<Statistics>) snapshots.get(writer);
      Field bytes = ChunkWriterImpl.class.getDeclaredField("pageBuffer");
      bytes.setAccessible(true);
      ByteBuffer buffer = ByteBuffer.wrap(((PublicBAOS) bytes.get(writer)).toByteArray());
      assertEquals(8, inputs.size());
      for (int i = 0; i < 8; i++) {
        PageHeader header = PageHeader.deserializeFrom(buffer, TSDataType.DOUBLE);
        payloadEquals(inputs.get(i), header.getStatistics());
        buffer.position(buffer.position() + header.getCompressedSize());
      }
      assertFalse(buffer.hasRemaining());
      assertEquals(8, builds(log));
    } finally {
      System.setOut(original);
    }
  }
}
