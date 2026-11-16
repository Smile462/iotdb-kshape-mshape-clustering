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
package org.apache.iotdb.tsfile.file.metadata.statistics.util;

import org.apache.iotdb.tsfile.read.common.BatchData;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;

/** Complete windows on the original time grid; never compress holes into observations. */
public final class CompleteSubsequences {
  private CompleteSubsequences() {}

  public static final class Window {
    public final long start;
    public final double[] values;

    public Window(long start, double[] values) {
      this.start = start;
      this.values = values;
    }
  }

  public static List<Window> read(BatchData page, int length) {
    return read(page.length(), page::getTimeByIndex, page::getDoubleByIndex, length);
  }

  public static List<Window> read(List<Long> times, List<Double> values, int length) {
    return read(times.size(), times::get, values::get, length);
  }

  private static List<Window> read(
      int size, IntToLongFunction time, IntToDoubleFunction value, int length) {
    if (length < 2) throw new IllegalArgumentException("Subsequence length must be at least 2");
    List<Window> windows = new ArrayList<>();
    long previous = Long.MIN_VALUE, start = 0;
    double[] current = null;
    int count = 0;
    for (int i = 0; i < size; i++) {
      long t = time.applyAsLong(i);
      if (i > 0 && t <= previous)
        throw new IllegalArgumentException("Page timestamps must be unique and increasing");
      previous = t;
      double v = value.applyAsDouble(i);
      if (!Double.isFinite(v)) throw new IllegalArgumentException("Nonfinite Page observation");
      if (Math.floorMod(t, length) == 0) {
        start = t;
        count = 0;
        current = new double[length];
      }
      if (current == null) continue;
      if (t != start + count) {
        current = null;
        continue;
      }
      current[count++] = v;
      if (count == length) {
        windows.add(new Window(start, current));
        current = null;
      }
    }
    return windows;
  }
}
