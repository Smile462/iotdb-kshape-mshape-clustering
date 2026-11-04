package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.query.reader.chunk.MemChunkLoader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Repair only samples too small to produce k medoids; ordinary samples stay untouched. */
public final class NonEmptyMedoidCandidates {
  private NonEmptyMedoidCandidates() {}

  interface PageSource {
    List<List<double[]>> read() throws IOException;
  }

  static void repair(List<double[]> sample, int k, PageSource source) throws IOException {
    if (k <= 0) throw new IOException("Medoid cluster count must be positive");
    if (sample.size() >= k) return;
    int originalSize = sample.size();
    List<List<double[]>> pages = source.read();
    Collections.shuffle(pages);
    List<double[]> rescued = new ArrayList<>();
    for (List<double[]> page : pages) {
      rescued.addAll(page);
      if (rescued.size() >= k) break;
    }
    if (rescued.size() < k) {
      throw new IOException(
          "Medoid sample repair: only "
              + rescued.size()
              + " eligible page subsequences; cannot produce "
              + k
              + " centers");
    }
    sample.clear();
    sample.addAll(rescued);
    System.out.println(
        "MEDOID_SAMPLE_RESCUE original="
            + originalSize
            + " repaired="
            + rescued.size()
            + " k="
            + k);
  }

  // Deliberately retain the reference implementation's whole-page subsequence bounds.
  static List<double[]> sequences(BatchData page, int length) {
    List<double[]> result = new ArrayList<>();
    for (int i = (int) Math.ceil(page.getMinTimestamp() * 1.0 / length);
        i < Math.floor(page.getMaxTimestamp() * 1.0 / length);
        i++) {
      double[] seq = new double[length];
      for (int j = 0; j < length; j++)
        seq[j] = page.getDoubleByIndex((int) (i * (long) length + j - page.getMinTimestamp()));
      result.add(seq);
    }
    return result;
  }

  public static void ensure(
      List<double[]> sample, List<ChunkMetadata> queryChunks, int length, int k)
      throws IOException {
    repair(
        sample,
        k,
        () -> {
          List<List<double[]>> pages = new ArrayList<>();
          for (ChunkMetadata metadata : queryChunks) {
            IChunkLoader loader = metadata.getChunkLoader();
            IChunkReader reader;
            if (loader instanceof MemChunkLoader) {
              reader = ((MemChunkLoader) loader).getChunkReader(metadata, null);
            } else {
              Chunk chunk = loader.loadChunk(metadata);
              chunk.setFromOldFile(metadata.isFromOldTsFile());
              reader = new ChunkReader(chunk, null);
            }
            try {
              while (reader.hasNextSatisfiedPage()) {
                List<double[]> seqs = sequences(reader.nextPageData(), length);
                if (!seqs.isEmpty()) pages.add(seqs);
              }
            } finally {
              reader.close();
            }
          }
          return pages;
        });
  }
}
