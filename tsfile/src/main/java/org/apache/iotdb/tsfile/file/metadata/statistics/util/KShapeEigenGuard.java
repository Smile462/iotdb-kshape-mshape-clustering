package org.apache.iotdb.tsfile.file.metadata.statistics.util;

import org.apache.iotdb.tsfile.file.metadata.statistics.util.Jama.EigenvalueDecomposition;
import org.apache.iotdb.tsfile.file.metadata.statistics.util.Jama.Matrix;

import java.util.concurrent.CancellationException;

/** Retains the supplied solver; recovers only stalled, nearly symmetric K-Shape matrices. */
public final class KShapeEigenGuard {
  private static final int MAX_QR_ITERATIONS_WITHOUT_DEFLATION = 256;
  private static final ThreadLocal<Boolean> ACTIVE = new ThreadLocal<Boolean>();

  public static final class IterationLimit extends ArithmeticException {
    public IterationLimit() {
      super("K-Shape QR eigen iteration did not deflate after 256 iterations");
    }
  }

  private KShapeEigenGuard() {}

  public static void checkIterations(int iterations) {
    if (!Boolean.TRUE.equals(ACTIVE.get())) return;
    if (Thread.currentThread().isInterrupted())
      throw new CancellationException("K-Shape eigen computation interrupted");
    if (iterations > MAX_QR_ITERATIONS_WITHOUT_DEFLATION) throw new IterationLimit();
  }

  public static EigenvalueDecomposition decompose(Matrix matrix) {
    Boolean previous = ACTIVE.get();
    ACTIVE.set(Boolean.TRUE);
    try {
      return new EigenvalueDecomposition(matrix);
    } catch (IterationLimit stalled) {
      // Q*(X^T*X)*Q is symmetric in exact arithmetic. Refuse a genuinely
      // nonsymmetric/non-finite matrix instead of silently changing its problem.
      Matrix symmetric = symmetricRoundoffCopy(matrix);
      System.err.println(
          "TsFileKShapeEigenGuard: QR iteration limit reached; using symmetric solver for roundoff-only asymmetry; n="
              + matrix.getRowDimension());
      return new EigenvalueDecomposition(symmetric);
    } finally {
      if (previous == null) ACTIVE.remove();
      else ACTIVE.set(previous);
    }
  }

  public static Matrix symmetricRoundoffCopy(Matrix matrix) {
    int n = matrix.getRowDimension();
    if (n != matrix.getColumnDimension())
      throw new ArithmeticException("K-Shape eigen matrix is not square");
    double[][] data = matrix.getArray();
    double scale = 1.0, asymmetry = 0.0;
    for (int i = 0; i < n; i++)
      for (int j = 0; j < n; j++) {
        if (!Double.isFinite(data[i][j]))
          throw new ArithmeticException("Non-finite K-Shape eigen matrix");
        scale = Math.max(scale, Math.abs(data[i][j]));
        asymmetry = Math.max(asymmetry, Math.abs(data[i][j] - data[j][i]));
      }
    double tolerance = 64.0 * Math.max(1, n) * Math.ulp(scale);
    if (asymmetry > tolerance)
      throw new ArithmeticException(
          "K-Shape matrix is not symmetric to roundoff: " + asymmetry + " > " + tolerance);
    double[][] copy = matrix.getArrayCopy();
    for (int i = 0; i < n; i++)
      for (int j = 0; j < i; j++) {
        double mean = 0.5 * data[i][j] + 0.5 * data[j][i];
        copy[i][j] = mean;
        copy[j][i] = mean;
      }
    return new Matrix(copy);
  }
}
