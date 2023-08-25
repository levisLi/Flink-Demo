package com.galaxy.neptune.flink.source.generator;

public class Throttler {

    private long throttleBatchSize;

    private long nanosPerBatch;

    private long endOfNextBatchNanos;

    private int currentBatch;

    Throttler(long maxRecordsPerSecond, int numberOfParallelSubtasks) {
        if (maxRecordsPerSecond == -1) {
            throttleBatchSize = -1;
            nanosPerBatch = 0;
            endOfNextBatchNanos = System.nanoTime() + nanosPerBatch;
            currentBatch = 0;
            return;
        }
        final float ratePerSubtask = (float) maxRecordsPerSecond / numberOfParallelSubtasks;
        if (maxRecordsPerSecond > 10000) {
            throttleBatchSize = (int) ratePerSubtask / 500;
            nanosPerBatch = 2_000_000L;
        } else {
            throttleBatchSize = ((int) (ratePerSubtask / 20)) + 1;
            nanosPerBatch = ((int) (1_000_000_000L / ratePerSubtask)) * throttleBatchSize;
        }
        this.endOfNextBatchNanos = System.nanoTime() + nanosPerBatch;
        this.currentBatch = 0;
    }

    void throttle() throws InterruptedException {
        if (throttleBatchSize == -1) {
            return;
        }
        if (++currentBatch != throttleBatchSize) {
            return;
        }
        currentBatch = 0;

        final long now = System.nanoTime();
        final int millisRemaining = (int) ((endOfNextBatchNanos - now) / 1_000_000);

        if (millisRemaining > 0) {
            endOfNextBatchNanos += nanosPerBatch;
            Thread.sleep(millisRemaining);
        } else {
            endOfNextBatchNanos = now + nanosPerBatch;
        }
    }

}
