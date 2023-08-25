package com.galaxy.neptune.flink.source.generator;

import com.galaxy.neptune.flink.State.StateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.SplittableRandom;

public abstract class BaseGenerator<T> extends RichParallelSourceFunction<T>
    implements CheckpointedFunction {

    private transient ListState<Long> idState;

    private volatile boolean running = true;

    protected Long maxRecordsPerSecond;

    private long id = -1;

    public BaseGenerator() {
        this.maxRecordsPerSecond = -1l;
    }

    public BaseGenerator(Long maxRecordsPerSecond) {
        this.maxRecordsPerSecond = maxRecordsPerSecond;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (id == -1) {
            id = getRuntimeContext().getIndexOfThisSubtask();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        idState.clear();
        idState.add(id);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        idState = context.getOperatorStateStore().getUnionListState(StateDescriptor.SOURCE_IDS_DESC);
        Long max = Long.MIN_VALUE;
        if (context.isRestored()) {
            for (Long id : idState.get()) {
                max = Math.max(max, id);
            }
        }
        id = max + getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        //当前线程任务taskId
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        //任务总task数
        final int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        //SplittableRandom:并行随机类;ThreadLocalRandom单线程随机类
        final SplittableRandom rnd = new SplittableRandom();

        final Throttler throttler = new Throttler(maxRecordsPerSecond, numberOfParallelSubtasks);

        Object checkpointLock = sourceContext.getCheckpointLock();
        while (running) {
            T event = invokeEvent(rnd, id);
//            log.error("当前taskId{}发送事件{}",indexOfThisSubtask, JSONObject.toJSONString(event));
            synchronized (checkpointLock) {
                if (event != null) {
                    sourceContext.collect(event);
                }
                id += numberOfParallelSubtasks;
            }
            // TODO: 2021/6/18 设置休眠时间
            throttler.throttle();
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    public abstract T invokeEvent(SplittableRandom rnd, Long id);

}
