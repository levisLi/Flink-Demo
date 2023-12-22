package com.galaxy.neptune.flink.trigger;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountAndTimeTrigger<W extends TimeWindow> extends Trigger<Object, W> {

    private static Logger logger = LoggerFactory.getLogger(CountAndTimeTrigger.class);

    public CountAndTimeTrigger(Long size,Long interval){
        this.size=size;
        this.interval=interval;
    }
    // 触发的条数
    private long size;

    // 触发的时长
    private  long interval;
    // 条数计数器
    private final ReducingStateDescriptor<Long> countStateDesc=new ReducingStateDescriptor<Long>("count", new ReduceSum(), LongSerializer.INSTANCE);
    // 时间计数器，保存下一次触发的时间
    private final ReducingStateDescriptor<Long> processTimeStateDesc = new ReducingStateDescriptor<>("fire-interval", new ReduceUpdate(), LongSerializer.INSTANCE);
    //记录时间时间定时器的状态
    private final ReducingStateDescriptor<Long>  eventTimerStateDescriptor = new ReducingStateDescriptor<Long>("eventTimer", new ReduceUpdate(),LongSerializer.INSTANCE);

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        logger.info("元素{}"+element.toString());
        /*数量状态*/
        ReducingState<Long> countState = ctx.getPartitionedState(countStateDesc);
        /*处理时间状态*/
        ReducingState<Long> processTimeState = ctx.getPartitionedState(processTimeStateDesc);
        /*事件时间状态*/
        ReducingState<Long> eventTimeState = ctx.getPartitionedState(eventTimerStateDescriptor);
        // 每条数据 counter + 1
        countState.add(1L);
        //如果没有设置事件时间定时器，需要设置一个窗口最大时间触发器，这个目的是为了在窗口清除的时候 利用事件时间触发计算，否则可能会缺少部分数据
        if (null==eventTimeState.get()||eventTimeState.get() == 0L) {
            eventTimeState.add(window.maxTimestamp());
            ctx.registerEventTimeTimer(window.maxTimestamp());
        }
        Long aLong = countState.get();
        if(aLong>size){
            logger.info("countTrigger triggered, count : {}"+ countState.get());
            // 满足条数的触发条件，先清 0 条数计数器
            countState.clear();
            // 满足条数时也需要清除时间的触发器，如果不是创建结束的触发器
            if (processTimeState.get() != window.maxTimestamp()) {
                ctx.deleteProcessingTimeTimer(processTimeState.get());
            }
            processTimeState.clear();
            // fire 触发计算
            return TriggerResult.FIRE;
        }else if (null==processTimeState.get()||processTimeState.get() == 0L) {
            //未达到指定数量，且没有指定定时器，需要指定定时器
            //当前定时器状态值加上间隔值
            processTimeState.add(ctx.getCurrentProcessingTime() + interval);
            //注册定执行时间定时器
            ctx.registerProcessingTimeTimer(processTimeState.get());
            return TriggerResult.CONTINUE;
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    // 执行时间定时器触发
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        ReducingState<Long> count = ctx.getPartitionedState(countStateDesc);
        ReducingState<Long> processTimestamp = ctx.getPartitionedState(processTimeStateDesc);
        // 时间达到最大窗口期了,触发计算
        if (time == window.maxTimestamp()) {
            logger.info("window close : {}"+ time);
            // 窗口结束，清0条数和时间的计数器
            count.clear();
            ctx.deleteProcessingTimeTimer(processTimestamp.get());
            processTimestamp.clear();
            return TriggerResult.FIRE_AND_PURGE;
        } else if (count.get()>0&&processTimestamp.get() != null && processTimestamp.get().equals(time)) {
            // 时间计数器触发，清理条数和时间计数器
            count.clear();
            processTimestamp.clear();
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    //事件时间定时器触发
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if ((time >= window.maxTimestamp()) && (ctx.getPartitionedState(countStateDesc).get() > 0L)) { //还有未触发计算的数据
            System.out.println("事件时间到达最大的窗口时间，并且窗口中还有未计算的数据:${ctx.getPartitionedState(countStateDescriptor).get()}，触发计算并清除窗口");
            ctx.getPartitionedState(eventTimerStateDescriptor).clear();
            return TriggerResult.FIRE_AND_PURGE;
        }else if ((time >= window.maxTimestamp()) && (ctx.getPartitionedState(countStateDesc).get() == 0L)) { //没有未触发计算的数据
            System.out.println("事件时间到达最大的窗口时间，但是窗口中没有有未计算的数据，清除窗口 但是不触发计算");
            return TriggerResult.PURGE;
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        logger.info("清除窗口状态，定时器");
        ctx.deleteEventTimeTimer(ctx.getPartitionedState(eventTimerStateDescriptor).get());
        ctx.deleteProcessingTimeTimer(ctx.getPartitionedState(processTimeStateDesc).get());
        ctx.getPartitionedState(processTimeStateDesc).clear();
        ctx.getPartitionedState(eventTimerStateDescriptor).clear();
        ctx.getPartitionedState(countStateDesc).clear();
    }
}

//更新状态为累加值
class ReduceSum implements ReduceFunction<Long> {
    @Override
    public Long reduce(Long value1, Long value2) throws Exception {
        return value1+value2;
    }
}

//更新状态为取新的值
class ReduceUpdate implements ReduceFunction<Long>{
    @Override
    public Long reduce(Long value1, Long value2) throws Exception {
        return value2;
    }
}
