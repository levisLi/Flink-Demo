package com.galaxy.neptune.flink.function;

import com.galaxy.neptune.flink.FlinkDorisDemo;
import com.galaxy.neptune.flink.utils.DateUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.Snapshotable;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO
 *
 * @author lile
 * @description
 **/
public class SeriesFlatMapFunction extends KeyedProcessFunction<String, String, String> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(SeriesFlatMapFunction.class);

    private static final MapStateDescriptor<String,String> lastDateDescriptor;

    static {
        StateTtlConfig stateTtlConfig = StateTtlConfig
                //这个是用来配置生存的过期时间
                .newBuilder(Time.days(1))
                //配置什么时候对状态TTL进行刷新：OnCreateAndWrite-仅在创建和写访问时，OnReadAndWrite-有读取访问时
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                //配置状态可见性用于配置是否清除尚未过期的默认值（
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        lastDateDescriptor=new MapStateDescriptor<String,String>("series_value_state", String.class,String.class);
        lastDateDescriptor.enableTimeToLive(stateTtlConfig);
    }

    private MapState<String,String> lastDateState;

//    private HashMap<String,String> lastDateValue=new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        lastDateState=getRuntimeContext().getMapState(lastDateDescriptor);
    }

    @Override
    public void processElement(String record, KeyedProcessFunction<String, String, String>.Context context, Collector<String> collector) throws Exception {
        String currentKey = context.getCurrentKey();
//        LOG.info("series类型processElement中接受到的key:{}",currentKey);
        if(lastDateState.contains(currentKey)){
            LOG.info("当前key {}存在了这个时间:{}",currentKey,lastDateState.get(currentKey));
        }else {
            lastDateState.put(currentKey,DateUtil.Day());
        }
        collector.collect(record);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        LOG.info("===snapshotState=========>");
//        long start = System.currentTimeMillis();
//        lastDateState.clear();
//        long mid = System.currentTimeMillis();
//        lastDateState.putAll(lastDateValue);
//        long end = System.currentTimeMillis();
//        System.out.println("clear耗时{"+(mid-start)+"}snapshotState时间{}"+(end-start));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        lastDateState=context.getKeyedStateStore().getMapState(lastDateDescriptor);
        if(context.isRestored()){
            System.out.println("==========>恢复期启动");
//            long start = System.currentTimeMillis();
//            for (Map.Entry<String, String> entry : lastDateState.entries()) {
////                LOG.info("===恢复期状态后端key{}====value{}=====>",entry.getKey(),entry.getValue());
//                lastDateValue.put(entry.getKey(),entry.getValue());
//            }
//            long end = System.currentTimeMillis();
//            System.out.println("initializeState时间{}"+(end-start)/1000);
        }
    }
}
