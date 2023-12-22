package com.galaxy.neptune.flink.utils;

import com.galaxy.neptune.flink.config.FlinkParamsConstants;
import com.galaxy.neptune.flink.enums.FlinkModelEnum;
import com.galaxy.neptune.flink.enums.SourceModelEnum;
import com.galaxy.neptune.flink.source.CustomKafkaSource;
import com.galaxy.neptune.flink.source.generator.SourceGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.rocksdb.RocksDB;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class FlinkEnv {

    /**
     * @description: 获取flink执行环境上下文
     * @param parameterTool main方法的参数
     * @return: org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
     * @author: lile
     */
    public static StreamExecutionEnvironment getStreamExecutionEnvironment(ParameterTool parameterTool){
        checkFlinkArgs(parameterTool);
        String flinkRunModel = parameterTool.get(FlinkParamsConstants.RUN.MODEL, FlinkModelEnum.PROD.name()).toUpperCase();
        StreamExecutionEnvironment env;
        if(FlinkModelEnum.valueOf(flinkRunModel).equals(FlinkModelEnum.PROD)){
            env=buildProdEnvironment(parameterTool);
        }else {
            env=buildLocalEnvironment();
        }
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                1,//重启尝试次数
                // 重启之间的延时时间
                Time.of(5, TimeUnit.SECONDS)));
        env.getConfig().setGlobalJobParameters(parameterTool);
        return env;
    }

    /**
     * @description: 获取flink输入数据源
     * @param env flink streamExecutionEnvironment 上下文
     * @param sourceModelEnum source来源枚举值 GENERATOR：本地模拟 KAFKA：kafka数据源
     * @return: org.apache.flink.streaming.api.datastream.DataStreamSource<org.apache.flink.api.java.tuple.Tuple3<java.lang.String,java.lang.String,java.lang.String>>
     * @author: lile
     */
    public static DataStreamSource<Tuple2<String, String>> getSourceDataStream(StreamExecutionEnvironment env, SourceModelEnum sourceModelEnum){
        ParameterTool parameters  = (ParameterTool) env.getConfig().getGlobalJobParameters();
        if(sourceModelEnum.equals(SourceModelEnum.GENERATOR)) {
            return env.addSource(new SourceGenerator(1L));
        }
        return env.fromSource(CustomKafkaSource.dataSourceStream(parameters), WatermarkStrategy.noWatermarks(), "Kafka_Source");
    }

    private static StreamExecutionEnvironment buildProdEnvironment(ParameterTool parameterTool){
        //1.获取执行环境
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString("classloader.check-leaked-classloader","false");
//        flinkConfig.setString(SavepointConfigOptions.SAVEPOINT_PATH, "");
//        flinkConfig.setString(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID.key(),"fd72014d4c864993a2e5a9287b4a9c5d");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        env.enableCheckpointing(parameterTool.getLong(FlinkParamsConstants.CHECKPOINT.INTERVAL,60*1000L));
        EmbeddedRocksDBStateBackend embededRocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        embededRocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        env.setStateBackend(embededRocksDBStateBackend);
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(parameterTool.get(FlinkParamsConstants.CHECKPOINT.PATH, "hdfs://namespace-HA-1/flink/checkpoints/flink-demo")));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.disableOperatorChaining();
        return env;
    }

    private static StreamExecutionEnvironment buildLocalEnvironment(){
        //1.获取执行环境
        Configuration flinkConfig = new Configuration();
        flinkConfig.setInteger(RestOptions.PORT.key(), 8081);
        flinkConfig.setString(SavepointConfigOptions.SAVEPOINT_PATH, "file:///tmp/flink/checkpoint/fd72014d4c864993a2e5a9287b4a9c5d/chk-1/_metadata");
        flinkConfig.setString(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID.key(),"fd72014d4c864993a2e5a9287b4a9c5d");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
        env.enableCheckpointing(60*1000L);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink/checkpoint");
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointTimeout(120*1000L);
        env.setParallelism(1);
        env.disableOperatorChaining();
        return env;
    }

    private static void checkFlinkArgs(ParameterTool parameters){
        if(!parameters.has(FlinkParamsConstants.RUN.MODEL)){
            throw new RuntimeException("flink运行模式缺失请添加参数：--flink.run.model ,参数值local(本地模式)/prod(生产模式)");
        }
    }



}
