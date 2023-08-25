package com.galaxy.neptune.flink;

import com.galaxy.neptune.flink.config.FlinkParamsConstants;
import com.galaxy.neptune.flink.enums.SourceModelEnum;
import com.galaxy.neptune.flink.source.CustomKafkaSource;
import com.galaxy.neptune.flink.source.generator.DorisGenerator;
import com.galaxy.neptune.flink.utils.FlinkEnv;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

/**
 * TODO
 *
 * @author lile
 * @description
 **/
public class FlinkDorisDemo {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkDorisDemo.class);

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = getParameterTool(args);
        StreamExecutionEnvironment env = FlinkEnv.getStreamExecutionEnvironment(parameterTool);
        env.enableCheckpointing(10*1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getConfig().setGlobalJobParameters(parameterTool);
        DataStreamSource<Tuple2<String, String>> dataStreamSource = getDorisDataStream(env, SourceModelEnum.GENERATOR);
        SingleOutputStreamOperator<String> streamOperator = dataStreamSource.rebalance().map(new RichMapFunction<Tuple2<String, String>, String>() {
            @Override
            public String map(Tuple2<String, String> record) throws Exception {
                // TODO: 2023/8/21 添加业务逻辑转化为jsonString的数据 
                return record.f1;
            }
        });
        streamOperator.print();
        streamOperator.sinkTo(getDorisSink());
        env.execute("FlinkKs3LaunchEngine");
    }

    public static DataStreamSource<Tuple2<String, String>> getDorisDataStream(StreamExecutionEnvironment env, SourceModelEnum sourceModelEnum){
        ParameterTool parameters  = (ParameterTool) env.getConfig().getGlobalJobParameters();
        if(sourceModelEnum.equals(SourceModelEnum.GENERATOR)) {
            return env.addSource(new DorisGenerator(1L));
        }
        return env.fromSource(CustomKafkaSource.dataSourceStream(parameters), WatermarkStrategy.noWatermarks(), "Kafka_Source");
    }

    private static DorisSink<String> getDorisSink(){
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes("10.128.23.64:8030")
                .setTableIdentifier("lile.dwd_screen_metrics_wrong_data_info_1")
                .setUsername("root")
                .setPassword("123456").build();

        DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label-prefix"+ UUID.randomUUID())
                .setStreamLoadProp(props)
                // TODO: 2023/8/25 设置删除为true后，输入json数据必须指定__DORIS_DELETE_SIGN__字段
                .setDeletable(true);

        DorisSink.Builder<String> builder = DorisSink.builder();
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setDorisOptions(dorisOptions)
                .setSerializer(new SimpleStringSerializer());
        return builder.build();
    }

    private static ParameterTool getParameterTool(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String profileFile = parameterTool.get(FlinkParamsConstants.RUN.PROFILES, "local") + "." + "properties";
        ParameterTool propertiesFile = ParameterTool.fromPropertiesFile(FlinkDorisDemo.class.getClassLoader().getResourceAsStream(profileFile));
        return propertiesFile.mergeWith(parameterTool);
    }
}
