package com.galaxy.neptune.flink.launch;

import com.alibaba.fastjson.JSONObject;
import com.galaxy.neptune.flink.State.OutPutTag;
import com.galaxy.neptune.flink.config.FlinkParamsConstants;
import com.galaxy.neptune.flink.enums.SourceModelEnum;
import com.galaxy.neptune.flink.function.SeriesFlatMapFunction;
import com.galaxy.neptune.flink.source.CustomKafkaSource;
import com.galaxy.neptune.flink.source.generator.CQDorisGenerator;
import com.galaxy.neptune.flink.utils.FlinkEnv;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
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
        env.getConfig().setGlobalJobParameters(parameterTool);
        DataStreamSource<Tuple2<String, String>> dataStreamSource = getDorisDataStream(env, SourceModelEnum.GENERATOR).setParallelism(2);
        SingleOutputStreamOperator<String> outputStreamOperator = dataStreamSource.process(new ProcessFunction<Tuple2<String, String>, String>() {
            @Override
            public void processElement(Tuple2<String, String> source, ProcessFunction<Tuple2<String, String>, String>.Context context, Collector<String> collector) throws Exception {
                String record = source.f1;
                JSONObject jsonObject = JSONObject.parseObject(record);
//                String apply = jsonObject.getString("apply");
//                context.output(OutPutTag.APPLY_OUTPUT_TAG, apply);
//                String report = jsonObject.getString("report");
//                context.output(OutPutTag.REPORT_OUTPUT_TAG, report);
//                String study = jsonObject.getString("study");
//                context.output(OutPutTag.STUDY_OUTPUT_TAG, study);
                String series = jsonObject.getString("series");
                context.output(OutPutTag.SERIES_OUTPUT_TAG, series);
            }
        });
        /*outputStreamOperator.getSideOutput(OutPutTag.APPLY_OUTPUT_TAG).keyBy((KeySelector<String, String>) record -> {
            JSONObject jsonObject = JSONObject.parseObject(record);
            String ris_study_id = jsonObject.getString("ris_study_id");
            String system_id = jsonObject.getString("system_id");
            String organ_code = jsonObject.getString("organ_code");
            return organ_code.concat(system_id).concat(ris_study_id);
        }).process(new KeyedProcessFunction<String, String, String>() {
            private  ValueState<String> lastDateState;
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastDateDescriptor= new ValueStateDescriptor<>("apply_value_state", String.class);
                lastDateState = getRuntimeContext().getState(lastDateDescriptor);
                LOG.info("apply类型状态后端,state值{}",lastDateState.value());
            }
            @Override
            public void processElement(String record, KeyedProcessFunction<String, String, String>.Context context, Collector<String> collector) throws Exception {
                String currentKey = context.getCurrentKey();
                LOG.info("apply类型processElement中接受到的key:{},state值{}",currentKey,LocalDateTime.now());
                String stateDateTime = lastDateState.value();
                if(stateDateTime!=null){
                    LOG.info("当前key {}存在了这个时间:{}",currentKey,stateDateTime);
                }
                lastDateState.update(DateUtil.Day());
                collector.collect(record);
            }
        }).sinkTo(getApplyDorisSink());

        outputStreamOperator.getSideOutput(OutPutTag.REPORT_OUTPUT_TAG).keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String record) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(record);
                String ris_study_id = jsonObject.getString("ris_study_id");
                String system_id = jsonObject.getString("system_id");
                String organ_code = jsonObject.getString("organ_code");
                String key=organ_code.concat(system_id).concat(ris_study_id);
                return key;
            }
        }).process(new KeyedProcessFunction<String, String, String>() {
            private transient ValueState<String> lastDateState;
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastDateDescriptor= new ValueStateDescriptor<>("apply_value_state", String.class);
                lastDateState = getRuntimeContext().getState(lastDateDescriptor);
                LOG.info("report类型状态后端,state值{}",lastDateState.value());
            }
            @Override
            public void processElement(String record, KeyedProcessFunction<String, String, String>.Context context, Collector<String> collector) throws Exception {
                String currentKey = context.getCurrentKey();
                LOG.info("report类型processElement中接受到的key:{}",currentKey);
                String stateDateTime = lastDateState.value();
                if(stateDateTime!=null){
                    LOG.info("当前key {}存在了这个时间:{}",currentKey,stateDateTime);
                }
                lastDateState.update(DateUtil.Day());
                collector.collect(record);
            }
        }).sinkTo(getReportDorisSink());

        outputStreamOperator.getSideOutput(OutPutTag.STUDY_OUTPUT_TAG).keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String record) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(record);
                String ris_study_id = jsonObject.getString("ris_study_id");
                String system_id = jsonObject.getString("system_id");
                String organ_code = jsonObject.getString("organ_code");
                String key=organ_code.concat(system_id).concat(ris_study_id);
                return key;
            }
        }).process(new KeyedProcessFunction<String, String, String>() {
            private transient ValueState<String> lastDateState;
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastDateDescriptor= new ValueStateDescriptor<>("apply_value_state", String.class);
                lastDateState = getRuntimeContext().getState(lastDateDescriptor);
                LOG.info("study类型状态后端,state值{}",lastDateState.value());

            }
            @Override
            public void processElement(String record, KeyedProcessFunction<String, String, String>.Context context, Collector<String> collector) throws Exception {
                String currentKey = context.getCurrentKey();
                LOG.info("study类型processElement中接受到的key:{}",currentKey);
                String stateDateTime = lastDateState.value();
                if(stateDateTime!=null){
                    LOG.info("当前key {}存在了这个时间:{}",currentKey,stateDateTime);
                }
                lastDateState.update(DateUtil.Day());
                collector.collect(record);
            }
        }).sinkTo(getStudyDorisSink());*/

        outputStreamOperator.getSideOutput(OutPutTag.SERIES_OUTPUT_TAG).keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String record) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(record);
                String ris_study_id = jsonObject.getString("ris_study_id");
                String system_id = jsonObject.getString("system_id");
                String organ_code = jsonObject.getString("organ_code");
                String key=organ_code.concat(system_id).concat(ris_study_id);
                return key;
            }
        })
                .process(new SeriesFlatMapFunction()).setParallelism(2).name("seriesFlatMap")
                .sinkTo(getSeriesDorisSink()).name("sinkToDoris").setParallelism(1);

        env.execute("FlinkKs3LaunchEngine");
    }

    public static DataStreamSource<Tuple2<String, String>> getDorisDataStream(StreamExecutionEnvironment env, SourceModelEnum sourceModelEnum){
        ParameterTool parameters  = (ParameterTool) env.getConfig().getGlobalJobParameters();
        if(sourceModelEnum.equals(SourceModelEnum.GENERATOR)) {
            return env.addSource(new CQDorisGenerator(2000L));
        }
        return env.fromSource(CustomKafkaSource.dataSourceStream(parameters), WatermarkStrategy.noWatermarks(), "Kafka_Source");
    }



    private static DorisSink<String> getReportDorisSink(){
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes("10.128.23.64:8030")
                .setTableIdentifier("cq_cloud.ods_film_report_info")
                .setUsername("root")
                .setPassword("123456").build();

        DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label-prefix"+ UUID.randomUUID())
                .setStreamLoadProp(props)
                // TODO: 2023/8/25 设置删除为true后，输入json数据必须指定__DORIS_DELETE_SIGN__字段
                .setDeletable(false);

        DorisSink.Builder<String> builder = DorisSink.builder();
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setDorisOptions(dorisOptions)
                .setSerializer(new SimpleStringSerializer());
        return builder.build();
    }

    private static DorisSink<String> getApplyDorisSink(){
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes("10.128.23.64:8030")
                .setTableIdentifier("cq_cloud.ods_film_apply_info")
                .setUsername("root")
                .setPassword("123456").build();

        DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label-prefix"+ UUID.randomUUID())
                .setStreamLoadProp(props)
                // TODO: 2023/8/25 设置删除为true后，输入json数据必须指定__DORIS_DELETE_SIGN__字段
                .setDeletable(false);

        DorisSink.Builder<String> builder = DorisSink.builder();
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setDorisOptions(dorisOptions)
                .setSerializer(new SimpleStringSerializer());
        return builder.build();
    }

    private static DorisSink<String> getStudyDorisSink(){
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes("10.128.23.64:8030")
                .setTableIdentifier("cq_cloud.ods_film_study_info")
                .setUsername("root")
                .setPassword("123456").build();

        DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label-prefix"+ UUID.randomUUID())
                .setStreamLoadProp(props)
                // TODO: 2023/8/25 设置删除为true后，输入json数据必须指定__DORIS_DELETE_SIGN__字段
                .setDeletable(false);

        DorisSink.Builder<String> builder = DorisSink.builder();
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setDorisOptions(dorisOptions)
                .setSerializer(new SimpleStringSerializer());
        return builder.build();
    }

    private static DorisSink<String> getSeriesDorisSink(){
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes("10.128.23.64:8030")
                .setTableIdentifier("cq_cloud.ods_film_series_info")
                .setUsername("root")
                .setPassword("123456").build();

        DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label-prefix"+ UUID.randomUUID())
                .setStreamLoadProp(props)
                // TODO: 2023/8/25 设置删除为true后，输入json数据必须指定__DORIS_DELETE_SIGN__字段
                .setDeletable(false);

        DorisReadOptions.Builder dorisReadBuilder = DorisReadOptions.builder();


        DorisSink.Builder<String> builder = DorisSink.builder();
        builder.setDorisReadOptions(dorisReadBuilder.build())
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
