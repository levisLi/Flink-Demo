package com.galaxy.neptune.flink.launch;

import com.alibaba.fastjson.JSONObject;
import com.galaxy.neptune.flink.config.FlinkParamsConstants;
import com.galaxy.neptune.flink.enums.SourceModelEnum;
import com.galaxy.neptune.flink.source.CustomKafkaSource;
import com.galaxy.neptune.flink.source.generator.JSDicomGenerator;
import com.galaxy.neptune.flink.utils.FlinkEnv;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * TODO
 *
 * @author lile
 * @description
 **/
public class DicomCloudData {

    private static final Logger LOG = LoggerFactory.getLogger(DicomCloudData.class);

    public static Properties kafkaProducerConfigure(){
        Properties properties=new Properties();
        //设置应答机制
        properties.put("acks","1");
        //批量提交大小
        properties.put("batch.size",16384);
        //延时提交
        properties.put("linger.ms",1000);
        //缓充大小
        properties.put("buffer.memory",33554432);
        return properties;
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = getParameterTool(args);
        LOG.info("==========>"+parameterTool);
        StreamExecutionEnvironment env = FlinkEnv.getStreamExecutionEnvironment(parameterTool);
        env.getConfig().setGlobalJobParameters(parameterTool);
        DataStreamSource<Tuple2<String, String>> dataStreamSource = env.addSource(new JSDicomGenerator(5L));
        SingleOutputStreamOperator<String> streamOperator = dataStreamSource.rebalance().flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public void flatMap(Tuple2<String, String> source, Collector<String> collector) throws Exception {
                String record = source.f1;
                JSONObject jsonObject = JSONObject.parseObject(record, JSONObject.class);
                String apply = jsonObject.getString("apply");
                collector.collect(apply);
                String report = jsonObject.getString("report");
                collector.collect(report);
                String study = jsonObject.getString("study");
                collector.collect(study);
                String series = jsonObject.getString("series");
                collector.collect(series);
            }
        });
        SingleOutputStreamOperator<String> ks3StreamOperator = dataStreamSource.rebalance().flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public void flatMap(Tuple2<String, String> source, Collector<String> collector) throws Exception {
                String record = source.f1;
                JSONObject jsonObject = JSONObject.parseObject(record, JSONObject.class);
                String ks3Record = jsonObject.getString("ks3");
                List<JSONObject> jsonObjects = JSONObject.parseArray(ks3Record, JSONObject.class);
                for (JSONObject object : jsonObjects) {
                    collector.collect(object.toString());
                }
            }
        });
        KafkaSink<String> dicomKafkaSink = KafkaSink.<String>builder().setBootstrapServers(parameterTool.get("kafka.dicom.bootstrapServers"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(parameterTool.get("kafka.dicom.topic"))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(kafkaProducerConfigure())
                .build();
        KafkaSink<String> ks3KafkaSink = KafkaSink.<String>builder().setBootstrapServers(parameterTool.get("kafka.dicom.bootstrapServers"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(parameterTool.get("kafka.ks3.topic"))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(kafkaProducerConfigure())
                .build();
        streamOperator.sinkTo(dicomKafkaSink);
        ks3StreamOperator.sinkTo(ks3KafkaSink);
        env.execute("dicomCloudData");
    }

    public static DataStreamSource<Tuple2<String, String>> getDorisDataStream(StreamExecutionEnvironment env, SourceModelEnum sourceModelEnum){
        ParameterTool parameters  = (ParameterTool) env.getConfig().getGlobalJobParameters();
        if(sourceModelEnum.equals(SourceModelEnum.GENERATOR)) {
            return env.addSource(new JSDicomGenerator(5L));
        }
        return env.fromSource(CustomKafkaSource.dataSourceStream(parameters), WatermarkStrategy.noWatermarks(), "Kafka_Source");
    }

    private static ParameterTool getParameterTool(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String profileFile = parameterTool.get(FlinkParamsConstants.RUN.PROFILES, "local") + "." + "properties";
        ParameterTool propertiesFile = ParameterTool.fromPropertiesFile(DicomCloudData.class.getClassLoader().getResourceAsStream(profileFile));
        return propertiesFile.mergeWith(parameterTool);
    }

}
