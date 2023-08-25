package com.galaxy.neptune.flink.source;

import com.galaxy.neptune.flink.config.FlinkParamsConstants;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CustomKafkaSource {

    // 创建Logger对象
    private static final Logger LOG = LoggerFactory.getLogger(CustomKafkaSource.class);


    public static KafkaSource<Tuple2<String, String>> dataSourceStream(ParameterTool parameters) {
        checkKafkaParams(parameters);
        String instance = parameters.get(FlinkParamsConstants.RUN.INSTANCE);
        String topic;
        String bootStrapServers;
        Properties kafkaProperties = new Properties();
        if (instance.equals("Ks3")) {
            topic = parameters.getRequired(FlinkParamsConstants.KAFKA.KS3_TOPICS);
            bootStrapServers = parameters.getRequired(FlinkParamsConstants.KAFKA.KS3_BOOTSTRAP_SERVERS);
            // TODO: 2023/3/20 生产环境ks3采集日志增加ssl机制校验
            Properties envProperties = parameters.getProperties();
            for (Object key : envProperties.keySet()) {
                if (key.toString().startsWith("kafka.")) {
                    String kafkaKey = key.toString().substring("kafka.".length(), key.toString().length());
                    kafkaProperties.setProperty(kafkaKey, envProperties.getProperty(key.toString()));
                }
            }
            LOG.info(kafkaProperties.toString());
        } else {
            topic = parameters.get(FlinkParamsConstants.KAFKA.DICOM_TOPICS);
            bootStrapServers = parameters.getRequired(FlinkParamsConstants.KAFKA.DICOM_BOOTSTRAP_SERVERS);
        }
        KafkaSourceBuilder<Tuple2<String, String>> kafkaSourceBuilder = KafkaSource.<Tuple2<String, String>>builder()
                //关闭自动提交模式
                .setProperty("enable.auto.commit", "false")
                .setBootstrapServers(bootStrapServers)
                .setTopics(topic)
                .setGroupId(parameters.get(FlinkParamsConstants.KAFKA.GROUP_ID))
                .setProperties(kafkaProperties)
                //自定义序列化类型
                .setDeserializer(KafkaRecordDeserializationSchema.of(new KafkaTopicConsumerSchema()));
        if (parameters.has(FlinkParamsConstants.KAFKA.OFFSET_TIMESTAMP)) {
            LOG.info("开始重topic【{}】的时间点【{}】消费数据",topic,parameters.getLong(FlinkParamsConstants.KAFKA.OFFSET_TIMESTAMP));
            kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.timestamp(parameters.getLong(FlinkParamsConstants.KAFKA.OFFSET_TIMESTAMP)));
        } else {
            kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST));
        }

        return kafkaSourceBuilder.build();
    }

    /**
     * @Description 设置kafka消费后的格式为元祖，第一个参数为topic，第二个参数为队列值
     * @Author:lile
     * @Param
     * @Return
     * @Date 2021/9/3
     * @Time 14:49
     */
    private static class KafkaTopicConsumerSchema implements KafkaDeserializationSchema<Tuple2<String, String>> {

        @Override
        public Tuple2<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
            String kafkaValue = null;
            try {
//                LOG.info("当前消费组分区{}偏移量{}",record.partition(),record.offset());
                kafkaValue = new String(record.value());
                String topic = record.topic();
                //添加kafka消息参数
                LOG.debug("kafka消息队列{}消息格式{}", topic, kafkaValue);
                return Tuple2.of(topic, kafkaValue);
            } catch (Exception e) {
                LOG.error("报文{},转义错误{}", kafkaValue, e.getMessage());
                return null;
            }
        }

        @Override
        public boolean isEndOfStream(Tuple2<String, String> stringStringTuple3) {
            return false;
        }

        @Override
        public TypeInformation<Tuple2<String, String>> getProducedType() {
            return TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
            });
        }
    }

    private static void checkKafkaParams(ParameterTool parameters) {
        if (!parameters.has(FlinkParamsConstants.KAFKA.DICOM_BOOTSTRAP_SERVERS)) {
            throw new RuntimeException("kafka数据源缺失bootstrapServers,请填写参数：" + FlinkParamsConstants.KAFKA.DICOM_BOOTSTRAP_SERVERS);
        }
        if (!parameters.has(FlinkParamsConstants.KAFKA.KS3_BOOTSTRAP_SERVERS)) {
            throw new RuntimeException("kafka数据源缺失bootstrapServers,请填写参数：" + FlinkParamsConstants.KAFKA.KS3_BOOTSTRAP_SERVERS);
        }
        if (!parameters.has(FlinkParamsConstants.KAFKA.KS3_TOPICS) || !parameters.has(FlinkParamsConstants.KAFKA.DICOM_TOPICS)) {
            throw new RuntimeException("kafka数据源缺失topic,请填写参数：" + FlinkParamsConstants.KAFKA.KS3_TOPICS + "/" + FlinkParamsConstants.KAFKA.DICOM_TOPICS);
        }
        if (!parameters.has(FlinkParamsConstants.KAFKA.GROUP_ID)) {
            throw new RuntimeException("kafka数据源缺失groupId,请填写参数：" + FlinkParamsConstants.KAFKA.GROUP_ID);
        }
    }
}
