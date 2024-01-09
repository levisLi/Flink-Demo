package com.galaxy.neptune.flink.source.generator;

import com.alibaba.fastjson.JSONObject;
import com.galaxy.neptune.flink.utils.DateUtil;
import com.galaxy.neptune.flink.utils.ReadFileUtil;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.SplittableRandom;

public class KS3Generator extends BaseGenerator<Tuple2<String, String>> implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(KS3Generator.class);


    // 随机数据生成器对象
    private RandomDataGenerator generator;


    public KS3Generator(Long maxRecordsPerSecond) {
        super(maxRecordsPerSecond);
        // 实例化生成器对象
        generator = new RandomDataGenerator();
    }


    @Override
    public Tuple2<String, String> invokeEvent(SplittableRandom rnd, Long id) {
        Long risStudyId = generator.nextLong(100000, 1000000);
        String organCode = generator.nextHexString(10);
        String systemId = generator.nextHexString(10);
        String examId = generator.nextHexString(10);
        String pacsSeriesId = generator.nextHexString(15);

        String dt = DateUtil.Day();
        String linkId = organCode.concat(systemId).concat(risStudyId.toString());
        String ks3Key = "ks3";


        JSONObject ks3JsonObject = JSONObject.parseObject(ReadFileUtil.readJsonLine(this.getClass().getClassLoader().getResource("source/ks3_source.json").getPath()), JSONObject.class);
        ks3JsonObject.put("linkId", linkId);
        ks3JsonObject.put("objSize", generator.nextLong(1000, 10000));
        ks3JsonObject.put("timeLocal", DateUtil.NOW());
        ks3JsonObject.put("requestTime", generator.nextF(0.1, 0.9));
        ks3JsonObject.put("responseTime", generator.nextF(0.1, 0.9));
        String key = "/image/" + organCode + "/" + systemId + "/" + risStudyId + "_" + pacsSeriesId + "/" + generator.nextHexString(20) + ".dcm";
        ks3JsonObject.put("key", key);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put(ks3Key, ks3JsonObject);

        String finalRecord = JSONObject.toJSONString(jsonObject);
        return new Tuple2<>("key", finalRecord);
    }
}
