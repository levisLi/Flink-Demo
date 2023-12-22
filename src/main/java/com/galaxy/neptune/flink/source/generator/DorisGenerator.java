package com.galaxy.neptune.flink.source.generator;

import com.alibaba.fastjson.JSONObject;
import com.galaxy.neptune.flink.bean.DorisData;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class DorisGenerator extends BaseGenerator<Tuple2<String, String>> implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DorisGenerator.class);

    private volatile AtomicInteger value=new AtomicInteger();

    private final static String _DORIS_DELETE_SIGN__="_DORIS_DELETE_SIGN__";

    private final static String __DORIS_DELETE_SIGN__="__DORIS_DELETE_SIGN__";


    // 随机数据生成器对象
    private RandomDataGenerator generator;


    public DorisGenerator(Long maxRecordsPerSecond) {
        super(maxRecordsPerSecond);
        // 实例化生成器对象
        generator = new RandomDataGenerator();
    }


    @Override
    public Tuple2<String, String> invokeEvent(SplittableRandom rnd, Long id) {
        String key="doris";
        DorisData dorisData = new DorisData(
                generator.nextInt(1, 100),
                generator.nextHexString(20),
                false
        );
        JSONObject record = JSONObject.parseObject(JSONObject.toJSONString(dorisData));
        if(record.containsKey(_DORIS_DELETE_SIGN__)){
            Boolean recordValue = record.getBoolean(_DORIS_DELETE_SIGN__);
            record.remove(_DORIS_DELETE_SIGN__);
            record.put(__DORIS_DELETE_SIGN__,recordValue);
        }
        value.addAndGet(1);
        String finalRecord=record.toJSONString();
        LOG.warn(value.get()+"发送测试样列数据====>" + finalRecord);
        return new Tuple2<>(key, finalRecord);
    }
}
