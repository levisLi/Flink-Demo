package com.galaxy.neptune.flink.source.generator;

import com.alibaba.fastjson.JSONObject;
import com.galaxy.neptune.flink.bean.DorisData;
import com.galaxy.neptune.flink.bean.cqcloud.ApplyDorisData;
import com.galaxy.neptune.flink.bean.cqcloud.ReportDorisData;
import com.galaxy.neptune.flink.bean.cqcloud.SeriesDorisData;
import com.galaxy.neptune.flink.bean.cqcloud.StudyDorisData;
import com.galaxy.neptune.flink.utils.DateUtil;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class CQDorisGenerator extends BaseGenerator<Tuple2<String, String>> implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(CQDorisGenerator.class);


    // 随机数据生成器对象
    private RandomDataGenerator generator;


    public CQDorisGenerator(Long maxRecordsPerSecond) {
        super(maxRecordsPerSecond);
        // 实例化生成器对象
        generator = new RandomDataGenerator();
    }


    @Override
    public Tuple2<String, String> invokeEvent(SplittableRandom rnd, Long id) {
        String risStudyId=generator.nextHexString(30);
        String organCode=generator.nextHexString(10);
        String systemId=generator.nextHexString(10);

//        String risStudyId="110";
//        String organCode="120";
//        String systemId="130";

        String monthPart="2023-10-01";
        String dt=DateUtil.Day();
        String linkId=organCode.concat(systemId).concat(risStudyId);

        String applyKey="apply";
        String reportKey="report";
        String studyKey="study";
        String seriesKey="series";
        Long checkDT=DateUtil.NOW_LONG();
        String cardNo= generator.nextHexString(18);

        ApplyDorisData applyDorisData = new ApplyDorisData(monthPart, organCode, risStudyId, systemId, generator.nextHexString(20), dt, linkId, "1", cardNo, generator.nextHexString(10), DateUtil.NOW(), generator.nextInt(1,10));
        ReportDorisData reportDorisData=new ReportDorisData(monthPart,organCode,risStudyId,systemId,generator.nextHexString(20),dt,linkId,"1",checkDT,cardNo,generator.nextHexString(5),DateUtil.NOW(), generator.nextInt(1,10));
        StudyDorisData studyDorisData=new StudyDorisData(monthPart,organCode,risStudyId,systemId,dt,linkId,"1",checkDT,cardNo,generator.nextHexString(5),generator.nextHexString(11),DateUtil.NOW_LONG(),DateUtil.NOW_LONG(),DateUtil.NOW(),generator.nextInt(1,2));
        SeriesDorisData seriesDorisData=new SeriesDorisData(monthPart,organCode,risStudyId,systemId,generator.nextHexString(10),dt,linkId,"1",DateUtil.NOW_LONG(),DateUtil.NOW(),generator.nextInt(1,10));

        JSONObject jsonObject=new JSONObject();
        jsonObject.put(applyKey,applyDorisData);
        jsonObject.put(reportKey,reportDorisData);
        jsonObject.put(studyKey,studyDorisData);
        jsonObject.put(seriesKey,seriesDorisData);

        String finalRecord=JSONObject.toJSONString(jsonObject);
//        LOG.warn(value.get()+"发送测试样列数据====>" + finalRecord);
        return new Tuple2<>("key", finalRecord);
    }
}
