package com.galaxy.neptune.flink.source.generator;

import com.alibaba.fastjson.JSONObject;
import com.galaxy.neptune.flink.utils.DateUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

public class JSDicomGenerator extends BaseGenerator<Tuple2<String, String>> implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(JSDicomGenerator.class);


    // 随机数据生成器对象
    private RandomDataGenerator generator;

    private JSONObject applyJsonObject;
    private JSONObject studyJsonObject;
    private JSONObject reportJsonObject;
    private JSONObject seriesJsonObject;
    private JSONObject ks3JsonObject;

    public JSDicomGenerator(Long maxRecordsPerSecond) {
        super(maxRecordsPerSecond);
        // 实例化生成器对象
        generator = new RandomDataGenerator();
        try {
            applyJsonObject = JSONObject.parseObject(IOUtils.toString(JSDicomGenerator.class.getClassLoader().getResourceAsStream("source/apply_source.json"), StandardCharsets.UTF_8), JSONObject.class);
            studyJsonObject = JSONObject.parseObject(IOUtils.toString(JSDicomGenerator.class.getClassLoader().getResourceAsStream("source/study_source.json"), StandardCharsets.UTF_8), JSONObject.class);
            reportJsonObject = JSONObject.parseObject(IOUtils.toString(JSDicomGenerator.class.getClassLoader().getResourceAsStream("source/report_source.json"), StandardCharsets.UTF_8), JSONObject.class);
            seriesJsonObject = JSONObject.parseObject(IOUtils.toString(JSDicomGenerator.class.getClassLoader().getResourceAsStream("source/series_source.json"), StandardCharsets.UTF_8), JSONObject.class);
            ks3JsonObject = JSONObject.parseObject(IOUtils.toString(JSDicomGenerator.class.getClassLoader().getResourceAsStream("source/ks3_source.json"), StandardCharsets.UTF_8), JSONObject.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public JSDicomGenerator(Long maxRecordsPerSecond,JSONObject apply,JSONObject study,JSONObject report,JSONObject series,JSONObject ks3) {
        super(maxRecordsPerSecond);
        // 实例化生成器对象
        generator = new RandomDataGenerator();
        this.applyJsonObject=apply;
        this.studyJsonObject=study;
        this.reportJsonObject=report;
        this.seriesJsonObject=series;
        this.ks3JsonObject=ks3;
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

        String applyKey = "apply";
        String reportKey = "report";
        String studyKey = "study";
        String seriesKey = "series";
        String ks3Key = "ks3";

        Long checkDT = DateUtil.NOW_LONG();
        String cardNo = generator.nextHexString(18);

        applyJsonObject.getJSONArray("data").getJSONObject(0).put("ris_study_id", risStudyId);
        applyJsonObject.getJSONArray("data").getJSONObject(0).put("organ_code", organCode);
        applyJsonObject.getJSONArray("data").getJSONObject(0).put("exam_id", examId);
        applyJsonObject.getJSONArray("data").getJSONObject(0).put("apply_dt", System.currentTimeMillis());
        applyJsonObject.getJSONArray("data").getJSONObject(0).put("update_time", System.currentTimeMillis());

        reportJsonObject.getJSONArray("data").getJSONObject(0).put("ris_study_id", risStudyId);
        reportJsonObject.getJSONArray("data").getJSONObject(0).put("organ_code", organCode);
        reportJsonObject.getJSONArray("data").getJSONObject(0).put("exam_id", examId);
        reportJsonObject.getJSONArray("data").getJSONObject(0).put("result_id", DateUtil.NOW_1());
        reportJsonObject.getJSONArray("data").getJSONObject(0).put("rpt_dt", System.currentTimeMillis());
        reportJsonObject.getJSONArray("data").getJSONObject(0).put("update_time", System.currentTimeMillis());

        studyJsonObject.getJSONArray("data").getJSONObject(0).put("ris_study_id", risStudyId);
        studyJsonObject.getJSONArray("data").getJSONObject(0).put("organ_code", organCode);
        studyJsonObject.getJSONArray("data").getJSONObject(0).put("exam_id", examId);
        studyJsonObject.getJSONArray("data").getJSONObject(0).put("chk_dt", System.currentTimeMillis());
        studyJsonObject.getJSONArray("data").getJSONObject(0).put("update_time", System.currentTimeMillis());

        seriesJsonObject.getJSONArray("data").getJSONObject(0).put("ris_study_id", risStudyId);
        seriesJsonObject.getJSONArray("data").getJSONObject(0).put("organ_code", organCode);
        seriesJsonObject.getJSONArray("data").getJSONObject(0).put("exam_id", examId);
        seriesJsonObject.getJSONArray("data").getJSONObject(0).put("linkId", examId);
        seriesJsonObject.getJSONArray("data").getJSONObject(0).put("pacs_series_id", pacsSeriesId);

        List<JSONObject> ks3JsonObjectList=new ArrayList<>();
        for(int i=0;i<100;i++){
            ks3JsonObject.put("linkId", linkId);
            ks3JsonObject.put("objSize", generator.nextLong(1000, 10000));
            ks3JsonObject.put("timeLocal", DateUtil.NOW());
            ks3JsonObject.put("requestTime", generator.nextF(0.1, 0.9));
            ks3JsonObject.put("responseTime", generator.nextF(0.1, 0.9));
            String key = "/image/" + organCode + "/" + systemId + "/" + risStudyId + "_" + pacsSeriesId + "/" + generator.nextHexString(20) + ".dcm";
            ks3JsonObject.put("key", key);
            ks3JsonObjectList.add(ks3JsonObject);
        }

        JSONObject jsonObject = new JSONObject();
        jsonObject.put(applyKey, applyJsonObject);
        jsonObject.put(reportKey, reportJsonObject);
        jsonObject.put(studyKey, studyJsonObject);
        jsonObject.put(seriesKey, seriesJsonObject);
        jsonObject.put(ks3Key, ks3JsonObjectList);

        String finalRecord = JSONObject.toJSONString(jsonObject);
        return new Tuple2<>("key", finalRecord);
    }
}
