package com.galaxy.neptune.flink.config;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

public class ParamConfiguration {

   public static JSONObject getJobConfig(String path) throws IOException {
        String userDir = System.getProperty("user.dir");
//        String jobResource = userDir.concat("/").concat(path);
        String jobContent = FileUtils.readFileToString(new File(path));
        return JSONObject.parseObject(jobContent);
    }

    public static List<String> getJobString(String path) throws IOException {
        String userDir = System.getProperty("user.dir");
        String sourceRecord = FileUtils.readFileToString(new File(path), Charset.defaultCharset());
        if(sourceRecord.contains(";")){
            return Arrays.asList(sourceRecord.split(";"));
        }else {
            return Arrays.asList(sourceRecord);
        }
    }

}
