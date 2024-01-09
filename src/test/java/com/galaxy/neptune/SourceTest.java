package com.galaxy.neptune;

import com.alibaba.fastjson.JSONObject;

import java.io.*;

/**
 * TODO
 *
 * @author lile
 * @description
 **/
public class SourceTest {
    public static void main(String[] args) throws Exception {
        String path=SourceTest.class.getClassLoader().getResource("source/apply_source.json").getPath();
        JSONObject jsonObject = JSONObject.parseObject(readJsonLine(path),JSONObject.class);
        jsonObject.getJSONArray("data").getJSONObject(0).put("apply_dt",System.currentTimeMillis());
        System.out.println(jsonObject);
    }

    private static String readJsonLine(String  path) throws Exception {
        String str="";
        FileInputStream fileInputStream = new FileInputStream(path);
        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
        BufferedReader reader = new BufferedReader(inputStreamReader);
        String tempString = null;
        while((tempString = reader.readLine()) != null){
            str += tempString;
        }
        return str;
    }

}
