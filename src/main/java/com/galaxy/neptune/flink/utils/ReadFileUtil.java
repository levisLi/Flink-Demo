package com.galaxy.neptune.flink.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;

/**
 * TODO
 *
 * @author lile
 * @description
 **/
public class ReadFileUtil {
    public static String readJsonLine(String  path) {
        String str="";
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(path);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
            BufferedReader reader = new BufferedReader(inputStreamReader);
            String tempString = null;
            while((tempString = reader.readLine()) != null){
                str += tempString;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return str;
    }
}
