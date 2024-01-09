package com.galaxy.neptune.flink.table;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Iterator;
import java.util.Map;

/**
 * TODO
 *
 * @author lile
 * @description
 **/

@FunctionHint(
        input = @DataTypeHint("MAP<STRING, STRING>"),
        output = @DataTypeHint("Row< key STRING, value STRING>"))
public class ParserMap extends TableFunction<Row> {
    public void eval(Map<String, String> map) {
        try {
            Iterator<String> iterator = map.keySet().iterator();
            while (iterator.hasNext()) {
                String key =  iterator.next();
                String value = map.get(key);
                collect(Row.of(key, value));
            }
        } catch (Exception e) {

        }
    }
}
