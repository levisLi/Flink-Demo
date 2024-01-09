package com.galaxy.neptune.flink.table;

import com.galaxy.neptune.flink.utils.DateUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Iterator;
import java.util.Map;

/**
 * TODO 定义flink_sql的udf函数
 *
 * @author lile
 * @description
 **/
@FunctionHint(
        input = @DataTypeHint("STRING"),
        output = @DataTypeHint("STRING"))
public class FirstDay extends ScalarFunction {
    public String  eval(String datetime) {
        return DateUtil.FirstDay(datetime);
    }
}
