package com.galaxy.neptune.flink.launch;

import com.galaxy.neptune.flink.bean.CommandArgs;
import com.galaxy.neptune.flink.config.ParamConfiguration;
import com.galaxy.neptune.flink.table.FirstDay;
import com.galaxy.neptune.flink.table.ParserMap;
import com.galaxy.neptune.flink.utils.CommandLineUtils;
import com.galaxy.neptune.flink.utils.FlinkEnv;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.io.IOException;
import java.util.List;

/**
 * TODO
 *
 * @author lile
 * @description
 **/
@Slf4j
public class FlinkSQLEngine {
    public static void main(String[] args) throws Exception {
        CommandArgs commandArgs = CommandLineUtils.parse(args, new CommandArgs());
        ParameterTool parameterTool = FlinkEnv.getParameterTool(args);
        //创建stream流环境
        StreamExecutionEnvironment env = FlinkEnv.getStreamExecutionEnvironment(parameterTool);
//        //创建table流环境
        StreamTableEnvironment tableEnv = FlinkEnv.buildStreamTableEnvironment(env);
        registerUDFFunction(tableEnv);
        executeTask(tableEnv,commandArgs);
    }

    /*
     * @description: 执行sql语法 insert select create
     * @param tableEnv
     * @param commandArgs
     * @author: lile
     */
    private static void executeTask(StreamTableEnvironment tableEnv,CommandArgs commandArgs) throws IOException {
        String targetFilePath = commandArgs.getConfigFile();
        log.info("开始执行作业任务操作job路径地址 {}",targetFilePath);
        List<String> jobSQL = ParamConfiguration.getJobString(targetFilePath);
        StatementSet statementSet = tableEnv.createStatementSet();
        for (String sqlTask : jobSQL) {
            System.out.println("执行FlinkSQL语句模块语法【{" + sqlTask + "}】");
            if(sqlTask.toUpperCase().contains("INSERT")){
                statementSet.addInsertSql(sqlTask);
            }else {
                tableEnv.executeSql(sqlTask);
            }
        }
        try {
            // 执行刚刚添加的所有 INSERT 语句
            TableResult tableResult = statementSet.execute();
            // 通过 TableResult 来获取作业状态
            System.out.println("====任务执行情况====>"+tableResult.getJobClient().get().getJobStatus());
        }catch (Exception e){
            System.out.println("========异常=======>"+e.getMessage());
            e.printStackTrace();
        }
    }

    /*
     * @description: 注册用户自定义的udf函数
     * @param tableEnv
     * @author: lile
     */
    private static void registerUDFFunction(StreamTableEnvironment tableEnv){
        tableEnv.createTemporaryFunction("parserMap", ParserMap.class);
        tableEnv.createTemporarySystemFunction("FIRST_DAY", FirstDay.class);
    }

}
