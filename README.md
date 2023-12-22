# Flink-Demo
# 部署脚本
flink run -m yarn-cluster -yjm 1024m -ytm 2048m -ynm "flink_demo_engine" -p 2 -d -c com.galaxy.neptune.flink.FlinkDorisDemo /home/hadoop/ll/Flink-Demo-1.0-SNAPSHOT.jar --flink.run.model prod --flink.profiles.active prod

#savepoint恢复启动
flink run -m yarn-cluster -s hdfs:///flink/checkpoints/flink-demo/10133f979fb8955c4f5fedd2a504ce85/chk-12/_metadata   -yjm 6144m -ytm 6144m -ynm "flink_demo_engine" -p 2 -d -c com.galaxy.neptune.flink.FlinkDorisDemo /home/hadoop/ll/Flink-Demo-1.0-SNAPSHOT.jar --flink.profiles.active prod

#触发savepoint操作
flink savepoint jobIb -yid yarn_applicationId