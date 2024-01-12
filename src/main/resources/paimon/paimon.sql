--创建paimon的catalog
CREATE CATALOG hdfs_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'hdfs://192.168.211.106:8020/tmp/paimon'
)
