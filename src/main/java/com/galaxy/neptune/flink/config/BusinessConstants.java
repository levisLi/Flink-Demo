package com.galaxy.neptune.flink.config;

/**
 * TODO
 *
 * @author lile
 * @description 业务系统常量类
 **/
public class BusinessConstants {

    public static final String ORGAN_CODE = "organ_code";

    public static final String SYSTEM_ID = "system_id";

    public static final String RIS_STUDY_ID = "ris_study_id";

    public static final String APPLY_DT = "apply_dt";

    public static final String CHK_DT = "chk_dt";

    public static final String LINK_ID = "linkid";

    public static final String IMAGE_NAME = "image_name";

    public static final String CHK_MODALITY_DIMENSION="chk_modality_dimension";
    //账期
    public static final String DT = "dt";
    //是否有限，1为有限，0为无效
    public static final String VALID = "valid";

    public static final String FAIR_VALID = "0";

    public static final String SUCCESS_VALID = "1";

    public class REDIS {
        public static final String HOST = "redis.host";

        public static final String PORT = "redis.port";

        public static final String PASSWORD = "redis.password";

    }

    public class DORIS {

        public static final String FE_NODES = "doris.fenodes";

        public static final String USER = "doris.user";

        public static final String URL = "doris.url";

        public static final String PASSWORD = "doris.password";

        public static final String DB = "doris.database";

        public static final String APPLY_TABLE = "doris.table.apply";

        public static final String STUDY_TABLE = "doris.table.study";

        public static final String REPORT_TABLE = "doris.table.report";

        public static final String SERIES_TABLE = "doris.table.series";

        public static final String KS3_TABLE = "doris.table.ks3";

        public static final String WRITE_PARALLELISM="doris.write.parallelism";

        public static final String WRITE_INTERVAL="doris.write.interval";

        public static final String WRITE_COUNTER="doris.write.counter";
    }
}
