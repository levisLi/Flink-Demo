package com.galaxy.neptune.flink.config;

/**
 * TODO
 *
 * @author lile
 * @description
 **/
public class FlinkParamsConstants {

    public static class KAFKA {

        public static final String KS3_TOPICS = "kafka.ks3.topic";

        public static final String KS3_BOOTSTRAP_SERVERS = "kafka.ks3.bootstrapServers";

        public static final String DICOM_TOPICS = "kafka.dicom.topic";

        public static final String DICOM_BOOTSTRAP_SERVERS = "kafka.dicom.bootstrapServers";

        public static final String GROUP_ID = "kafka.groupId";

        public static final String OFFSET_TIMESTAMP = "kafka.offset.timestamp";
    }

    public static class RUN {
        public static final String MODEL = "flink.run.model";
        public static final String INSTANCE = "flink.run.instance";
        public static final String PROFILES = "flink.profiles.active";
    }

    public static class CHECKPOINT {
        public static final String INTERVAL = "flink.checkpoint.interval";
        public static final String PATH = "flink.checkpoint.path";
    }


}
