package com.galaxy.neptune.flink.source.generator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class SourceGenerator extends BaseGenerator<Tuple2<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(SourceGenerator.class);

    private volatile AtomicInteger value=new AtomicInteger();

    public SourceGenerator(Long maxRecordsPerSecond) {
        super(maxRecordsPerSecond);
    }

    @Override
    public Tuple2<String, String> invokeEvent(SplittableRandom rnd, Long id) {
        Random random = new Random();
        int nextInt = random.nextInt(50);
        return ndm(rnd,id,nextInt);
    }


    private Tuple2<String, String> ndm(SplittableRandom rnd, Long id,int nextInt) {
        String kafkaTopic="";
        String kafkaKey ="";
        String kafkaValue = "";
        switch (nextInt%4){
            case 3:
                kafkaTopic= "iceberg";
                kafkaKey="tb7";
                kafkaValue = "{\n" +
                        "\"data\":[\n" +
                        " {\n" +
                        "\"accession_number\":\"M220075381\",\n" +
                        "\"acquisition_matrix\":\"144\",\n" +
                        "\"acquisition_time\":\"091134.79\",\n" +
                        "\"bits_allocated\":\"16\",\n" +
                        "\"bits_stored\":\"12\",\n" +
                        "\"body_part_examined\":\"LIVER\",\n" +
                        "\"chk_modality\":\"MR\",\n" +
                        "\"columns\":\"320\",\n" +
                        "\"content_date\":\"20221229\",\n" +
                        "\"content_time\":\"091403.03\",\n" +
                        "\"data_center\":\"CZ\",\n" +
                        "\"data_process_time\":1672287972571,\n" +
                        "\"data_receive_time\":1672288028776,\n" +
                        "\"db_dt\":\"125.412696838378\",\n" +
                        "\"dcm_analysis_time\":1672288028739,\n" +
                        "\"dcm_upload_time\":1672288028740,\n" +
                        "\"device_serial_number\":\"78354\",\n" +
                        "\"dicom_image_type\":\"ORIGINAL\",\n" +
                        "\"disable_flag\":0,\n" +
                        "\"echo_numbers\":\"1\",\n" +
                        "\"echo_time\":\"65.966\",\n" +
                        "\"echo_train_length\":\"55\",\n" +
                        "\"flip_angle\":\"90\",\n" +
                        "\"high_bit\":\"11\",\n" +
                        "\"image_count\":30,\n" +
                        "\"image_orientation\":\"0.99996387958526\",\n" +
                        "\"image_position\":\"-185.04980109911\",\n" +
                        "\"image_type\":1,\n" +
                        "\"imaged_nucleus\":\"1H\",\n" +
                        "\"imaging_frequency\":\"127.760608\",\n" +
                        "\"in_plane_phase_encoding_direction\":\"COL\",\n" +
                        "\"instance_creation_date\":\"20221229\",\n" +
                        "\"instance_creation_time\":\"091419.766\",\n" +
                        "\"instance_number\":\"44\",\n" +
                        "\"institution_name\":\"JS Yancheng No.1 People Hosp.\",\n" +
                        "\"magnetic_field_strength\":\"3\",\n" +
                        "\"manufacturer\":\"Philips\",\n" +
                        "\"manufacturers_model_name\":\"Ingenia CX\",\n" +
                        "\"mr_acquisition_type\":\"2D\",\n" +
                        "\"number_of_averages\":\"1\",\n" +
                        "\"number_of_phase_encoding_steps\":\"130\",\n" +
                        "\"organ_code\":\"12320900468211209A\",\n" +
                        "\"pacs_series_id\":\"14993785\",\n" +
                        "\"pacs_study_id\":\"M220075381\",\n" +
                        "\"patient_age\":\"079Y\",\n" +
                        "\"patient_birth_date\":\"19430219\",\n" +
                        "\"patient_id\":\"22152630\",\n" +
                        "\"patient_name\":\"Xu Pei Xiu\",\n" +
                        "\"patient_position\":\"HFS\",\n" +
                        "\"patient_sex\":\"F\",\n" +
                        "\"patient_weight\":\"80\",\n" +
                        "\"photometric_interpretation\":\"MONOCHROME2\",\n" +
                        "\"pixel_bandwidth\":\"3304\",\n" +
                        "\"pixel_representation\":\"0\",\n" +
                        "\"pixel_spacing\":\"1.25\",\n" +
                        "\"pregnancy_status\":\"1\",\n" +
                        "\"protocol_name\":\"sDWI_ b800\",\n" +
                        "\"reconstruction_diameter\":\"400\",\n" +
                        "\"repetition_time\":\"1401.36145019531\",\n" +
                        "\"ris_study_id\":\"221234877\",\n" +
                        "\"rows\":\"320\",\n" +
                        "\"samples_per_pixel\":\"1\",\n" +
                        "\"sar\":\"1.09642648696899\",\n" +
                        "\"scan_options\":\"FS\",\n" +
                        "\"scanning_sequence\":\"SE\",\n" +
                        "\"sequence_variant\":\"SK\",\n" +
                        "\"series_date\":\"20221229\",\n" +
                        "\"series_description\":\"sDWI_ b800\",\n" +
                        "\"series_instance_uid\":\"1.3.46.670589.11.78354.5.0.17476.2022122909140276000\",\n" +
                        "\"series_number\":\"704\",\n" +
                        "\"series_sort\":\"704\",\n" +
                        "\"series_time\":\"091402.76\",\n" +
                        "\"slice_location\":\"-120.06520306041\",\n" +
                        "\"slice_thickness\":\"6\",\n" +
                        "\"software_version\":\"6.1.571\",\n" +
                        "\"spacing_between_slices\":\"7\",\n" +
                        "\"station_name\":\"PHILIPS-KD0393R\",\n" +
                        "\"study_date\":\"20221229\",\n" +
                        "\"study_description\":\"MRCP\",\n" +
                        "\"study_id\":\"0\",\n" +
                        "\"study_instance_uid\":\"1.2.840.20221229090000.221234877\",\n" +
                        "\"study_time\":\"090020\",\n" +
                        "\"system_id\":\"pacs1\",\n" +
                        "\"transmit_coil_name\":\"BODY\",\n" +
                        "\"window_center\":\"2047.5\",\n" +
                        "\"window_width\":\"4095\"\n" +
                        " }\n" +
                        " ],\n" +
                        "\"dataType\":\"seriesInfo\",\n" +
                        "\"index_fields\":[\n" +
                        "\"ris_study_id\",\n" +
                        "\"system_id\",\n" +
                        "\"organ_code\",\n" +
                        "\"pacs_series_id\"\n" +
                        " ],\n" +
                        "\"link_fields\":[\n" +
                        "\"ris_study_id\",\n" +
                        "\"system_id\",\n" +
                        "\"organ_code\"\n" +
                        " ]\n" +
                        "}";
            break;
            case 2:
                kafkaTopic= "iceberg";
                kafkaKey="tb7";
                kafkaValue = "{\n" +
                        "    \"data\":[\n" +
                        "        {\n" +
                        "            \"organ_code\":\"c202303091011\",\n" +
                        "            \"organ_name\":\"江苏省人民医院\",\n" +
                        "            \"system_id\":\"pacs20230309001\",\n" +
                        "            \"ris_study_id\":\"risstudyid202303090654\",\n" +
                        "            \"cardno\":\"430729199104221419\",\n" +
                        "            \"cardtype\":\"01\",\n" +
                        "            \"mobile\":\"18217659442\",\n" +
                        "            \"name\":\"王五患者\",\n" +
                        "            \"sex\":2,\n" +
                        "            \"birthday\":199104221419,\n" +
                        "            \"reg_doc_idcard\":\"43072320201205008X\",\n" +
                        "            \"reg_doc_name\":\"张三医生\",\n" +
                        "            \"reg_dt\":1678292435000,\n" +
                        "            \"op_em_hp_ex_mark\":\"\",\n" +
                        "            \"chk_modality\":\"\",\n" +
                        "            \"chk_status\":8,\n" +
                        "            \"ckpt_name\":\"胸部\",\n" +
                        "            \"dev_name\":\"放射荧光透视数字胃肠机器\",\n" +
                        "            \"chk_dt\":1672275966000,\n" +
                        "            \"proj_name\":\"胸部\",\n" +
                        "            \"proj_no\":\"\",\n" +
                        "            \"img_count\":1,\n" +
                        "            \"arrive_dt\":1672275966000,\n" +
                        "            \"idcard_hos_type\":\"\",\n" +
                        "            \"chk_desc\":\"\",\n" +
                        "            \"chk_doc_idcard\":\"\",\n" +
                        "            \"chk_doc_name\":\"\",\n" +
                        "            \"chk_dpt_code\":\"\",\n" +
                        "            \"chk_dpt_name\":\"\",\n" +
                        "            \"chk_methodname\":\"\",\n" +
                        "            \"data_center\":\"CZ\",\n" +
                        "            \"data_receive_time\":1672288028307,\n" +
                        "            \"dev_room\":\"\",\n" +
                        "            \"disable_flag\":0,\n" +
                        "            \"ename\":\"\",\n" +
                        "            \"hash_id\":\"1672287947000\",\n" +
                        "            \"hos_proj_no\":\"\",\n" +
                        "            \"idcard_hos\":\"\",\n" +
                        "            \"op_em_hp_ex_no\":\"202212290111\",\n" +
                        "            \"patient_idcard\":\"320483000000003921\",\n" +
                        "            \"system_patient_id\":\"0251168\",\n" +
                        "            \"update_time\":1672287947000\n" +
                        "        }\n" +
                        "    ],\n" +
                        "    \"dataType\":\"examInfo\",\n" +
                        "    \"index_fields\":[\n" +
                        "        \"organ_code\",\n" +
                        "        \"ris_study_id\",\n" +
                        "        \"system_id\"\n" +
                        "    ],\n" +
                        "    \"link_fields\":[\n" +
                        "        \"organ_code\",\n" +
                        "        \"ris_study_id\",\n" +
                        "        \"system_id\"\n" +
                        "    ]\n" +
                        "}";
                break;
            case 1:
                kafkaTopic= "iceberg";
                kafkaKey="tb7";
                kafkaValue = "{\n" +
                        "  \"data\":[\n" +
                        "    {\n" +
                        "      \"allergy_history\":\"\",\n" +
                        "      \"app_doc_idcard\":\"430121199506252810\",\n" +
                        "      \"app_doc_name\":\"李四\",\n" +
                        "      \"app_dpt_code\":\"\",\n" +
                        "      \"app_dpt_name\":\"ZQCT\",\n" +
                        "      \"app_remark\":\"\",\n" +
                        "      \"apply_dt\":1672275966000,\n" +
                        "      \"birthday\":483552000000,\n" +
                        "      \"cardno\":\"320721199202142518\",\n" +
                        "      \"cardtype\":\"01\",\n" +
                        "      \"chk_methodname\":\"\",\n" +
                        "      \"chk_modality\":\"CT\",\n" +
                        "      \"ckpt_name\":\"胸部\",\n" +
                        "      \"clinic_diagnose\":\"肺部感染主诉：咳嗽咳痰⼀周余现病史：痰为脓⻩⾊。伴胸闷\",\n" +
                        "      \"data_center\":\"CZ\",\n" +
                        "      \"data_receive_time\":1672288028303,\n" +
                        "      \"disable_flag\":0,\n" +
                        "      \"exam_id\":\"5ecd4e54-ba65-4b9d-ab92-40e8d01d6a91\",\n" +
                        "      \"hash_id\":\"1672287947000\",\n" +
                        "      \"hos_pack_name\":\"\",\n" +
                        "      \"hos_pack_no\":\"\",\n" +
                        "      \"hos_proj_no\":\"\",\n" +
                        "      \"idcard_hos\":\"\",\n" +
                        "      \"idcard_hos_type\":\"\",\n" +
                        "      \"mobile\":\"15961427087\",\n" +
                        "      \"name\":\"张三\",\n" +
                        "      \"op_em_hp_ex_mark\":null,\n" +
                        "      \"op_em_hp_ex_no\":\"202212290111\",\n" +
                        "      \"organ_code\":\"123204044673580491\",\n" +
                        "      \"organ_empi\":\"0251168\",\n" +
                        "      \"organ_global_empi\":\"\",\n" +
                        "      \"organ_name\":\"中文医院二\",\n" +
                        "      \"patient_idcard\":\"320483198504293921\",\n" +
                        "      \"proj_name\":\"胸部\",\n" +
                        "      \"proj_no\":\"\",\n" +
                        "      \"ris_study_id\":\"310840123456\",\n" +
                        "      \"ris_study_ids\":\"310840\",\n" +
                        "      \"rpt_no\":\"5ecd4e54-ba65-4b9d-ab92-40e8d01d6a91\",\n" +
                        "      \"sex\":2,\n" +
                        "      \"sickbed_number\":\"\",\n" +
                        "      \"subj_complaint\":\"肺部感染\",\n" +
                        "      \"symptom\":\"\",\n" +
                        "      \"system_id\":\"pacs01\",\n" +
                        "      \"type\":2,\n" +
                        "      \"update_time\":1672287947000,\n" +
                        "      \"ward\":\"\"\n" +
                        "    }\n" +
                        "  ],\n" +
                        "  \"dataType\":\"examApplyInfo\",\n" +
                        "  \"index_fields\":[\n" +
                        "    \"organ_code\",\n" +
                        "    \"ris_study_id\",\n" +
                        "    \"system_id\",\n" +
                        "    \"exam_id\"\n" +
                        "  ],\n" +
                        "  \"link_fields\":[\n" +
                        "    \"organ_code\",\n" +
                        "    \"ris_study_id\",\n" +
                        "    \"system_id\"\n" +
                        "  ]\n" +
                        "}";
                break;
            case 0:
                kafkaTopic= "iceberg";
                kafkaKey="tb7";
                kafkaValue = "{\"data\":[\n" +
                        " {\n" +
                        "\"data_center\":\"CZ\",\n" +
                        "\"data_receive_time\":1672288028679,\n" +
                        "\"disable_flag\":0,\n" +
                        "\"hash_id\":\"ReportPo_3a375547f96f5f3dbf4bb7faee94860b\",\n" +
                        "\"organ_code\":\"123204044673580491\",\n" +
                        "\"organ_name\":\"中文医院二\",\n" +
                        "\"result_id\":\"20221229000480\",\n" +
                        "\"ris_study_id\":\"20221229000480\",\n" +
                        "\"rpt_code\":\"20221229000480\",\n" +
                        "\"rpt_descrip\":\"\",\n" +
                        "\"rpt_dt\":\"123-\",\n" +
                        "\"rpt_idcard\":\"342522000000000629\",\n" +
                        "\"rpt_image_count\":0,\n" +
                        "\"rpt_name\":\"张三\",\n" +
                        "\"rpt_pub_dt\":1672286413000,\n" +
                        "\"rpt_seeing\":\"\",\n" +
                        "\"rpt_share\":0,\n" +
                        "\"rpt_status\":10,\n" +
                        "\"rpt_type\":\"2\",\n" +
                        "\"rv_dt\":\"123-\",\n" +
                        "\"rv_idcard\":\"340322000000008840\",\n" +
                        "\"rv_name\":\"张三\",\n" +
                        "\"snapshot_count\":1,\n" +
                        "\"system_id\":\"01\",\n" +
                        "\"update_time\":-123\n" +
                        " }\n" +
                        " ],\n" +
                        "\"dataType\":\"reportInfo\",\n" +
                        "\"index_fields\":[\n" +
                        "\"organ_code\",\n" +
                        "\"ris_study_id\",\n" +
                        "\"system_id\",\n" +
                        "\"result_id\"\n" +
                        " ],\n" +
                        "\"link_fields\":[\n" +
                        "\"organ_code\",\n" +
                        "\"ris_study_id\",\n" +
                        "\"system_id\"\n" +
                        " ]\n" +
                        "}";
                break;
//            case 2:
//                kafkaKey="tb3";
//                kafkaValue = "[{\"name\":\"xx\",\"age\":23,\"station_code\":\""+nextInt+"\"}]";
//                break;
//            case 1:
//                kafkaTopic= "iceberg";
//                kafkaKey="tb5";
//                kafkaValue = "[{\"id\":345,\"msg\":\""+nextInt+"\",\"date\":\"2022-12-28\"}]";
//                break;
        }
        value.addAndGet(1);
        LOG.warn(value.get()+"发送测试样列数据====>" + kafkaValue);
        return new Tuple2<>(kafkaTopic, kafkaValue);
    }
}
