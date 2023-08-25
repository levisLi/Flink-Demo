package com.galaxy.neptune.flink.config;

public class FieldConstants {
    //共有字段
    public static final String WEEK = "week";
    public static final String DT = "dt";
    public static final String SOURCE = "source";
    public static final String COUNTER = "counter";
    public static final String VALID = "valid";
    public static final String UPDATE_TIME = "update_time";
    public static final String DATA_RECEIVE_TIME = "data_receive_time";
    public static final String DATA_PROCESS_TIME = "data_process_time";
    public static final String DATA_STORE_TIME = "data_store_time";
    public static final String ORGAN_CODE = "organ_code";
    public static final String SYSTEM_ID = "system_id";
    public static final String RIS_STUDY_ID = "ris_study_id";
    //apply稽查字段
    public static final String APPLY_EXAM_ID = "exam_id";
    public static final String APPLY_IS_FOUNDED = "is_founded";
    public static final String APPLY_SUBJ_COMPLAINT = "subj_complaint";
    public static final String APPLY_CLINIC_DIAGNOSE = "clinic_diagnose";
    public static final String APPLY_APP_REMARK = "app_remark";
    public static final String APPLY_SYMPTOM = "symptom";
    public static final String APPLY_NAME = "name";
    public static final String APPLY_CARD_NO = "cardno";
    public static final String APPLY_SEX = "sex";

    public static final String[] APPLY_IS_APPLY_DOC_ID_CARD = {"is_apply_doc_idcard", "app_doc_idcard"};
    public static final String[] APPLY_IS_APPLICATION_DEPARTMENT={"is_application_department","app_dpt_name"};
    public static final String[] APPLY_IS_SUBJ_COMPLAINT={"is_subj_complaint","subj_complaint"};

    //report稽查字段
    public static final String REPORT_ORGAN_NAME = "organ_name";
    public static final String REPORT_RESULT_ID = "result_id";
    public static final String[] REPORT_IS_RPT_DT = {"is_rpt_dt", "rpt_dt"};
    public static final String[] REPORT_IS_RV_DT = {"is_rv_dt", "rv_dt"};
    public static final String[] REPORT_IS_RPT_SEEING = {"is_rpt_seeing", "rpt_seeing", "rpt_status"};
    public static final String[] REPORT_IS_RPT_TYPE = {"is_rpt_type", "rpt_type"};
    public static final String[] REPORT_IS_REPORT_DOC = {"is_report_doc", "rpt_idcard"};
    public static final String[] REPORT_IS_RV_DOC = {"is_rv_doc", "rv_idcard"};
    public static final String[] REPORT_IS_RPT_DPT_NAME = {"is_rpt_dpt_name", "rpt_dpt_name"};
    public static final String[] REPORT_IS_RPT_NAME = {"is_rpt_name", "rpt_name"};
    public static final String[] REPORT_IS_RV_NAME = {"is_rv_name", "rv_name"};

    //exam稽查字段
    public static final String EXAM_ORGAN_NAME = "organ_name";
    public static final String[] EXAM_IS_CARD_NO = {"is_cardno", "cardno"};
    public static final String[] EXAM_IS_NAME = {"is_name", "name"};
    public static final String[] EXAM_IS_PROJ_NO = {"is_proj_no", "hos_proj_no"};
    public static final String[] EXAM_IS_PROJ_NAME = {"is_proj_name", "proj_name"};
    public static final String[] EXAM_IS_CHK_MODALITY = {"is_chk_modality", "chk_modality","chk_modality_std"};
    public static final String[] EXAM_IS_CHECK_STATUS = {"is_check_status", "chk_status"};
    public static final String[] EXAM_IS_CARD_NO_TYPE = {"is_cardno_type", "cardtype"};
    public static final String[] EXAM_IS_OP_EM_HP_EX_MARK = {"is_op_em_hp_ex_mark", "op_em_hp_ex_mark"};
    public static final String[] EXAM_IS_REG_DOC = {"is_reg_doc", "reg_doc_idcard"};
    public static final String[] EXAM_IS_ORGAN_NAME = {"is_organ_name", "organ_name"};
    public static final String[] EXAM_IS_SEX = {"is_sex", "sex"};
    public static final String[] EXAM_IS_CHK_DT = {"is_chk_dt", "chk_dt"};
    public static final String[] EXAM_IS_DEV_NAME = {"is_dev_name", "dev_name","tmp_chk_modality"};
    public static final String[] EXAM_IS_CKPT_NAME={"is_ckpt_name","ckpt_name"};
    public static final String[] EXAM_IS_CHK_DOC_NAME={"is_chk_doc_name","chk_doc_name"};

    //序列稽查字段
    public static final String SERIES_ORGAN_NAME = "organ_name";
    public static final String SERIES_PACS_SERIES_ID = "pacs_series_id";
    public static final String SERIES_IMAGE_COUNT = "image_count";
    public static final String SERIES_DCM_ANALYSIS_TIME = "dcm_analysis_time";
    public static final String SERIES_DCM_UPLOAD_TIME = "dcm_upload_time";


}
