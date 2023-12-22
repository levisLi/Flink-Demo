package com.galaxy.neptune.flink.bean.cqcloud;

/**
 * TODO
 *
 * @author lile
 * @description
 **/
public class ApplyDorisData {
    private String month_part;
    private String organ_code;
    private String ris_study_id;
    private String system_id;
    private String exam_id;
    private String dt;
    private String link_id;
    private String version_id;
    private String cardno;
    private String name;
    private String create_datetime;
    private int cnt;

    public String getMonth_part() {
        return month_part;
    }

    public void setMonth_part(String month_part) {
        this.month_part = month_part;
    }

    public String getOrgan_code() {
        return organ_code;
    }

    public void setOrgan_code(String organ_code) {
        this.organ_code = organ_code;
    }

    public String getRis_study_id() {
        return ris_study_id;
    }

    public void setRis_study_id(String ris_study_id) {
        this.ris_study_id = ris_study_id;
    }

    public String getSystem_id() {
        return system_id;
    }

    public void setSystem_id(String system_id) {
        this.system_id = system_id;
    }

    public String getExam_id() {
        return exam_id;
    }

    public void setExam_id(String exam_id) {
        this.exam_id = exam_id;
    }

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public String getLink_id() {
        return link_id;
    }

    public void setLink_id(String link_id) {
        this.link_id = link_id;
    }

    public String getVersion_id() {
        return version_id;
    }

    public void setVersion_id(String version_id) {
        this.version_id = version_id;
    }

    public String getCardno() {
        return cardno;
    }

    public void setCardno(String cardno) {
        this.cardno = cardno;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCreate_datetime() {
        return create_datetime;
    }

    public void setCreate_datetime(String create_datetime) {
        this.create_datetime = create_datetime;
    }

    public int getCnt() {
        return cnt;
    }

    public void setCnt(int cnt) {
        this.cnt = cnt;
    }

    public ApplyDorisData(String month_part, String organ_code, String ris_study_id, String system_id, String exam_id, String dt, String link_id, String version_id, String cardno, String name, String create_datetime, int cnt) {
        this.month_part = month_part;
        this.organ_code = organ_code;
        this.ris_study_id = ris_study_id;
        this.system_id = system_id;
        this.exam_id = exam_id;
        this.dt = dt;
        this.link_id = link_id;
        this.version_id = version_id;
        this.cardno = cardno;
        this.name = name;
        this.create_datetime = create_datetime;
        this.cnt = cnt;
    }

}
