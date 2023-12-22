package com.galaxy.neptune.flink.bean.cqcloud;

/**
 * TODO
 *
 * @author lile
 * @description
 **/
public class ReportDorisData {
    private String month_part;
    private String organ_code;
    private String ris_study_id;
    private String system_id;
    private String result_id;
    private String dt;
    private String link_id;
    private String version_id;
    private long update_time;
    private String cardno;
    private String name;
    private String create_datetime;
    private int  cnt;

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

    public String getResult_id() {
        return result_id;
    }

    public void setResult_id(String result_id) {
        this.result_id = result_id;
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

    public long getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(long update_time) {
        this.update_time = update_time;
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



    public ReportDorisData(String month_part, String organ_code, String ris_study_id, String system_id, String result_id, String dt, String link_id, String version_id, long update_time, String cardno, String name, String creat_datetime, int cnt) {
        this.month_part = month_part;
        this.organ_code = organ_code;
        this.ris_study_id = ris_study_id;
        this.system_id = system_id;
        this.result_id = result_id;
        this.dt = dt;
        this.link_id = link_id;
        this.version_id = version_id;
        this.update_time = update_time;
        this.cardno = cardno;
        this.name = name;
        this.create_datetime = creat_datetime;
        this.cnt = cnt;
    }
}
