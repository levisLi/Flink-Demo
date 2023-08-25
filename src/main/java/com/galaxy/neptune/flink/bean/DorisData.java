package com.galaxy.neptune.flink.bean;

import java.io.Serializable;

/**
 * TODO
 *
 * @author lile
 * @description
 **/
public class DorisData implements Serializable {
    private Integer id;
    private String name;
    private Boolean __DORIS_DELETE_SIGN__;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean get__DORIS_DELETE_SIGN__() {
        return __DORIS_DELETE_SIGN__;
    }

    public void set__DORIS_DELETE_SIGN__(Boolean __DORIS_DELETE_SIGN__) {
        this.__DORIS_DELETE_SIGN__ = __DORIS_DELETE_SIGN__;
    }

    public DorisData(Integer id, String name, Boolean __DORIS_DELETE_SIGN__) {
        this.id = id;
        this.name = name;
        this.__DORIS_DELETE_SIGN__ = __DORIS_DELETE_SIGN__;
    }

    public DorisData(Integer id, String name) {
        this.id = id;
        this.name = name;
    }
}
