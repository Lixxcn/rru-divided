package com.inspur.model;

import java.util.Date;

/**
 * @author Li-Xiaoxu
 * @version 1.0
 * @date 2020/7/17 17:53
 */
public class RruInfo {
    private String oid;
    private String  cellId;
    private String rruId;
    private Date timeStamp;
    private Integer provinceId;

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public String getCellId() {
        return cellId;
    }

    public void setCellId(String cellId) {
        this.cellId = cellId;
    }

    public String getRruId() {
        return rruId;
    }

    public void setRruId(String rruId) {
        this.rruId = rruId;
    }

    public Date getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Date timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Integer getProvinceId() {
        return provinceId;
    }

    public void setProvinceId(Integer provinceId) {
        this.provinceId = provinceId;
    }

    @Override
    public String toString() {
        return "RruInfo{" +
                "oid='" + oid + '\'' +
                ", cellId='" + cellId + '\'' +
                ", rruId='" + rruId + '\'' +
                ", timeStamp=" + timeStamp +
                ", provinceId=" + provinceId +
                '}';
    }
}
