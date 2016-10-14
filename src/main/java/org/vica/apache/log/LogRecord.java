package org.vica.apache.log;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * The Entity Project Of Apache Server Log Record
 * Created by Vica-tony on 9/22/2016.
 */
public class LogRecord {
//103.226.133.67 - - [21/Sep/2016:23:42:34 +0800] "GET /html/themes/some/custom-style.css?ver=4.5.4 HTTP/1.1" 200 105 "http://www.golveo.cn/" "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0"

    private static final SimpleDateFormat format = new SimpleDateFormat("d/MMM/yy:HH:mm:ss Z", Locale.ENGLISH);

    private String ip;
    private String loginName;
    private String userName;
    private Date dateTime;
    private String method;
    private String url;
    private String protocol;
    private int stateCode;
    private long responseLength;
    private String referFrom;
    private String userAgent;

    private String unhandled;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getLoginName() {
        return loginName;
    }

    public void setLoginName(String loginName) {
        this.loginName = loginName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Date getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        try {
            this.dateTime = format.parse(dateTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public int getStateCode() {
        return stateCode;
    }

    public void setStateCode(int stateCode) {
        this.stateCode = stateCode;
    }

    public long getResponseLength() {
        return responseLength;
    }

    public void setResponseLength(long responseLength) {
        this.responseLength = responseLength;
    }

    public String getReferFrom() {
        return referFrom;
    }

    public void setReferFrom(String referFrom) {
        if(referFrom!=null&&referFrom.equals("-"))
            referFrom="";
        this.referFrom = referFrom;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        if(userAgent!=null&&userAgent.equals("-"))
            userAgent="";
        this.userAgent = userAgent;
    }

    public String getUnhandled() {
        return unhandled;
    }

    public void setUnhandled(String unhandled) {
        this.unhandled = unhandled;
    }

    @Override
    public String toString() {
        return "LogRecord{" +
                "ip='" + ip + '\'' +
                ", loginName='" + loginName + '\'' +
                ", userName='" + userName + '\'' +
                ", dateTime=" + dateTime +
                ", method='" + method + '\'' +
                ", url='" + url + '\'' +
                ", protocol='" + protocol + '\'' +
                ", stateCode=" + stateCode +
                ", responseLength=" + responseLength +
                ", referFrom='" + referFrom + '\'' +
                ", userAgent='" + userAgent + '\'' +
                '}';
    }
}
