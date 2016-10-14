package org.vica.apache.log;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The Log Parser Of Apache Server
 * Created by Vica-tony on 9/22/2016.
 */
public class LogAnalyser {

    /*
    ^ :匹配每一行的开头。
    ^([0-9.]+)\s :匹配IP地址，\s匹配不可见字符，例如空格、制表符等。
    ([\w."-]+)\s :匹配identity，由下划线在内的任何单词字符（包括数字）或点引号杠组成。
    ([\w."-]+)\s :匹配userid。
    \[([^\[\]]+)\]\s :匹配时间，匹配外围的中括号且内部不存在中括号。
    "(.+)"\s :匹配请求信息，匹配外围双引号，匹配内部任意字符。
    (\d{3})\s :匹配状态码，三个长度的数字。
    (\d+|-)\s :匹配响应字节数或-。
    "((?:[^"]|\"-)*)"\s :匹配"Referer"请求头，双引号中可能出现转义的双引号\"。
    "((?:[^"]|\"-)*)" :匹配"User-Agent"请求头。
    $ :匹配行尾。
     */
    private static final String regx = "^([0-9.]+)\\s([\\w.\"-]+)\\s([\\w.\"-]+)\\s\\[([^\\[\\]]+)\\]\\s\"(.+)\"\\s(\\d{3})\\s(\\d+|-)\\s\"((?:[^\"]|\\\"-)*)\"\\s\"((?:[^\"]|\\\"-)*)\"$";
    private static final Pattern pattern = Pattern.compile(regx);

    /**
     * Read String And Assemble Record Object
     * @param source
     * @return
     */
    public static LogRecord read(String source){
        Matcher matcher = pattern.matcher(source);
        if(!matcher.matches()|| matcher.groupCount()!=9){
            throw new IllegalArgumentException("error input format, matcher groups only is "+matcher.groupCount()+", source is : "+source);
        }else {
            LogRecord record = new LogRecord();
            record.setIp(matcher.group(1));
            record.setLoginName(matcher.group(2).replaceAll("-",""));
            record.setUserName(matcher.group(3).replaceAll("-",""));
            record.setDateTime(matcher.group(4));
            String[] tmp = matcher.group(5).split(" ");
            if(tmp.length!=3){
                record.setUrl(matcher.group(5));
                //throw new IllegalArgumentException("the %r (RequestHeader) format error, that is : "+matcher.group(5)+", source is : "+source);
            }else {
                record.setMethod(tmp[0]);
                record.setUrl(tmp[1]);
                record.setProtocol(tmp[2]);
            }
            record.setStateCode(Integer.parseInt(matcher.group(6).replace('-','0')));
            record.setResponseLength(Long.parseLong(matcher.group(7).replace('-','0')));
            record.setReferFrom(matcher.group(8));
            record.setUserAgent(matcher.group(9));
            return record;
//            for (int i=0; i<matcher.groupCount();i++){
//                System.out.println(matcher.group(i+1));
//            }
        }

    }

//    public static void main(String[] args) {
////        String pat="((?:[^\"]|\\\"){1,760})";
////        Pattern pattern = Pattern.compile(pat);
////        Matcher matcher = pattern.matcher("GET /wp-admin/load-scripts.php?c=1&load%5B%5D=hoverIntent,common,admin-bar,heartbeat,autosave,wp-ajax-response,jquery-color,wp-lists,quicktags,jquery-query,admin-comments,sug&load%5B%5D=gest,jquery-ui-widget,jquery-ui-mouse,jquery-ui-sortable,postbox,tags-box,underscore,word-count,wp-a11y,post,editor-expand,thick&load%5B%5D=box,shortcode,backbone,wp-util,wp-backbone,media-models,wp-plupload,mediaelement,wp-mediaelement,media-views,media-editor,media-&load%5B%5D=audiovideo,mce-view,imgareaselect,image-edit,svg-painter,wp-auth-check,jquery-ui-tabs,jquery-ui-draggable,jquery-ui-slider,jquer&load%5B%5D=y-touch-punch,iris,wp-color-picker,media-upload,editor,wplink,jquery-ui-position,jquery-ui-menu,jquery-ui-autocomplete&ver=4.5.4 HTTP/1.1");
////        System.out.println(matcher.matches());
//        System.out.println("GET http://www.izhongsai.comhttphttphttphttphttphttphttphttphttphttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttphttphttphttphttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttphttphttphttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttphttphttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttphttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttp/www.izhongsai.comhttphttphttphttp/www.izhongsai.comhttphttphttp/www.izhongsai.comhttphttp/www.izhongsai.comhttp/www.izhongsai.com/ HTTP/1.1".length());
//        String example = "123.56.64.43 - - [21/Sep/2016:09:33:54 +0800] \"GET http://www.izhongsai.comhttphttphttphttphttphttphttphttphttphttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttphttphttphttphttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttphttphttphttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttphttphttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttphttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttphttp/www.izhongsai.comhttphttphttphttphttp/www.izhongsai.comhttphttphttphttp/www.izhongsai.comhttphttphttp/www.izhongsai.comhttphttp/www.izhongsai.comhttp/www.izhongsai.com/ HTTP/1.1\" 301 - \"-\" \"Mozilla/5.0 (compatible; MSIE 9.0; AOL 9.0; Windows NT 6.0; Trident/5.0)\"";
//        System.out.println(LogAnalyser.read(example));
//    }
}
