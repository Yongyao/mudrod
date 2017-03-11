package esiptestbed.mudrod.streaming;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class represents an Apache access log line.
 * See http://httpd.apache.org/docs/2.2/logs.html for more details.
 */
public class ApacheAccessLog implements Serializable {
  private static final Logger logger = Logger.getLogger("Access");

  private String ipAddress;
  private String clientIdentd;
  private String userID;
  private String dateTimeString;
  private String request;
  private int responseCode;
  private long contentSize;
  
  private String referrer;
  private String agent;
  
  private final static String[] crawlerList = {"crawler", "bot", "slurp", "perl", "java", "httpclient", "curl"};
  private final static String[] searchRequestList = {"datasetlist", "dataset"};

  private ApacheAccessLog(String ipAddress, String clientIdentd, String userID,
                          String dateTime, String request, String responseCode,
                          String contentSize, String referrer, String agent) {
    this.ipAddress = ipAddress;
    this.clientIdentd = clientIdentd;
    this.userID = userID;
    this.dateTimeString = dateTime;
    this.request = request;
    this.responseCode = Integer.parseInt(responseCode);
    
    if(contentSize.equals("-"))
      contentSize = "0";
    this.contentSize = Long.parseLong(contentSize);
    
    this.referrer = referrer;
    this.agent = agent;
  }

  public String getIpAddress() {
    return ipAddress;
  }

  public String getClientIdentd() {
    return clientIdentd;
  }

  public String getUserID() {
    return userID;
  }

  public String getDateTimeString() {
    return dateTimeString;
  }

  public String getRequest() {
    return request;
  }

  public int getResponseCode() {
    return responseCode;
  }

  public long getContentSize() {
    return contentSize;
  }

  public void setIpAddress(String ipAddress) {
    this.ipAddress = ipAddress;
  }

  public void setClientIdentd(String clientIdentd) {
    this.clientIdentd = clientIdentd;
  }

  public void setUserID(String userID) {
    this.userID = userID;
  }

  public void setDateTimeString(String dateTimeString) {
    this.dateTimeString = dateTimeString;
  }

  public void setRequest(String request) {
    this.request = request;
  }

  public void setResponseCode(int responseCode) {
    this.responseCode = responseCode;
  }

  public void setContentSize(long contentSize) {
    this.contentSize = contentSize;
  }

  public boolean isCrawler()
  {
    for(String c:crawlerList)
    {
      if(agent.toLowerCase().trim().contains(c)||agent.equals("-"))
      {
        return true;
      }
    }
    return false;
  }
  
  public boolean isSearchLog()
  {
    for(String s:searchRequestList)
    {
      if(request.toLowerCase().trim().contains(s))
      {
        return true;
      }
    }
    return false;
  }

  // Example Apache log line:
  //   127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
  private static final String LOG_ENTRY_PATTERN =
      // 1:IP  2:client 3:user 4:date time                   5:request   6:respcode 7:size 8:referrer 9:agent
      "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+|-) \"((?:[^\"]|\")+)\" \"([^\"]+)\"";
  private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

  public static ApacheAccessLog parseFromLogLine(String logline) {
    Matcher m = PATTERN.matcher(logline);
    if (!m.find()) {
      logger.log(Level.ALL, "Cannot parse logline{0}", logline);
      throw new RuntimeException("Error parsing logline");
    }

    return new ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4),
        m.group(5), m.group(6), m.group(7), m.group(8), m.group(9));
  }

  @Override public String toString() {
    return String.format("%s %s %s [%s] \"%s %s %s\" %s %s \"%s\" \"%s\"",
        ipAddress, clientIdentd, userID, dateTimeString, request, responseCode, contentSize, referrer, agent);
  }
}
