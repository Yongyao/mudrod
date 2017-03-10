package esiptestbed.mudrod.streaming;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class Session implements Serializable{

  public Session() {
    // TODO Auto-generated constructor stub
  }

  private String start_time;
  private String end_time;
  private String ip;
  private List<ApacheAccessLog> logList = new ArrayList<ApacheAccessLog>();

  //private DateTimeFormatter formatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z");

  public Session(ApacheAccessLog log)
  {
    this.start_time = log.getDateTimeString();
    this.end_time = log.getDateTimeString();
    this.ip = log.getIpAddress();
    this.logList.add(log);
  }

  public Session(String start_time, String end_time, String ip, List<ApacheAccessLog> list)
  {
    this.start_time = start_time;
    this.end_time = end_time;
    this.ip = ip;
    this.logList = list;
  }

  public String getStartTime()
  {
    return start_time;
  }

  public String getEndTime()
  {
    return end_time;
  }

  public String getIpAddress()
  {
    return ip;
  }

  public List<ApacheAccessLog> getLogList()
  {
    return logList;
  }

  public void setStartTime(String time)
  {
    start_time = time;
  }

  public void setEndTime(String time)
  {
    end_time = time;
  }

  public void setIpAddress(String ipString)
  {
    ip = ipString;
  }

  //  public Session add(Session s)
  //  {
  //    DateTimeFormatter formatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z");
  //    DateTime new_start_time = formatter.parseDateTime(s.getStartTime()); 
  //    DateTime new_end_time = formatter.parseDateTime(s.getEndTime());
  //    List<ApacheAccessLog> new_logList = new ArrayList<ApacheAccessLog>();
  //    
  //    if(new_start_time.isAfter(formatter.parseDateTime(this.getStartTime())))
  //    {
  //      new_start_time = formatter.parseDateTime(this.getStartTime());
  //    }
  //    
  //    if(new_end_time.isBefore(formatter.parseDateTime(this.getEndTime())))
  //    {
  //      new_end_time = formatter.parseDateTime(this.getEndTime());
  //    }
  //    
  //    new_logList.addAll(s.getLogList());
  //    new_logList.addAll(this.getLogList());
  //    
  //    return new Session(new_start_time.toString(formatter), new_end_time.toString(formatter), this.getIpAddress(), new_logList);
  //  }

  public static Session add(Session s1, Session s2)
  {
    if(s1 == null && s2!=null)
    {
      return s2;
    }else if (s1 !=null && s2==null)
    {
      return s1;
    }else if (s1 !=null && s2!=null)
    {
      DateTimeFormatter formatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z");
      DateTime new_start_time = formatter.parseDateTime(s1.getStartTime()); 
      DateTime new_end_time = formatter.parseDateTime(s1.getEndTime());
      List<ApacheAccessLog> new_logList = new ArrayList<ApacheAccessLog>();

      if(new_start_time.isAfter(formatter.parseDateTime(s2.getStartTime())))
      {
        new_start_time = formatter.parseDateTime(s2.getStartTime());
      }

      if(new_end_time.isBefore(formatter.parseDateTime(s2.getEndTime())))
      {
        new_end_time = formatter.parseDateTime(s2.getEndTime());
      }

      new_logList.addAll(s1.getLogList());
      new_logList.addAll(s2.getLogList());

      return new Session(new_start_time.toString(formatter), new_end_time.toString(formatter), s2.getIpAddress(), new_logList);
    }
    
    return null;
  }

}
