package esiptestbed.mudrod.weblog.structure;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.gson.Gson;

/**
 * This class represents an Apache access log line. See
 * http://httpd.apache.org/docs/2.2/logs.html for more details.
 */
public class FtpLog extends WebLog implements Serializable {

  public static String parseFromLogLine(String log)
      throws IOException, ParseException {

    String ip = log.split(" +")[6];

    String time = log.split(" +")[1] + ":" + log.split(" +")[2] + ":"
        + log.split(" +")[3] + ":" + log.split(" +")[4];

    time = SwithtoNum(time);
    SimpleDateFormat formatter = new SimpleDateFormat("MM:dd:HH:mm:ss:yyyy");
    Date date = formatter.parse(time);
    String bytes = log.split(" +")[7];

    String request = log.split(" +")[8].toLowerCase();

    if (!request.contains("/misc/") && !request.contains("readme")) {
      FtpLog ftplog = new FtpLog();
      ftplog.LogType = "ftp";
      ftplog.IP = ip;
      ftplog.Request = request;
      ftplog.Bytes = Double.parseDouble(bytes);

      SimpleDateFormat df = new SimpleDateFormat(
          "yyyy-MM-dd'T'HH:mm:ss.sss'Z'");
      ftplog.Time = df.format(date);
      // ftplog.Time = date;

      Gson gson = new Gson();
      String lineJson = gson.toJson(ftplog);

      return lineJson;
    }

    return "{}";
  }

  public static boolean checknull(WebLog s) {
    if (s == null) {
      return false;
    }
    return true;
  }
}
