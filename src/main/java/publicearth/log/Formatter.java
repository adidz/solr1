package publicearth.log;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.LogRecord;

public class Formatter extends java.util.logging.Formatter {

    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");

    @Override
    public String format(LogRecord logRecord) {
        StringBuffer out = new StringBuffer();
        out.append(logRecord.getLevel());
        out.append(" - ");
        out.append(dateFormat.format(new Date(logRecord.getMillis())));
        out.append(": ");
        out.append(logRecord.getMessage());
        out.append("\n");
        return out.toString();
    }
}
