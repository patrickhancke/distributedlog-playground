package be.patrickhancke.distributedlog;

import java.time.ZonedDateTime;

class Settings {

    static class DLog {
        static String NAMESPACE = "patrick-test1";
        static String URI = "distributedlog://127.0.0.1:2181/" + NAMESPACE;
        static String logName() {
            ZonedDateTime now = ZonedDateTime.now();
            return String.format("log-%d-%d-%d", now.getYear(), now.getMonthValue(), now.getDayOfMonth());
        }
    }
}
