package be.patrickhancke.distributedlog;

import java.time.ZonedDateTime;

class Settings {

    static class DLog {
        static String NAMESPACE = "testcase";
        static String URI = "distributedlog://localhost:2181/" + NAMESPACE;
        static String logName() {
            ZonedDateTime now = ZonedDateTime.now();
            return String.format("log-%d-%d-%d", now.getYear(), now.getMonthValue(), now.getDayOfMonth());
        }
    }
}
