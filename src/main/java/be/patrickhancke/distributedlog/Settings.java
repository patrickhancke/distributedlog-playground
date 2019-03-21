package be.patrickhancke.distributedlog;

import java.time.ZonedDateTime;

class Settings {
    interface App {
        int NUMBER_OF_LOGS = 300;
    }

    static class DLog {
        final static String NAMESPACE = "testcase";
        final static String URI = "distributedlog://localhost:2181/" + NAMESPACE;

        static String logName(int sequenceNumber) {
            ZonedDateTime now = ZonedDateTime.now();
            return String.format("log-%d-%d-%d-%d", now.getYear(), now.getMonthValue(), now.getDayOfMonth(), sequenceNumber);
        }
    }
}
