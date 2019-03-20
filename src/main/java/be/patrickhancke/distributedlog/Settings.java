package be.patrickhancke.distributedlog;

import java.time.ZonedDateTime;

class Settings {

    static class DLog {
        final static String NAMESPACE = "testcase";
        final static String URI = "distributedlog://localhost:2181/" + NAMESPACE;
        final static int NUMBER_OF_LOGS = 5;

        static String logName(int sequenceNumber) {
            ZonedDateTime now = ZonedDateTime.now();
            return String.format("log-%d-%d-%d-%d", now.getYear(), now.getMonthValue(), now.getDayOfMonth(), sequenceNumber);
        }
    }
}
