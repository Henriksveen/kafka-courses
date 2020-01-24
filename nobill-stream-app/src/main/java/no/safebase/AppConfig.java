package no.safebase;

public class AppConfig {

    public static final String APPLICATION_ID = "nobill-extract-step";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String SCHEMA_REGISTRY = "http://127.0.0.1:8081";
    public static final String NOBILL_CALL_RECORD_TOPIC = "nobill-call-record";
    public static final String NOBILL_CALL_RECORD_HOUR_TOPIC = "nobill-call-record-hour";
    public static final String NOBILL_CALL_RECORD_DAY_TOPIC = "nobill-call-record-day";
    public static final String STATE_STORE_HOUR = "state-store-HOUR"; // TODO
    public static final String STATE_STORE_HOUR_CONSUMER = "state-store-HOUR-consumer"; // TODO
    public static final String STATE_STORE_DAY = "state-store-DAY"; // TODO

    public final static String queryServerHost = "localhost";
    public final static int queryServerPort = 7010;
}
