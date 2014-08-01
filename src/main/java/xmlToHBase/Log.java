package xmlToHBase;

public class Log {

    private static final boolean DEBUG = false;

    public static void log(String msg) {
        if (DEBUG) {
            System.out.println(msg);
        }
    }

    public static void log(Throwable ex) {
        if (DEBUG) {
            ex.printStackTrace();
        }
    }
}
