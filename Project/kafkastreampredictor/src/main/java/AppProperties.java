import java.io.IOException;
import java.util.Properties;

public final class AppProperties {

    private static final Properties properties = new Properties();

    static {
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            properties.load(loader.getResourceAsStream("config.properties"));
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    public static double getPropertyAsDouble(String key) {
        return Double.parseDouble(properties.getProperty(key));
    }

}
