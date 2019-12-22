public class App {

    public static void main(String[] args) {
        KafkaUtils.waitForKafka();
        FraudTxsPredictor fraudTxsPredictor = new FraudTxsPredictor();
        fraudTxsPredictor.run();
    }
}