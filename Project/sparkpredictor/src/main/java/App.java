import java.util.regex.Pattern;

public class App {

    public static void main(String[] args) {
        FraudTxsProducer producer = new FraudTxsProducer();
        producer.run();

        TxsConsumer consumer = new TxsConsumer();
        consumer.run();


//        SparkSession spark = SparkSession
//                .builder()
//                .appName("JavaSparkPi")
//                .getOrCreate();
//
//        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//
//        int slices = (args.length == 1) ? 555432 : 2;
//        int n = 100000 * slices;
//        List<Integer> l = new ArrayList<>(n);
//        for (int i = 0; i < n; i++) {
//            l.add(i);
//        }
//
//        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);
//
//        int count = dataSet.map(integer -> {
//            double x = Math.random() * 2 - 1;
//            double y = Math.random() * 2 - 1;
//            return (x * x + y * y <= 1) ? 1 : 0;
//        }).reduce((integer, integer2) -> integer + integer2);
//
//        System.out.println("Pi is roughly " + 4.0 * count / n);
//
//        spark.stop();
    }
}