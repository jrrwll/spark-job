package org.dreamcat.job.spark;

import static org.apache.spark.sql.functions.asc_nulls_last;
import static org.apache.spark.sql.functions.desc_nulls_last;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @author Jerry Will
 * @version 2022-09-13
 */
public class Main {

    static final SparkSession spark = SparkSession.builder()
            .config("spark.master", "local")
            .appName("example")
            .getOrCreate();

    public static void main(String[] args) throws Exception {
        UDFRegistration udfReg = spark.udf();
        udfReg.register("toJson", s -> null, DataTypes.StringType);

        /// read & union

        // union by positions of fields
        Dataset<Row> t1 = t1().union(t1());
        // union by names of fields
        Dataset<Row> t2 = t2().unionByName(t2(), true).distinct();

        /// join

        Column col = t1.col("name").$eq$eq$eq(t2.col("student_name"));
        Dataset<Row> t3 = t1.join(t2, col, "inner");
        t3.printSchema();

        /// map & filter

        Dataset<Row> t4 = t3.drop("student_name"); // drop column
        // add or modify column
        Dataset<Row> t5 = t4.withColumn(
                "bmi", expr("weight / pow(height / 100, 2)"));
        JavaRDD<Row> t5JavaRDD = t5.javaRDD()
                .map(a -> a)
                .filter(a -> true); // t5JavaRDD.collect();
        Dataset<Row> t6 = spark.createDataFrame(t5JavaRDD, t5.schema());
        t6.printSchema();

        /// group
        Dataset<Row> t7 = t6.groupBy("date", "name")
                .agg(expr("avg(bmi) as bmi_avg"),
                        expr("max(height) as height_max"),
                        expr("first(score) as score_first"))
                .orderBy(asc_nulls_last("date"),
                        desc_nulls_last("height_max"))
                .withColumn("index", monotonically_increasing_id())
                .limit(100);
        t7.show();

        /// sample
        Dataset<Row> s1 = t7.sample(0.25);
        Dataset<Row> s2 = t7.sample(0.1, System.currentTimeMillis());
        s1.union(s2).stat().freqItems(new String[]{"height_max"}).javaRDD().foreach(row ->
                System.out.println(Arrays.toString(toArray(row))));

        /// async

        JavaRDD<Row> t7JavaRDD = t7.javaRDD();
        JavaFutureAction<List<Row>> future = t7JavaRDD.collectAsync();
        Thread thread = new Thread(() -> {
            try {
                List<Row> rows = future.get();
                rows.forEach(row -> System.out.println(Arrays.toString(toArray(row))));
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();
        thread.join();
        spark.close();
    }

    public static Dataset<Row> t1() {
        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "root");
        return spark.read().jdbc(
                "jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true",
                "student", props);
    }

    public static Dataset<Row> t2() {
        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "root");
        return spark.read().jdbc(
                "jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=UTC",
                "score", props);
    }

    public static Object[] toArray(Row row) {
        int n = row.size();
        Object[] a = new Object[n];
        for (int i = 0; i < n; i++) {
            a[i] = row.get(i);
        }
        return a;
    }

    public void s() {
        RowFactory.create();

        List<StructField> fields = new ArrayList<>();
        StructField field = DataTypes.createStructField(
                "url", DataTypes.StringType, true);
        fields.add(field);
        StructType schema = DataTypes.createStructType(fields);
    }
}
