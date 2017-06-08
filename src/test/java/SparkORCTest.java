import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class SparkORCTest {
    Logger logger = LoggerFactory.getLogger(SparkORCTest.class);
    static String warehouseDir = getResourcePath(".");
    static SparkSession session ;

    @BeforeClass
    public static void before() {
        session = SparkSession.builder().enableHiveSupport().
                config("spark.driver.port", 12345).
                config("spark.sql.warehouse.dir", warehouseDir).
                config("spark.master", "local[4]").
                getOrCreate();
    }

    @AfterClass
    public static void after() {
        session.stop();
    }


    @Test
    public void testBugInORC(){
        String json1Path = getResourcePath("json-1.json");
        String json2Path = getResourcePath("json-2.json");
        String orcInput1 = getResourcePath("testBugInORC1");
        String orcInput2 = getResourcePath("testBugInORC2");
        session.read().json(json1Path,json2Path).show();

        session.read().json(json1Path).write().mode("overwrite").orc(orcInput1);
        session.read().json(json2Path).write().mode("overwrite").orc(orcInput2);
        Dataset<Row> ds = session.read().orc(orcInput1);
        ds.show();
        ds = session.read().orc(orcInput2);
        ds.show();
        ds = session.read().orc(orcInput1,orcInput2);
        ds.printSchema();
        ds.show();
    }

    @Test
    public void testParquet(){
        String json1Path = getResourcePath("json-1.json");
        String json2Path = getResourcePath("json-2.json");
        String parquetInput1 = getResourcePath("testParquet1");
        String parquetInput2 = getResourcePath("testParquet2");
        session.read().json(json1Path,json2Path).show();

        session.read().json(json1Path).write().mode("overwrite").parquet(parquetInput1);
        session.read().json(json2Path).write().mode("overwrite").parquet(parquetInput2);
        Dataset<Row> ds = session.read().parquet(parquetInput1);
        ds.show();
        ds = session.read().parquet(parquetInput2);
        ds.show();
        ds = session.read().parquet(parquetInput1,parquetInput2);
        ds.show();
    }

    @Test
    public void testCsv(){
        String json1Path = getResourcePath("json-1.json");
        String json2Path = getResourcePath("json-2.json");
        String csvInput1 = getResourcePath("testCsv1");
        String csvInput2 = getResourcePath("testCsv2");
        session.read().json(json1Path,json2Path).show();

        session.read().json(json1Path).write().mode("overwrite").option("header","true").csv(csvInput1);
        session.read().json(json2Path).write().mode("overwrite").option("header","true").csv(csvInput2);
        Dataset<Row> ds = session.read().option("header","true").csv(csvInput1);
        ds.show();
        ds = session.read().option("header","true").csv(csvInput2);
        ds.show();
        ds = session.read().option("header","true").csv(csvInput1,csvInput2);
        ds.show();
    }

    private static String getResourcePath(String resource) {
        String root = SparkORCTest.class.getClassLoader().getResource(".").getPath();
        File directory = new File(root + File.separator + resource);
        if (! directory.exists()) {
            directory.mkdir();
            // If you require it to make the entire directory path including parents,
            // use directory.mkdirs(); here instead.
        }
        return SparkORCTest.class.getClassLoader().getResource(resource).getPath();
    }
}
