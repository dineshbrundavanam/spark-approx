package com.walmart.dataplatform.approximation;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.spark.sql.SparkSession;
import org.verdictdb.VerdictContext;
import org.verdictdb.VerdictSingleResult;
import org.verdictdb.exception.VerdictDBException;
import java.lang.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
/**
 * Hello world!
 *
 */
public class Driver {
	private SparkSession getSparkSession() {
		SparkSession spark = SparkSession
				.builder()
				.appName("Connecting with Hive")
				.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
				.enableHiveSupport()
				.getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		return spark;
	}

	public static void main(String[] args) throws VerdictDBException, IOException {
		System.out.println("Starting");

			new Driver().createScramble(args[0]);

			new Driver().createResultsTable();
			for(int i=1;i<=3;i++){
				String query = new Driver().giveQuery(i);
				System.out.println(query);
				long hive_time = new Driver().hiveExample(query);
				long verdict_time = new Driver().verdictExample(query);
				new Driver().storeResults(i,hive_time,verdict_time);
			}
		

		System.out.println("Complete");
	}

	public long verdictExample(String query) throws VerdictDBException {
		VerdictContext verdict = VerdictContext.fromSparkSession(getSparkSession());

		System.out.println("Running Verdict query");
		long start = System.currentTimeMillis();
		VerdictSingleResult vrd = verdict.sql(query);
		long end = System.currentTimeMillis();
		vrd.printCsv();
		long time_taken = (end - start)/1000;
		System.out.println("VerdictDB query took " + time_taken + " s");
		return time_taken;
	}

	public long hiveExample(String query) {
		SparkSession spark = getSparkSession();
		System.out.println("Running Hive query");
		long start = System.currentTimeMillis();
		Dataset<Row> result = spark.sql(query);
		long end = System.currentTimeMillis();
		result.show();
		long time_taken = (end - start)/1000;
		System.out.println("Hive query took " + time_taken + " s");
		return time_taken;
	}

	public void createScramble(String size) throws VerdictDBException {
		VerdictContext verdict = VerdictContext.fromSparkSession(getSparkSession());

		System.out.println("Creating Scramble tables");

		//creating scrambles for the new tables

		long start = System.currentTimeMillis();

		verdict.sql("BYPASS DROP TABLE IF EXISTS d0k02ll2.customer_scramble");
		verdict.sql("CREATE SCRAMBLE d0k02ll2.customer_scramble FROM d0k02ll2.customer size " + size);

		verdict.sql("BYPASS DROP TABLE IF EXISTS d0k02ll2.lineitem_scramble");
		verdict.sql("CREATE SCRAMBLE d0k02ll2.lineitem_scramble FROM d0k02ll2.lineitem size " + size);

		verdict.sql("BYPASS DROP TABLE IF EXISTS d0k02ll2.nation_scramble");
		verdict.sql("CREATE SCRAMBLE d0k02ll2.nation_scramble FROM d0k02ll2.nation size " + size);

		verdict.sql("BYPASS DROP TABLE IF EXISTS d0k02ll2.orders_scramble");
		verdict.sql("CREATE SCRAMBLE d0k02ll2.orders_scramble FROM d0k02ll2.orders size " + size);

		verdict.sql("BYPASS DROP TABLE IF EXISTS d0k02ll2.part_scramble");
		verdict.sql("CREATE SCRAMBLE d0k02ll2.part_scramble FROM d0k02ll2.part size " + size);

		verdict.sql("BYPASS DROP TABLE IF EXISTS d0k02ll2.partsupp_scramble");
		verdict.sql("CREATE SCRAMBLE d0k02ll2.partsupp_scramble FROM d0k02ll2.partsupp size " + size);

		verdict.sql("BYPASS DROP TABLE IF EXISTS d0k02ll2.region_scramble");
		verdict.sql("CREATE SCRAMBLE d0k02ll2.region_scramble FROM d0k02ll2.region size " + size);

		verdict.sql("BYPASS DROP TABLE IF EXISTS d0k02ll2.supplier_scramble");
		verdict.sql("CREATE SCRAMBLE d0k02ll2.supplier_scramble FROM d0k02ll2.supplier size " + size);

		long end = System.currentTimeMillis();
		long time_taken = (end - start)/1000;

		new Driver().createScrambleResults(time_taken);

		System.out.println("Creating Scramble tables Completed");
	}

	public String giveQuery(int i) throws IOException {
		String filename = "/sql/"+i+".sql";
		InputStream is = Driver.class.getResourceAsStream(filename);
		InputStreamReader isReader = new InputStreamReader(is);
		BufferedReader reader = new BufferedReader(isReader);
		StringBuffer sb = new StringBuffer();
		String str;
		while((str = reader.readLine())!= null){
			sb.append(str);
			sb.append(" ");
		}
		String final_result = sb.toString();
		return final_result;
	}

	public void storeResults(int num, long hive_time, long verdict_time){
		SparkSession spark = getSparkSession();
		spark.sql("INSERT INTO TABLE d0k02ll2.results VALUES (" + num + "," + hive_time + "," + verdict_time + " )");
	}
	public void createResultsTable(){
		SparkSession spark = getSparkSession();
		spark.sql("DROP TABLE IF EXISTS d0k02ll2.results");
		spark.sql("CREATE TABLE d0k02ll2.results (sql int, hive_time bigint, verdict_time bigint)");
	}
	public void createScrambleResults(long scramble_time){
		SparkSession spark = getSparkSession();
		spark.sql("DROP TABLE IF EXISTS d0k02ll2.sresults");
		spark.sql("CREATE TABLE d0k02ll2.sresults (scramble_time bigint)");
		spark.sql("INSERT INTO TABLE d0k02ll2.sresults VALUES (" + scramble_time + " )" );
	}

}
