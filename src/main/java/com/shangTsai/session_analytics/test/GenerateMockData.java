package com.shangTsai.session_analytics.test;

import com.shangTsai.session_analytics.utils.DateUtils;
import com.shangTsai.session_analytics.utils.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;


/**
 * Generate mock data
 * @author shangluntsai
 *
 */
public class GenerateMockData {

	public static void mock(
			JavaSparkContext sc,
			SparkSession session) {
		List<Row> rows = new ArrayList<Row>();
		
		String[] searchKeywords = new String[] {"books", "office", "clothes", "kitchen",
				"index", "Artificial Intelligence", "Hadoop", "Bitcoin", "data minging", "NFL"};
		
		// Mock data is generated based on the date of today
		String date = DateUtils.getTodayDate();
		String[] actions = new String[]{"search", "click", "order", "pay"};
		Random random = new Random();
		
		for(int i = 0; i < 100; i++) {
			long userid = random.nextInt(100);    
			
			for(int j = 0; j < 10; j++) {
				String sessionid = UUID.randomUUID().toString().replace("-", "");  
				String baseActionTime = date + " " + random.nextInt(23);
				  
				for(int k = 0; k < random.nextInt(100); k++) {
					long pageid = random.nextInt(10);    
					String actionTime = baseActionTime + ":" + StringUtils.fillZero(
							String.valueOf(random.nextInt(59))) + ":"
							+ StringUtils.fillZero(String.valueOf(random.nextInt(59)));
					String searchKeyword = null;
					Long clickCategoryId = null;
					Long clickProductId = null;
					String orderCategoryIds = null;
					String orderProductIds = null;
					String payCategoryIds = null;
					String payProductIds = null;
					
					String action = actions[random.nextInt(4)];
					if("search".equals(action)) {
						searchKeyword = searchKeywords[random.nextInt(10)];   
					} else if("click".equals(action)) {
						clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));    
						clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));  
					} else if("order".equals(action)) {
						orderCategoryIds = String.valueOf(random.nextInt(100));  
						orderProductIds = String.valueOf(random.nextInt(100));
					} else if("pay".equals(action)) {
						payCategoryIds = String.valueOf(random.nextInt(100));  
						payProductIds = String.valueOf(random.nextInt(100));
					}
					
					Row row = RowFactory.create(date, userid, sessionid, 
							pageid, actionTime, searchKeyword,
							clickCategoryId, clickProductId,
							orderCategoryIds, orderProductIds,
							payCategoryIds, payProductIds);
					rows.add(row);
				}
			}
		}
		
		JavaRDD<Row> rowsRDD = sc.parallelize(rows);
		
		StructType schema = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("date", DataTypes.StringType, true),
				DataTypes.createStructField("user_id", DataTypes.LongType, true),
				DataTypes.createStructField("session_id", DataTypes.StringType, true),
				DataTypes.createStructField("page_id", DataTypes.LongType, true),
				DataTypes.createStructField("action_time", DataTypes.StringType, true),
				DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
				DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
				DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
				DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
				DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
				DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
				DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true)));
		
		rowsRDD.foreach(x -> System.out.println(x));
		
		Dataset<Row> df = session.createDataFrame(rowsRDD, schema);		
		df.createOrReplaceTempView("user_visit_action");  

		/**
		 * ==================================================================
		 */
		
		rows.clear();
		String[] genders = new String[]{"male", "female"};
		for(int i = 0; i < 100; i ++) {
			long userid = i;
			String username = "user" + i;
			String name = "name" + i;
			int age = random.nextInt(60);
			String occupation = "occupation" + random.nextInt(100);
			String city = "city" + random.nextInt(100);
			String gender = genders[random.nextInt(2)];
			
			Row row = RowFactory.create(userid, username, name, age, 
					occupation, city, gender);
			rows.add(row);
		}
		
		rowsRDD = sc.parallelize(rows);
		
		StructType schema2 = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("user_id", DataTypes.LongType, true),
				DataTypes.createStructField("username", DataTypes.StringType, true),
				DataTypes.createStructField("name", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.IntegerType, true),
				DataTypes.createStructField("occupation", DataTypes.StringType, true),
				DataTypes.createStructField("city", DataTypes.StringType, true),
				DataTypes.createStructField("gender", DataTypes.StringType, true)));
		
		rowsRDD.foreach(x -> System.out.println(x));
		
		Dataset<Row> df2 = session.createDataFrame(rowsRDD, schema2);
		df2.createOrReplaceTempView("user_info");  
	}
	
}
