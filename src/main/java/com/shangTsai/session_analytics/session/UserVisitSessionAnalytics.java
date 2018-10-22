package com.shangTsai.session_analytics.session;
/**
 * Users visit session analytics by Spark
 * 
 * Received analytic tasks created by users, parameters from users:
 * 1. Range of dates: Start_date, End_date
 * 2. Gender: male, female
 * 3. Age
 * 4. Occupation (multiple)
 * 5. Location: city (multiple)
 * 6. Key words (multiple)
 * 7. Clicked Categories
 * 
 * @author shangluntsai
 */

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.shangTsai.session_analytics.constants.Constants;
import com.shangTsai.session_analytics.dao.ISessionAggrStatDAO;
import com.shangTsai.session_analytics.dao.ISessionDetailsDAO;
import com.shangTsai.session_analytics.dao.ISessionRandomSamplingDAO;
import com.shangTsai.session_analytics.dao.ITaskDAO;
import com.shangTsai.session_analytics.dao.ITop10CategoryDAO;
import com.shangTsai.session_analytics.dao.ITop10SessionDAO;
import com.shangTsai.session_analytics.dao.impl.DAOFactory;
import com.shangTsai.session_analytics.domain.SessionAggrStat;
import com.shangTsai.session_analytics.domain.SessionDetails;
import com.shangTsai.session_analytics.domain.SessionRandomSampling;
import com.shangTsai.session_analytics.domain.Task;
import com.shangTsai.session_analytics.domain.Top10Category;
import com.shangTsai.session_analytics.domain.Top10Session;
import com.shangTsai.session_analytics.utils.DateUtils;
import com.shangTsai.session_analytics.utils.NumberUtils;
import com.shangTsai.session_analytics.utils.ParamUtils;
import com.shangTsai.session_analytics.utils.SparkUtils;
import com.shangTsai.session_analytics.utils.StringUtils;
import com.shangTsai.session_analytics.utils.ValidUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

@SuppressWarnings("deprecation")
public class UserVisitSessionAnalytics {

	private static JsonParser jsonParser = new JsonParser();
	
	public static void main(String[] args) throws Exception {
		
		SparkConf conf = new SparkConf()
				.setAppName(Constants.APP_USER_SESSION_ANALYTICS)
				.setMaster("local[*]")
//				.set("spark.default.parallelism", "400")          
				.set("spark.locality.wait", "4")
				.set("spark.storage.memoryFraction", "0.5")
				.set("spark.shuffle.manager", "tungsten-sort")
				.set("spark.shuffle.file.buffer", "64")
				.set("spark.shuffle.memoryFraction", "0.3")
				.set("spark.shuffle.consolidateFiles", "true")
				.set("spark.serializer", "org.apache.spark.serializer.kryoSerializer")
				.registerKryoClasses(new Class[] {CategorySortingKey.class});  
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		SparkSession session = SparkSession.builder().getOrCreate();
		// Get mock data for local test
		SparkUtils.mockData(sc, session);
		
		// Create JDBC components
		ITaskDAO taskDAO = DAOFactory.getTaskDAO(); 
		
		// Get parameters of task
		long task_id = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
		Task task = taskDAO.findById(task_id);

		if(task == null) {
			System.out.println(new Date() + ": cannot find this task with ID [" + task_id + "].");
			return;
		}
		
		JsonObject taskParam = jsonParser.parse(task.getTaskParam()).getAsJsonObject();
		
		// Query by date from table user_visit_action
		JavaRDD<Row> actionDateRangeRDD = SparkUtils.getActionRDDByDateRange(session, taskParam);
		
		JavaPairRDD<String, Row> sessionId2ActionRDD = getSessionId2ActionRDD(actionDateRangeRDD);
		sessionId2ActionRDD = sessionId2ActionRDD.persist(StorageLevel.MEMORY_ONLY());
		
		// Aggregate actionDateRangeRDD by session_id, get session granularity data
		JavaPairRDD<String, String> sessionId2FullUserInfoRDD = 
				aggregateBySession(session, sessionId2ActionRDD);
		
		// Filter out data by user selected parameters
		Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
				"", new SessionAggrStatAccumulator());
		
		JavaPairRDD<String, String> filteredSessionid2AggrStatRDD = filterSessionAggrStat(
				sessionId2FullUserInfoRDD, taskParam, sessionAggrStatAccumulator);
		
		filteredSessionid2AggrStatRDD = 
				filteredSessionid2AggrStatRDD.persist(StorageLevel.MEMORY_ONLY());
		
		
		// Get session details RDD from filtered sessions
		JavaPairRDD<String, Row> sessionId2DetailsRDD = getSessionId2DetailsRDD(
				filteredSessionid2AggrStatRDD, sessionId2ActionRDD);
		
		// Set persist storage for RDD reuse
		sessionId2DetailsRDD = sessionId2DetailsRDD.persist(StorageLevel.MEMORY_ONLY());
		
		// Stratified sampling daily sessions 
		stratifiedSamplingSessions(sc, task.getTaskId(), 
				filteredSessionid2AggrStatRDD, sessionId2DetailsRDD);
		
		// Calculate aggregated portion and output to MySQL
	    calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskId());
	    
	    
	    // Get top-10 popular products
	    List<Tuple2<CategorySortingKey, String>> top10categoryList = 
	    		getTop10Categories(task.getTaskId(), sessionId2DetailsRDD);
	    
	    
	    // Get top-10 most active sessions
	    getTop10Sessions(sc, task.getTaskId(), top10categoryList, sessionId2DetailsRDD);
		
		// Close SparkSession
		session.close();
	}


	// -----------------------------------------------------------------------------------------.
	
	
	/**
	 * Get a <session_id, actionDateRange>RDD
	 * @param actionDateRangeRDD
	 * @return
	 */
	
	private static JavaPairRDD<String, Row> getSessionId2ActionRDD(
			JavaRDD<Row> actionDateRangeRDD) {
//		return actionDateRangeRDD.mapToPair(
//				(PairFunction<Row, String, Row>) row -> {
//					return new Tuple2<String, Row>(row.getString(2), row);
//				});
		
		return actionDateRangeRDD.mapPartitionsToPair(
				(PairFlatMapFunction<Iterator<Row>, String, Row>) iterator -> {
					List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
					
					while(iterator.hasNext()) {
						Row row = iterator.next();
						list.add(new Tuple2<String, Row>(row.getString(2), row));
					}
					
					return (Iterator<Tuple2<String, Row>>) list;
				});
	}
	
	
	// -----------------------------------------------------------------------------------------.
	
	
	/**
	 * Sampling daily sessions via Stratified Sampling after 
	 * filtering out uncompleted sessions
	 * @param sessionid2AggrInfoRDD (filtered)
	 */
	private static void stratifiedSamplingSessions(
			JavaSparkContext sc, 
			final long task_id,
			JavaPairRDD<String, String> filteredSessionid2AggrStatRDD,
			JavaPairRDD<String, Row> sessionId2DetailsRDD) {
		
		/**
		 * Step1: get RDD in format of <yyyy-mm-dd_HH, aggrinfo>
		 */
		//                          (yyyy-mm-dd_HH, aggrInfo)
		JavaPairRDD<String, String> dateHourSessionId2AggrInfoRDD = 
				filteredSessionid2AggrStatRDD.mapToPair(
						
					(PairFunction<Tuple2<String, String>, String, String>)
					(Tuple2<String, String> tuple) -> {
						String aggrInfo = tuple._2;
						String startTime = StringUtils.getFieldFromConcatString(
								aggrInfo, "\\|", Constants.FIELD_START_TIME);
						String dateHour = DateUtils.getDateHour(startTime);
						return new Tuple2<String, String>(dateHour, aggrInfo);
				});
		
		// get session counts of each hour (yyyy-mm-dd_HH, count)
		final Map<String, Long> countMap = dateHourSessionId2AggrInfoRDD.countByKey();
		
		/**
		 * Step2: get Index for sampling sessions
		 */
		// Transform (yyyy-mm-dd_HH, count) format into (yyyy-mm-dd, <HH, count>) format
		final Map<String, Map<String, Long>> dateHourCountMap = 
				new HashMap<String, Map<String, Long>>();
		
		for(Map.Entry<String, Long> entry : countMap.entrySet()) {
			
			String dateHour = entry.getKey();
			String date = dateHour.split("_")[0];
			String hour = dateHour.split("_")[1];
			Long count = entry.getValue();
			
			Map<String, Long> hourCountMap = dateHourCountMap.get(date);
			if(hourCountMap == null) {      // if empty, then create
				hourCountMap = new HashMap<String, Long>(); // (hour, count)
				dateHourCountMap.put(date, hourCountMap);   // (yyyy-mm-dd_HH, (hour, count))
			}
			// Build up hourCountMap
			hourCountMap.put(hour, count);  
		}
		
		
		/**
		 * implement Stratified Sampling from daily session data
		 */
		//                        100 / number_of_days
		long dailySamplingSize = (100 / dateHourCountMap.size());  
		
		final Map<String, Map<String, List<Long>>> dateHourSamplingMap =
				new HashMap<String, Map<String, List<Long>>>();  //<date, <hour, (20, 30, 50...)>>
		
		Random random = new Random();
		
		for(Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
			
			String date = dateHourCountEntry.getKey();
			Map<String, Long> hourCountMap = dateHourCountEntry.getValue();
					
			long dailySessionCount = 0L;
			for(long count : hourCountMap.values()) {
				dailySessionCount += count;   // get session count of a day
			}
			
			Map<String, List<Long>> hourSamplingMap = dateHourSamplingMap.get(date);
			
			if(hourSamplingMap == null) {    // if empty, then create
				hourSamplingMap = new HashMap<String, List<Long>>();
				dateHourSamplingMap.put(date, hourSamplingMap);
			}
			
			// iterate hourly data
			for(Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
				String hour = hourCountEntry.getKey();
				long hourlySessionCount = hourCountEntry.getValue();
				
				// get hourly sampling size => hour-day-portion * dailySamplingSize
				long hourlySamplingSize = 
						(long) ((double)hourlySessionCount / (double)dailySessionCount * dailySamplingSize);
				
				if(hourlySamplingSize > hourlySessionCount) {
					hourlySamplingSize = hourlySessionCount;
				}
				
				// Empty list of sampling index of each hour
				List<Long> samplingIndexList = hourSamplingMap.get(hour);
				if(samplingIndexList == null) {    
					samplingIndexList = new ArrayList<Long>();
         		    hourSamplingMap.put(hour, samplingIndexList); 
				}
				
				// Build up sampling index of each hour
				for(int i = 0; i < hourlySamplingSize; ++i) {
					long samplingIndex = random.nextInt((int) hourlySessionCount);  
					// avoid repeated index
					while(samplingIndexList.contains(samplingIndex)) {
						samplingIndex = random.nextInt((int) hourlySessionCount);
					}
					
					samplingIndexList.add(samplingIndex);
				}	
			}
		}
		
		
		// broadcast variable
		final Broadcast<Map<String, Map<String, List<Long>>>> dateHourSamplingMapBradcast = 
				sc.broadcast(dateHourSamplingMap);
		
		
		/**
		 * Step3: for daily session, iterate hourly sessions, randomly sampling according to index 
		 */
		//   get an (yyyy-mm-dd_HH, <session aggrInfo>)RDD                   
		JavaPairRDD<String, Iterable<String>> dateHourSessionGroupRDD = 
				dateHourSessionId2AggrInfoRDD.groupByKey();
		
		// get a RDD of session_id which matches sampling indices
		JavaPairRDD<String, String> samplingSessionIdsRDD = dateHourSessionGroupRDD.flatMapToPair(
				
				(PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>)
				(Tuple2<String, Iterable<String>> tuple) -> {
				 
					List<Tuple2<String, String>> samplingSessionIds = 
							new ArrayList<Tuple2<String, String>>();
					
					String dateHour = tuple._1;
					String date = dateHour.split("_")[0];
					String hour = dateHour.split("_")[1];
					Iterator<String> iterator = tuple._2.iterator();
					
					// get value of broadcast variable
					Map<String, Map<String, List<Long>>> broadcastSamplingMap = 
							dateHourSamplingMapBradcast.value();
					
					// get sampling index of a certain hour data
					List<Long> samplingIndexList = broadcastSamplingMap.get(date).get(hour); 
					
					ISessionRandomSamplingDAO sessionRandomSamplingDAO = 
							DAOFactory.getSessionRandomSamplingDAO();
					
					int index = 0;
					while(iterator.hasNext()) {
						String sessionAggrInfo = iterator.next();
						
						if(samplingIndexList.contains(index)) {
							String session_id = StringUtils.getFieldFromConcatString(
									sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
							
							// output data to MySQL
							SessionRandomSampling sessionRandomSampling = new SessionRandomSampling();
							
							sessionRandomSampling.setSessionid(session_id);
							sessionRandomSampling.setTaskid(task_id);
							sessionRandomSampling.setStartTime(StringUtils.getFieldFromConcatString(
									sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
							sessionRandomSampling.setSearchKeywords(StringUtils.getFieldFromConcatString(
									sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
							sessionRandomSampling.setClickCategoryIds(StringUtils.getFieldFromConcatString(
									sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
							
							sessionRandomSamplingDAO.insert(sessionRandomSampling);
							// add session_id into list
							samplingSessionIds.add(new Tuple2<String, String>(session_id, session_id));
						}
						
						index++;
					}
					
					return  (Iterator<Tuple2<String, String>>) samplingSessionIds; 
				});
		
		
		/**
		 * Step4: Obtain details of sampled session
		 */
		JavaPairRDD<String, Tuple2<String, Row>> samplingSessionDetailsRDD = 
				samplingSessionIdsRDD.join(sessionId2DetailsRDD);
		
		samplingSessionDetailsRDD.foreach(
				
				(VoidFunction<Tuple2<String, Tuple2<String, Row>>>)
				(Tuple2<String, Tuple2<String, Row>> tuple) -> {
					Row row = tuple._2._2;
					SessionDetails sessionDetails = new SessionDetails();
					sessionDetails.setTaskId(task_id);
					sessionDetails.setUserId(row.getLong(1));
					sessionDetails.setSessionId(row.getString(2));
					sessionDetails.setPageId(row.getLong(3));
					sessionDetails.setActionTime(row.getString(4));
					sessionDetails.setSearchKeyword(row.getString(5));
					sessionDetails.setClickCategoryId(row.getLong(6));
					sessionDetails.setClickProductId(row.getLong(7));
					sessionDetails.setOrderCategoryIds(row.getString(8));
					sessionDetails.setOrderProductIds(row.getString(9));
					sessionDetails.setPayCategoryIds(row.getString(10));
					sessionDetails.setPayProductIds(row.getString(11));
					
					ISessionDetailsDAO sessionDetailsDAO = DAOFactory.getSessionDetailsDAO();
					sessionDetailsDAO.insert(sessionDetails);
					
				});
		
	}	
	

	// -----------------------------------------------------------------------------------------.
	

	/**
	 * Aggregate action data by session granularity
	 * Each row represents a record of user visit, e.g. a click, or a search
	 * @param actionRDD
	 * @return 
	 */
	private static JavaPairRDD<String, String> aggregateBySession(
			SparkSession session, 
			JavaPairRDD<String, Row> sessionId2ActionRDD){
		
		JavaPairRDD<String, Iterable<Row>> sessionGroupActionsRDD = 
				sessionId2ActionRDD.groupByKey();  // <session_id, searchKword, clickCategory>
		
        // (Actions groupBy session_id) map to user_id relates to keywords and categories searched/clicked by user 	
		JavaPairRDD<Long, String> userIdPartialAggretatedRDD = sessionGroupActionsRDD.mapToPair(
				(PairFunction<Tuple2<String, Iterable<Row>>, Long, String>) 
				(Tuple2<String, Iterable<Row>> tuple) -> {
						String session_id = tuple._1;
						Iterator<Row> iterator = tuple._2.iterator();
						
						StringBuffer searchKeywordsBuffer = new StringBuffer("");
						StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
						
						Long user_id = null;
						
						//time of start and end of a session
						Date startTime = null;
						Date endTime = null;
						
						//step length of a session
						int stepLength = 0;
						
						// iterate all visits in a session
						while(iterator.hasNext()) {
							// extract keywords and categories in a visit 
							Row row = iterator.next();
							// get user_id if the field is null
							if(user_id == null) {
								user_id = row.getLong(1);
							}
							
							String searchKeyword = row.getString(5);
							Long clickCategoryId = row.getLong(6);
							// filter out null and repeated searchKeyword
							if(StringUtils.isNotEmpty(searchKeyword)) {
								if(!searchKeywordsBuffer.toString().contains(searchKeyword)) {
									searchKeywordsBuffer.append(searchKeyword + ",");
								}
							}
							// filter out null or repeated clickCategoryId
							if(clickCategoryId != null) {
								if(!clickCategoryIdsBuffer.toString().contains(
										String.valueOf(clickCategoryId))) {
									clickCategoryIdsBuffer.append(clickCategoryId + ",");
								}
							}
						
							// calculate the time duration of a session
							Date actionTime = DateUtils.parseTime(row.getString(4));
							if(startTime == null) {
								startTime = actionTime;
							}
							if(endTime == null) {
								endTime = actionTime;
							}
							
							if(actionTime.before(startTime)) {
								startTime = actionTime;
							}
							if(actionTime.after(endTime)) {
								endTime = actionTime;
							}
							//increment step_length of session
							stepLength ++;
						}
						// trim down trailing comma if any
						String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
						String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
						
						// calculate the duration of a session in second(s)
						long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
						
						String partialAggrInfo = 
								Constants.FIELD_SESSION_ID + ":" + session_id + "|"
								+ Constants.FIELD_SEARCH_KEYWORDS + ":" + searchKeywords + "|"
								+ Constants.FIELD_CLICK_CATEGORY_IDS + ":" + clickCategoryIds + "|"
								+ Constants.FIELD_VISIT_LENGTH + ":" + visitLength + "|"
								+ Constants.FIELD_STEP_LENGTH + ":" + stepLength + "|"
								+ Constants.FIELD_START_TIME + ":" + DateUtils.formatDate(startTime);
						
						return new Tuple2<Long, String>(user_id, partialAggrInfo);
				});
		

		String sql = "SELECT * FROM user_info";
		
		// Get RDD from view user_info
		JavaRDD<Row> userInfoRDD = session.sql(sql).javaRDD();
		
		// Map userInfo RDD to Row
		JavaPairRDD<Long, Row> userId2UserInfoRDD = userInfoRDD.mapToPair(
				(PairFunction<Row, Long, Row>) row -> new Tuple2<Long, Row>(row.getLong(0), row));
		
		// Join user_session_data with user_info
		JavaPairRDD<Long, Tuple2<String, Row>> userID2FullInfoRDD = 
				userIdPartialAggretatedRDD.join(userId2UserInfoRDD);
		
		// Concatenate joined data and return data in <session_id, userInfoMapSession> format
		JavaPairRDD<String, String> sessionId2FullUserInfoRDD = userID2FullInfoRDD.mapToPair(
				(PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>) 
				(Tuple2<Long, Tuple2<String, Row>> tuple) -> {
					String partialAggrInfo = tuple._2._1;
					Row userInfoRow = tuple._2._2;
					
					String session_id = StringUtils.getFieldFromConcatString(
							partialAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
					
					int age = userInfoRow.getInt(3);
					String occupation = userInfoRow.getString(4);
					String city = userInfoRow.getString(5);
					String gender = userInfoRow.getString(6);
					
					String fullAggrInfo = partialAggrInfo + "|"
							+ Constants.FIELD_AGE + ":" + age + "|"
							+ Constants.FIELD_OCCUPATION + ":" + occupation + "|"
							+ Constants.FIELD_CITY + ":" + city + "|"
							+ Constants.FIELD_GENDER + ":" + gender ;
					
					return new Tuple2<String, String>(session_id, fullAggrInfo);
				});
		
		return sessionId2FullUserInfoRDD;
	}

	
	// -----------------------------------------------------------------------------------------.

	
	/**
	 * Filter out session data
	 * @param sessionid2AggrInfoRDD
	 * @param taskParam
	 * @param sessionAggrAccumulator
	 * @return
	 */
	@SuppressWarnings("deprecation")
	private static JavaPairRDD<String, String> filterSessionAggrStat (
			JavaPairRDD<String, String> sessionId2AggrInfoRDD, 
			final JsonObject taskParam, 
			final Accumulator<String> sessionAggrStatAccumulator) {
				
		String startAge = ParamUtils.getParamFromJson(taskParam, Constants.SELECT_MIN_AGE);
		String endAge = ParamUtils.getParamFromJson(taskParam, Constants.SELECT_MAX_AGE);
		String occupations = ParamUtils.getParamFromJson(taskParam, Constants.SELECT_OCCUPATIONS);
		String cities = ParamUtils.getParamFromJson(taskParam, Constants.SELECT_CITIES);
		String gender = ParamUtils.getParamFromJson(taskParam, Constants.SELECT_GENDER);
		String keywords = ParamUtils.getParamFromJson(taskParam, Constants.SELECT_KEYWORDS);
		String categoryIds = ParamUtils.getParamFromJson(taskParam, Constants.SELECT_CATEGORY_IDS);
		
		String _parameter = (startAge != null ? Constants.SELECT_MIN_AGE + ":" + startAge + "|" : "")
				+ (endAge != null ? Constants.SELECT_MAX_AGE + ":" + endAge + "|" : "")
				+ (occupations != null ? Constants.SELECT_OCCUPATIONS + "=" + occupations + "|" : "")
				+ (cities != null ? Constants.SELECT_CITIES + ":" + cities + "|" : "")
				+ (gender != null ? Constants.SELECT_GENDER + ":" + gender + "|" : "")
				+ (keywords != null ? Constants.SELECT_KEYWORDS + ":" + keywords + "|" : "")
				+ (categoryIds != null ? Constants.SELECT_CATEGORY_IDS + ":" + categoryIds : ""); 
		
		if (_parameter.endsWith("\\|")) {
			_parameter = _parameter.substring(0, _parameter.length() - 1);
		}
		
		final String parameter = _parameter;
		
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionId2AggrInfoRDD.filter(
				(Function<Tuple2<String, String>, Boolean>) (Tuple2<String, String> tuple) -> {
				
					String aggrInfo = tuple._2;
					
					if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, 
							parameter, Constants.SELECT_MIN_AGE, Constants.SELECT_MAX_AGE)) {
						return false;
					}
					
					if(!ValidUtils.in(aggrInfo, Constants.FIELD_OCCUPATION, 
							parameter, Constants.SELECT_OCCUPATIONS)) {
						return false;
					}
					
					if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, 
							parameter, Constants.SELECT_CITIES)) {
						return false;
					}
					
					if(!ValidUtils.equal(aggrInfo, Constants.FIELD_GENDER, 
							parameter, Constants.SELECT_GENDER)) {
						return false;
					}
					
					if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, 
							parameter, Constants.SELECT_KEYWORDS)) {
						return false;
					}
					
					if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, 
							parameter, Constants.SELECT_CATEGORY_IDS)) {
						return false;
					}
					
					// accumulate filtered session_count
					sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
					
					long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
							aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
					long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
							aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
					
					
					calculateVisitLength(visitLength, sessionAggrStatAccumulator);
					calculateStepLength(stepLength, sessionAggrStatAccumulator);
					
					return true;

				});
		
		return filteredSessionid2AggrInfoRDD;
	}

	
	// -----------------------------------------------------------------------------------------.

	
	@SuppressWarnings("deprecation")
	private static void calculateStepLength(long stepLength, 
			Accumulator<String> sessionAggrAccumulator) {
		if(stepLength >= 1 && stepLength <= 3) {
			sessionAggrAccumulator.add(Constants.STEP_PERIOD_1_3);
		}else if(stepLength >= 4 && stepLength <= 6) {
			sessionAggrAccumulator.add(Constants.STEP_PERIOD_4_6);
		}else if(stepLength >= 7 && stepLength <= 9) {
			sessionAggrAccumulator.add(Constants.STEP_PERIOD_7_9);
		}else if(stepLength >= 10 && stepLength <= 30) {
			sessionAggrAccumulator.add(Constants.STEP_PERIOD_10_30);
		}else if(stepLength > 30 && stepLength <= 60) {
			sessionAggrAccumulator.add(Constants.STEP_PERIOD_30_60);
		}else if(stepLength > 60) {
			sessionAggrAccumulator.add(Constants.STEP_PERIOD_60);
		}
			
	}

	
	// -----------------------------------------------------------------------------------------.

	
	@SuppressWarnings("deprecation")
	private static void calculateVisitLength(long visitLength, 
			Accumulator<String> sessionAggrAccumulator) {
		if(visitLength >= 1 && visitLength <= 3) {
			sessionAggrAccumulator.add(Constants.TIME_PERIOD_1s_3s);
		}else if(visitLength >= 4 && visitLength <= 6) {
			sessionAggrAccumulator.add(Constants.TIME_PERIOD_4s_6s);
		}else if(visitLength >= 7 && visitLength <= 9) {
			sessionAggrAccumulator.add(Constants.TIME_PERIOD_7s_9s);
		}else if(visitLength >= 10 && visitLength <= 30) {
			sessionAggrAccumulator.add(Constants.TIME_PERIOD_10s_30s);
		}else if(visitLength > 30 && visitLength <= 60) {
			sessionAggrAccumulator.add(Constants.TIME_PERIOD_30s_60s);
		}else if(visitLength > 60 && visitLength <= 180) {
			sessionAggrAccumulator.add(Constants.TIME_PERIOD_1m_3m);
		}else if(visitLength > 180 && visitLength <= 600) {
			sessionAggrAccumulator.add(Constants.TIME_PERIOD_3m_10m);
		}else if(visitLength > 600 && visitLength <= 1800) {
			sessionAggrAccumulator.add(Constants.TIME_PERIOD_10m_30m);
		}else if(visitLength > 1800) {
			sessionAggrAccumulator.add(Constants.TIME_PERIOD_30m);
		}
	
	}
	

	// -----------------------------------------------------------------------------------------.
	
	/**
	 * Get aggregated counts from accumulator 
	 * @param value
	 * @param taskId
	 */
	private static void calculateAndPersistAggrStat(
			String value, 
			final long taskId) {
		
		long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.SESSION_COUNT));
		
		long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1s_3s));
		long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_4s_6s));
		long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_7s_9s));
		long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10s_30s));
		long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30s_60s));
		long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1m_3m));
		long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_3m_10m));
		long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10m_30m));
		long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30m));
		
		long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_1_3));
		long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_4_6));
		long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_7_9));
		long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_10_30));
		long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_30_60));
		long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_60));
		
		// Calculate visit_lengh and step_length
		double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
				(double)visit_length_1s_3s / (double)session_count, 2);
		double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
				(double)visit_length_4s_6s / (double)session_count, 2);
		double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
				(double)visit_length_7s_9s / (double)session_count, 2);
		double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
				(double)visit_length_10s_30s / (double)session_count, 2);
		double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
				(double)visit_length_30s_60s / (double)session_count, 2);
		double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
				(double)visit_length_1m_3m / (double)session_count, 2);
		double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
				(double)visit_length_3m_10m / (double)session_count, 2);
		double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
				(double)visit_length_10m_30m / (double)session_count, 2);
		double visit_length_30m_ratio = NumberUtils.formatDouble(
				(double)visit_length_30m / (double)session_count, 2);
		
		double step_length_1_3_ratio = NumberUtils.formatDouble(
				(double)step_length_1_3 / (double)session_count, 2);
		double step_length_4_6_ratio = NumberUtils.formatDouble(
				(double)step_length_4_6 / (double)session_count, 2);
		double step_length_7_9_ratio = NumberUtils.formatDouble(
				(double)step_length_7_9 / (double)session_count, 2);
		double step_length_10_30_ratio = NumberUtils.formatDouble(
				(double)step_length_10_30 / (double)session_count, 2);
		double step_length_30_60_ratio = NumberUtils.formatDouble(
				(double)step_length_30_60 / (double)session_count, 2);
		double step_length_60_ratio = NumberUtils.formatDouble(
				(double)step_length_60 / (double)session_count, 2);
		
		// Get Domain objects of visit results
		SessionAggrStat sessionAggrStat = new SessionAggrStat();
		sessionAggrStat.setTaskId(taskId);
		sessionAggrStat.setSession_count(session_count);
		sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
		sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
		sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
		sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
		sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
		sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
		sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
		sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
		sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
		
		sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
		sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
		sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
		sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
		sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
		sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);
		
		ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
		sessionAggrStatDAO.insert(sessionAggrStat);
	}
	
	
	// -----------------------------------------------------------------------------------------.
	
	 
	/**
	 * Get qualified sessions' sessionDetailsRRD
	 * @param filteredSessionid2AggrStatRDD
	 * @param sessionId2ActionRDD
	 * @return
	 */
	private static JavaPairRDD<String, Row> getSessionId2DetailsRDD(
			JavaPairRDD<String, String> filteredSessionid2AggrStatRDD,
			JavaPairRDD<String, Row> sessionId2ActionRDD){
				
		JavaPairRDD<String, Row>sessionId2DetailsRDD = filteredSessionid2AggrStatRDD
				.join(sessionId2ActionRDD)     // remove aggregated info
				.mapToPair(
						(PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>)
						(Tuple2<String, Tuple2<String, Row>> tuple) -> {
							return new Tuple2<String, Row>(tuple._1, tuple._2._2);
						});
		
		return sessionId2DetailsRDD;
		
	}
	
	
	// -----------------------------------------------------------------------------------------.
	
	
	/**
	 * Get top 10 popular categories
	 * @param filteredSessionid2AggrStatRDD
	 * @param sessionId2ActionRDD
	 */
	private static List<Tuple2<CategorySortingKey, String>> getTop10Categories(
			final long taskId, 
			JavaPairRDD<String, Row> sessionId2DetailsRDD) {
		/**
		 * Step1: get all categories visited by qualified sessions 
		 */
		
		// get visited(clicked, ordered, and paid) category_ids
		JavaPairRDD<Long, Long> categoryIdRDD = sessionId2DetailsRDD.flatMapToPair(
				(PairFlatMapFunction<Tuple2<String, Row>, Long, Long>)
				(Tuple2<String, Row> tuple) -> {
					Row row = tuple._2;
					
					List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
					
					Long clickCategoryId = row.getLong(6);
					if(clickCategoryId != null) {
						list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
					}
					
					String orderCategoryIds = row.getString(8);  // a string of comma separated CategoryIds
					if(orderCategoryIds != null) {
						String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
						for(String orderCategoryId : orderCategoryIdsSplited) {
							list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 
									Long.valueOf(orderCategoryId)));
						}
					}
					
					String payCategoryIds = row.getString(10);
					if(payCategoryIds != null) {
						String[] payCategoryIdsSplited = payCategoryIds.split(",");
						for(String payCategoryId : payCategoryIdsSplited) {
							list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 
									Long.valueOf(payCategoryId)));
						}
					}
					
					return (Iterator<Tuple2<Long, Long>>) list;
				});
		
		// filter out repeated data
		categoryIdRDD = categoryIdRDD.distinct();
		
		
		/**
		 * Step2: Calculate counts of clicks, orders, and payments on each categories
		 */
		// Get click count of each categories
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = 
				getClickCategoryId2CountRDD(sessionId2DetailsRDD);
						
		// Get order count of each categories
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = 
				getOrderCategoryId2CountRDD(sessionId2DetailsRDD);
				
		// Get payment count of each categories
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = 
				getPayCategoryId2CountRDD(sessionId2DetailsRDD);
		
		
		/**
		 * Step3: Join counts of clicks, orders, and payments on each categories
		 * Left Outer Join RDDs
		 */
		JavaPairRDD<Long, String> categoryTop10CountsRDD =  
				joinCategoryTop10Counts(categoryIdRDD, 
						                clickCategoryId2CountRDD, 
						                orderCategoryId2CountRDD, 
						                payCategoryId2CountRDD);
		
		
		/**
		 * Step4: define multiple-sorting key
		 * @see com.shangTsai.session_analytics.session.CategorySortingKey.java
		 */
		
		
		/**
		 * Step5: transform data into RDD<Sorting_Key, Info>, then sort
		 */
		
		JavaPairRDD<CategorySortingKey, String> sortKey2CountRDD = categoryTop10CountsRDD.mapToPair(
				(PairFunction<Tuple2<Long, String>, CategorySortingKey, String>)
				(Tuple2<Long, String> tuple) -> {
					String countInfo = tuple._2();
					long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
							countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
					long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
							countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
					long paymentCount = Long.valueOf(StringUtils.getFieldFromConcatString(
							countInfo, "\\|", Constants.FIELD_PAYMENT_COUNT));
					
					CategorySortingKey sortingKey = 
							new CategorySortingKey(clickCount, orderCount, paymentCount);
					
					return new Tuple2<CategorySortingKey, String>(sortingKey, countInfo);
				});
		
		
		JavaPairRDD<CategorySortingKey, String> sortedCategoryTop10CountsRDD = 
				sortKey2CountRDD.sortByKey(false); // sort in descending order
		
		/**
		 * Step6: Get top 10 categories and output to MySQL
		 */
		
		List<Tuple2<CategorySortingKey, String>> top10categoryList = 
				sortedCategoryTop10CountsRDD.take(10);
		
		ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
		
		for(Tuple2<CategorySortingKey, String> tuple : top10categoryList) {
			String countInfo = tuple._2();
			long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
			long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
			long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
			long paymentCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_PAYMENT_COUNT));
			
			Top10Category category = new Top10Category();
			category.setTaskId(taskId);
			category.setCategoryId(categoryId);
			category.setClickCount(clickCount);
			category.setOrderCount(orderCount);
			category.setPaymentCount(paymentCount);
			
			top10CategoryDAO.insert(category);
		}
		
		return top10categoryList;
	}
	
	
	// -----------------------------------------------------------------------------------------.
	
	
	/**
	 * Get click count of each categories
	 * @param sessionId2DetailsRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(
			JavaPairRDD<String, Row> sessionId2DetailsRDD){
		
		JavaPairRDD<String, Row> clickActionRDD = sessionId2DetailsRDD.filter(
				(Function<Tuple2<String, Row>, Boolean>)
				(Tuple2<String, Row> tuple) -> {
					Row row = tuple._2;
					return row.get(6) != null ? true : false;	
				}).coalesce(100);
		
		JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(
				(PairFunction<Tuple2<String, Row>, Long, Long>)
				(Tuple2<String, Row> tuple) -> {
					long clickCategoryId = tuple._2.getLong(6);
					return new Tuple2<Long, Long>(clickCategoryId, 1L);
				});
		
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(
				(Function2<Long, Long, Long>)(Long v1, Long v2) -> {
					return v1 + v2;
				});
		
		return clickCategoryId2CountRDD;
	}
	
	
	// -----------------------------------------------------------------------------------------.
	
	
	/**
	 * Get order count of each categories
	 * @param sessionId2DetailsRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(
			JavaPairRDD<String, Row> sessionId2DetailsRDD){
		
		JavaPairRDD<String, Row> orderActionRDD = sessionId2DetailsRDD.filter(
				(Function<Tuple2<String, Row>, Boolean>)
				(Tuple2<String, Row> tuple) -> {
					Row row = tuple._2;
					return (String.valueOf(row.getString(8)) != null ? true : false);	
				});
		
		JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(
				(PairFlatMapFunction<Tuple2<String, Row>, Long, Long>)
				(Tuple2<String, Row> tuple) -> {
					
					String orderCategoryIds = tuple._2.getString(8);
					String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
					
					List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
					
					for(String orderCategoryId : orderCategoryIdsSplited) {
						list.add(new Tuple2<Long, Long> (Long.valueOf(orderCategoryId), 1L));
					}
					
					return (Iterator<Tuple2<Long, Long>>) list;
				});
		
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(
				(Function2<Long, Long, Long>)(Long v1, Long v2) -> {
					return v1 + v2;
				});
		
		return orderCategoryId2CountRDD;
	}

	
	// -----------------------------------------------------------------------------------------.
	
	
	/**
	 * Get payment count of each categories
	 * @return
	 */
	private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(
			JavaPairRDD<String, Row>sessionId2DetailsRDD){
		
		JavaPairRDD<String, Row> payActionRDD = sessionId2DetailsRDD.filter(
				(Function<Tuple2<String, Row>, Boolean>)
				(Tuple2<String, Row> tuple) -> {
					Row row = tuple._2;
					return (String.valueOf(row.getString(10)) != null ? true : false);	
				});
		
		JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(
				(PairFlatMapFunction<Tuple2<String, Row>, Long, Long>)
				(Tuple2<String, Row> tuple) -> {
					
					String payCategoryIds = tuple._2.getString(10);
					String[] payCategoryIdsSplited = payCategoryIds.split(",");
					
					List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
					
					for(String payCategoryId : payCategoryIdsSplited) {
						list.add(new Tuple2<Long, Long> (Long.valueOf(payCategoryId), 1L));
					}
					
					return (Iterator<Tuple2<Long, Long>>) list;
				});
		
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(
				(Function2<Long, Long, Long>)(Long v1, Long v2) -> {
					return v1 + v2;
				});
		
		return payCategoryId2CountRDD;
	}

	
	// -----------------------------------------------------------------------------------------.
	
	
	/**
	 * Join counts of clicks, orders, and payments on each categories
	 * @param clickCategoryId2CountRDD
	 * @param orderCategoryId2CountRDD
	 * @param payCategoryId2CountRDD
	 * @return
	 */
	private static JavaPairRDD<Long, String> joinCategoryTop10Counts(
			JavaPairRDD<Long, Long> categoryIdRDD,
			JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
			JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
			JavaPairRDD<Long, Long> payCategoryId2CountRDD){
		
		JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tempRDD = 
				categoryIdRDD.leftOuterJoin(clickCategoryId2CountRDD);
		
		JavaPairRDD<Long, String> tempMapRDD = tempRDD.mapToPair(
				(PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>)
				(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple) -> {
					long categoryId = tuple._1;
					Optional<Long> optional = tuple._2._2;
					long clickCount = 0L;
					
					if(optional.isPresent()) {
						clickCount = optional.get();
					}
					
					String value = Constants.FIELD_CATEGORY_ID + ":" + categoryId + "|" + 
					               Constants.FIELD_CLICK_COUNT + ":" + clickCount;
					
					return new Tuple2<Long, String>(categoryId, value);
				});
		
		tempMapRDD = tempMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(
				(PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>)
				(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) -> {
					long categoryId = tuple._1;
					String value = tuple._2._1;
					Optional<Long> optional = tuple._2._2;
					long orderCount = 0L;
					
					if(optional.isPresent()) {
						orderCount = optional.get();
					}
					
					value = value + "|" + Constants.FIELD_ORDER_COUNT + ":" + orderCount;
					
					return new Tuple2<Long, String>(categoryId, value);
				});
		
		tempMapRDD = tempMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(
				(PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>)
				(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) -> {
					long categoryId = tuple._1;
					String value = tuple._2._1;
					Optional<Long> optional = tuple._2._2;
					long paymentCount = 0L;
					
					if(optional.isPresent()) {
						paymentCount = optional.get();
					}
					
					value = value + "|" + Constants.FIELD_PAYMENT_COUNT + ":" + paymentCount;
					
					return new Tuple2<Long, String>(categoryId, value);
				});
		
		return tempMapRDD;
		
	}

	
	// -----------------------------------------------------------------------------------------.

	/**
	 * Get top 10 most active session
	 * @param taskId
	 * @param sessionId2DetailsRDD
	 */
	private static void getTop10Sessions(
			JavaSparkContext sc,
			final long taskId, 
			List<Tuple2<CategorySortingKey, String>> top10categoryList,
			JavaPairRDD<String, Row> sessionId2DetailsRDD) {
		/**
		 * Step1: Generate an RDD of top10 categoryIds
		 */
		List<Tuple2<Long, Long>> top10categoryIdList = 
				new ArrayList<Tuple2<Long, Long>>();
		
		for(Tuple2<CategorySortingKey, String> category : top10categoryList) {
			long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(
					category._2, "\\|", Constants.FIELD_CATEGORY_ID));
			
			top10categoryIdList.add(new Tuple2<Long, Long>(categoryId, categoryId));
		}
		
		JavaPairRDD<Long, Long> top10categoryIdRDD = 
				sc.parallelizePairs(top10categoryIdList);
		
		/**
		 * Step2: Calculate click counts of top10 categories
		 */
		JavaPairRDD<String, Iterable<Row>> sessionGroupedDetailsRDD = 
				sessionId2DetailsRDD.groupByKey();
		
		JavaPairRDD<Long, String> categoryId2ClickCountRDD = sessionGroupedDetailsRDD
				.flatMapToPair(
				(PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>)
				(Tuple2<String, Iterable<Row>> tuple) -> {
					
					String sessionId = tuple._1;
					Iterator<Row> iterator = tuple._2.iterator();
					
					Map<Long, Long> categoryClickCountMap = new HashMap<Long, Long>();
					
					// get each session's click_count on each categoryId
					while(iterator.hasNext()) {
						Row row = iterator.next();
						if(row.get(6) != null) {    // if categoryId exists
							long categoryId = row.getLong(6);
							Long clickCount = categoryClickCountMap.get(categoryId);
							if(clickCount == null) {
								clickCount = 0L;
							}
							clickCount++;
							categoryClickCountMap.put(categoryId, clickCount);
						}
					}
					
					List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();
					
					// return format: <categoryId,sessionId,count>
					for(Map.Entry<Long, Long> countEntry : categoryClickCountMap.entrySet()) {
						long categoryId = countEntry.getKey();
						long clickCount = countEntry.getValue();
						String value = sessionId + "," + clickCount;
						list.add(new Tuple2<Long, String>(categoryId, value));
					}
				
					return (Iterator<Tuple2<Long, String>>) list;
				});
		
		// sessions' clickCounts on top10 categories in RDD<categoryId, clickCount>
		JavaPairRDD<Long, String> top10categorySessionClickCountRDD = top10categoryIdRDD
				.join(categoryId2ClickCountRDD)
				.mapToPair(
						(PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>)
						(Tuple2<Long, Tuple2<Long, String>> tuple) -> {
							
							return new Tuple2<Long, String>(tuple._1, tuple._2._2);
						});
		
		/**
		 * Step3: Get top10 most active session on each category, via groupByKey 
		 * session activity measure by clickCount 
		 */
		JavaPairRDD<Long, Iterable<String>> top10categorySessionClickGroupedRDD = 
				top10categorySessionClickCountRDD.groupByKey();
		
		JavaPairRDD<String, String> top10SessionRDD = top10categorySessionClickGroupedRDD
				.flatMapToPair(
						(PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>)
						(Tuple2<Long, Iterable<String>> tuple) -> {
						
							long categoryId = tuple._1;
							Iterator<String> iterator = tuple._2.iterator();
							
							String[] top10Sessions = new String[10];
							
							while(iterator.hasNext()) {
								String sessionCount = iterator.next();
								String sessionId = sessionCount.split(",")[0]; 
								long clickCount = Long.valueOf(sessionCount.split(",")[1]);
								
								for(int i = 0; i < top10Sessions.length; ++i) {
									if(top10Sessions[i] == null) {
										top10Sessions[i] = sessionCount;
										break;
									} else {
										long _count = Long.valueOf(top10Sessions[i].split(",")[1]);
											
											if(clickCount > _count) {
												for(int j = 9; j > i; --j) {
													top10Sessions[j] = top10Sessions[j - 1];
;												}
											}
											top10Sessions[i] = sessionCount;
											break;
										}
									}
								}
								
								// output to MySQL
							List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
							
							for(String sessionCount : top10Sessions) {
								if(sessionCount != null) {
									String sessionId = sessionCount.split(",")[0];
									long clickCount = Long.valueOf(sessionCount.split(",")[1]);
									// inserting each column
									Top10Session top10Session = new Top10Session();
									top10Session.setTaskId(taskId);
									top10Session.setCategoryId(categoryId);
									top10Session.setSessionId(sessionId);
									top10Session.setClickCount(clickCount);
									
									ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
									top10SessionDAO.insert(top10Session);
									// add into list
									list.add(new Tuple2<String, String>(sessionId, sessionId));
								}
							}
							
							return (Iterator<Tuple2<String, String>>) list;
						});
		
		/**
		 * Step4: Get details of top10 most active sessions and output to MySQL
		 */
		
		JavaPairRDD<String, Tuple2<String, Row>> sessionDetailsRDD = 
				top10SessionRDD.join(sessionId2DetailsRDD);
		
		sessionDetailsRDD.foreach(
				(VoidFunction<Tuple2<String, Tuple2<String, Row>>>)
				(Tuple2<String, Tuple2<String, Row>> tuple) -> {
					Row row = tuple._2._2;
					SessionDetails sessionDetails = new SessionDetails();
					sessionDetails.setTaskId(taskId);
					sessionDetails.setUserId(row.getLong(1));
					sessionDetails.setSessionId(row.getString(2));
					sessionDetails.setPageId(row.getLong(3));
					sessionDetails.setActionTime(row.getString(4));
					sessionDetails.setSearchKeyword(row.getString(5));
					sessionDetails.setClickCategoryId(row.getLong(6));
					sessionDetails.setClickProductId(row.getLong(7));
					sessionDetails.setOrderCategoryIds(row.getString(8));
					sessionDetails.setOrderProductIds(row.getString(9));
					sessionDetails.setPayCategoryIds(row.getString(10));
					sessionDetails.setPayProductIds(row.getString(11));
					
					ISessionDetailsDAO sessionDetailsDAO = DAOFactory.getSessionDetailsDAO();
					sessionDetailsDAO.insert(sessionDetails);
				});
		
	}
	
}
