package com.shangTsai.session_analytics.page_conversion;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.shangTsai.session_analytics.constants.Constants;
import com.shangTsai.session_analytics.dao.IPageSplitConversionRateDAO;
import com.shangTsai.session_analytics.dao.ITaskDAO;
import com.shangTsai.session_analytics.dao.impl.DAOFactory;
import com.shangTsai.session_analytics.domain.PageSplitConversionRate;
import com.shangTsai.session_analytics.domain.Task;
import com.shangTsai.session_analytics.session.CategorySortingKey;
import com.shangTsai.session_analytics.utils.DateUtils;
import com.shangTsai.session_analytics.utils.NumberUtils;
import com.shangTsai.session_analytics.utils.ParamUtils;
import com.shangTsai.session_analytics.utils.SparkUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * Page one-step conversion rate Spark operation
 * @author shangluntsai
 *
 */
public class PageOneStepConversionRate {

	private static JsonParser jsonParser = new JsonParser();
	
	public static void main(String[] args) throws Exception {
			
		SparkConf conf = new SparkConf()
				.setAppName(Constants.APP_PAGE_CONVERSION_ANALYTICS)
				.setMaster("local[*]")
				.set("spark.serializer", "org.apache.spark.serializer.kryoSerializer")
				.registerKryoClasses(new Class[] {CategorySortingKey.class});  
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession session = SparkSession.builder().getOrCreate();
		
		// Generate mock data in local environment, disable in production
		SparkUtils.mockData(sc, session);
		
		// get parameters of a task
		long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);
		
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(taskId);
		
		if(task == null) {
			System.out.println(new Date() + ": cannot find this task with ID [" + taskId + "].");
			return;
		}
		
		JsonObject taskParam = jsonParser.parse(task.getTaskParam()).getAsJsonObject();
		
		// Query user visiting data in a certain range of date
		JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(session, taskParam);
		
		
		// Get session granularity data
		JavaPairRDD<String, Row> sessionId2ActionRDD = getSessionId2ActionRDD(actionRDD);
		sessionId2ActionRDD = sessionId2ActionRDD.cache();
		
		// Get each session's visiting details
		JavaPairRDD<String, Iterable<Row>> sessionIdGroupActionRDD = sessionId2ActionRDD.groupByKey();
		
		
		// Get one-step page splits, page flow matching
		JavaPairRDD<String, Long> pageSplitRDD = generateAndMatchPageSplit(
				sc, sessionIdGroupActionRDD, taskParam);
		
		Map<String, Long> pageSplitPvMap = pageSplitRDD.countByKey();
	
		
		long startPagePv = getStartPagePv(taskParam, sessionIdGroupActionRDD);
		
		
		// Compute conversion rate of page splits
		Map<String, Double> conversionRateMap = computePageSplitConversionRate(
				taskParam, pageSplitPvMap, startPagePv);
		
		
		// Persist conversion rate data
		persistConversionRate(taskId, conversionRateMap);
		
	}

	
	// -----------------------------------------------------------------------------------------.
	
	
	/**
	 * Get a <session_id, actionDateRange>RDD
	 * @param actionRDD
	 * @return
	 */
	
	private static JavaPairRDD<String, Row> getSessionId2ActionRDD(
			JavaRDD<Row> actionRDD) {
		return actionRDD.mapToPair(
				(PairFunction<Row, String, Row>) row -> {
					return new Tuple2<String, Row>(row.getString(2), row);
				});

	}
		
	// -----------------------------------------------------------------------------------------.
	
	
	/**
	 * Get page Split and match
	 * @param sc
	 * @param sessionId2ActionsRDD
	 * @param taskParam
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private static JavaPairRDD<String, Long> generateAndMatchPageSplit(
			JavaSparkContext sc,
			JavaPairRDD<String, Iterable<Row>> sessionIdGroupActionRDD, 
			JsonObject taskParam) {
		
		String targetPageFlow = 
				ParamUtils.getParamFromJson(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		final Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);
		
		return sessionIdGroupActionRDD.flatMapToPair(
				
				(PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Long>)
				(Tuple2<String, Iterable<Row>> tuple) -> {
					
					List<Tuple2<String, Iterable<Row>>> list = 
							new ArrayList<Tuple2<String, Iterable<Row>>>();
					
					Iterator<Row> iterator = (Iterator<Row>) tuple._2.iterator();
					
					String[] targetPages = targetPageFlowBroadcast.value().split(",");
					
					List<Row> rows = new ArrayList<Row>();
					
					while(iterator.hasNext()) {
						rows.add(iterator.next());
					}
					
					Collections.sort(rows, new Comparator<Row> () {
						
						public int compare(Row o1, Row o2) {
							String actionTime1 = o1.getString(4);
							String actionTime2 = o2.getString(4);
							
							Date date1 = DateUtils.parseTime(actionTime1);
							Date date2 = DateUtils.parseTime(actionTime2);
							
							return (int) (date1.getTime() - date2.getTime());
						}
					});
					
					Long lastPageId = null;
					
					for(Row row : rows) {
						long pageId = row.getLong(3);
						
						if(lastPageId == null) {
							lastPageId = pageId;
							continue;
						}
						
						String pageSplit = lastPageId + "_" + pageId;
						
						for(int i = 1; i < targetPages.length; ++i) {
							
							String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
							
							if(pageSplit.equals(targetPageSplit)) {
								list.addAll((Collection<? extends Tuple2<String, Iterable<Row>>>) 
										new Tuple2<String, Integer>(pageSplit, 1));
								break;
							}
							
							
						}
						
						lastPageId = pageId;
					}
					
					return (Iterator<Tuple2<String, Long>>) list;
				});
		
	}
	
	
	// -----------------------------------------------------------------------------------------.
	
	
	/**
	 * Obtain PV of start page in a page flow
	 * @param taskParam
	 * @param sessionId2ActionsRDD
	 * @return
	 */
	private static long getStartPagePv(
			JsonObject taskParam, 
			JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD
			) {
		String targetPageFlow  = ParamUtils.getParamFromJson(
						taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		
		final long startPageId = Long.valueOf(targetPageFlow.split(",")[0]);
		
		JavaRDD<Long> startPageRDD = sessionId2ActionsRDD.flatMap(
				(FlatMapFunction<Tuple2<String, Iterable<Row>>, Long>) 
				(Tuple2<String, Iterable<Row>> tuple) -> {
					
					List<Long> list = new ArrayList<Long>();
					
					Iterator<Row> iterator = (Iterator<Row>) tuple._2.iterator();
					
					while(iterator.hasNext()) {
						Row row = iterator.next();
						long pageId = row.getLong(3);
						
						if(pageId ==startPageId) {
							list.add(pageId);
						}
					}
						
					return (Iterator<Long>) list;
				});
		
		return startPageRDD.count();
	}
	
	
	// -----------------------------------------------------------------------------------------.
	
	
	/**
	 * Compute conversion rate of page splits
	 * @param pageSplitPvMap
	 * @param startPagePv
	 * @return
	 */
	private static Map<String, Double> computePageSplitConversionRate(
			JsonObject taskParam,
			Map<String, Long> pageSplitPvMap,
			long startPagePv){
			
		Map<String, Double> conversionRateMap = new HashMap<String, Double>();
		
		String[] targetPages = 
				ParamUtils.getParamFromJson(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",");
		
		long lastPageSplitPv = 0L;
		
		for(int i = 0; i < targetPages.length; ++i) {
			String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
			long targetPageSplitPv = Long.valueOf(
					String.valueOf(pageSplitPvMap.get(targetPageSplit)));
			
			double conversionRate = 0.0;
			
			if(i == 1) {
				conversionRate = NumberUtils.formatDouble(
						(double)targetPageSplitPv / (double)startPagePv, 2);
			} else {
				conversionRate = NumberUtils.formatDouble(
						(double)targetPageSplitPv / (double)lastPageSplitPv, 2);
			}
			
			conversionRateMap.put(targetPageSplit, conversionRate);
			
			lastPageSplitPv = targetPageSplitPv;
		}
		
		return conversionRateMap;
		
	}

	
	// -----------------------------------------------------------------------------------------.
	
	
	/**
	 * Persist conversion rate data
	 * @param conversionRateMap
	 */
	private static void persistConversionRate(
			long taskId,
			Map<String, Double> conversionRateMap) {
		
		StringBuffer buffer = new StringBuffer();
		
		for(Map.Entry<String, Double> conversionRateEntry : conversionRateMap.entrySet()) {
			String pageSplit = conversionRateEntry.getKey();
			double conversionRateValue = conversionRateEntry.getValue();
			buffer.append(pageSplit + ":" + conversionRateValue + "|");
		}
		
		String conversionRate = buffer.toString();
		conversionRate = conversionRate.substring(0, conversionRate.length() - 1);
		
		PageSplitConversionRate pageSplitConversionRate = new PageSplitConversionRate();
		
		pageSplitConversionRate.setTaskId(taskId);
		pageSplitConversionRate.setConversionRate(conversionRate);
		
		IPageSplitConversionRateDAO pageSplitConversionRateDAO = 
				DAOFactory.getPageSplitConversionRateDAO();
		
		pageSplitConversionRateDAO.insert(pageSplitConversionRate);
	}
}
