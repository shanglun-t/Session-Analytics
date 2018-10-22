package com.shangTsai.session_analytics.dao.impl;

import com.shangTsai.session_analytics.dao.IPageSplitConversionRateDAO;
import com.shangTsai.session_analytics.dao.ISessionAggrStatDAO;
import com.shangTsai.session_analytics.dao.ISessionDetailsDAO;
import com.shangTsai.session_analytics.dao.ISessionRandomSamplingDAO;
import com.shangTsai.session_analytics.dao.ITaskDAO;
import com.shangTsai.session_analytics.dao.ITop10CategoryDAO;
import com.shangTsai.session_analytics.dao.ITop10SessionDAO;

/**
 * 
 * DAO Factory class
 * @author shangluntsai
 *
 */
public class DAOFactory {

	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}
	
	public static ISessionAggrStatDAO getSessionAggrStatDAO() {
		return new SessionAggrStatDAOImpl();
	}
	
	public static ISessionRandomSamplingDAO getSessionRandomSamplingDAO() {
		return new SessinoRandomSamplingDAOImpl();
	}
	
	public static ISessionDetailsDAO getSessionDetailsDAO() {
		return new SessionDetailsDAOImpl();
	}
	
	public static ITop10CategoryDAO getTop10CategoryDAO() {
		return new Top10CategoryDAOImpl();
	}
	
	public static ITop10SessionDAO getTop10SessionDAO() {
		return new Top10SessionDAOImpl();
	}
	
	public static IPageSplitConversionRateDAO getPageSplitConversionRateDAO() {
		return new PagaSplitConversionRateImpl();
	}
	
}
