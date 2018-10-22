package com.shangTsai.session_analytics.session;

import com.shangTsai.session_analytics.constants.Constants;
import com.shangTsai.session_analytics.utils.StringUtils;

import org.apache.spark.AccumulatorParam;

@SuppressWarnings("deprecation")
public class SessionAggrStatAccumulator implements AccumulatorParam<String>{

	private static final long serialVersionUID = 1L;
	
	public String zero(String v) {
		return Constants.SESSION_COUNT + ":0|"
				+ Constants.TIME_PERIOD_1s_3s + ":0|"
				+ Constants.TIME_PERIOD_4s_6s + ":0|"
				+ Constants.TIME_PERIOD_7s_9s + ":0|"
				+ Constants.TIME_PERIOD_10s_30s + ":0|"
				+ Constants.TIME_PERIOD_30s_60s + ":0|"
				+ Constants.TIME_PERIOD_1m_3m + ":0|"
				+ Constants.TIME_PERIOD_3m_10m + ":0|"
				+ Constants.TIME_PERIOD_10m_30m + ":0|"
				+ Constants.TIME_PERIOD_30m + ":0|"
				+ Constants.STEP_PERIOD_1_3 + ":0|"
				+ Constants.STEP_PERIOD_4_6 + ":0|"
				+ Constants.STEP_PERIOD_7_9 + ":0|"
				+ Constants.STEP_PERIOD_10_30 + ":0|"
				+ Constants.STEP_PERIOD_30_60 + ":0|"
				+ Constants.STEP_PERIOD_60 + ":0";
	}
	
	
	public String addInPlace(String v1, String v2) {
		return add(v1, v2);
	}

	public String addAccumulator(String v1, String v2) {
		return add(v1, v2);
	}
	
	
	/**
	 * session accumulator logic
	 * @param v1
	 * @param v2
	 * @return
	 */
	private String add(String v1, String v2) {
		
		if(StringUtils.isEmpty(v1)) {
			return v2;
		}
		// extract the initial value from string
		String initialValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2); 
		
		if(initialValue != null) {
			int updatedValue = Integer.valueOf(initialValue) + 1;
			return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(updatedValue));
		}
		
		return v1;   // if initialValue is null, just return v1
	}
}
