package com.shangTsai.session_analytics.utils;

import java.math.BigDecimal;

/**
 * 
 * Tools for formating numbers
 * @author shangluntsai
 *
 */

public class NumberUtils {

	public static double formatDouble(double num, int scale) {
		BigDecimal bd = new BigDecimal(num);
		return bd.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
	}
}
