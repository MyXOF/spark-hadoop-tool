package com.corp.tsdb.utils;

import com.corp.tsdb.spark.SparkPurchaseAnalysis.DateType;

public class PurchaseResult implements Comparable<PurchaseResult>{
	public DateType type;
	public String date;
	public Integer numberSum;
	public Double priceSum;

	public PurchaseResult(DateType type,String date,Integer numberSum,Double priceSum) {
		// TODO Auto-generated constructor stub
		this.type = type;
		this.date = date;
		this.numberSum = numberSum;
		this.priceSum = priceSum;			
	}
	
	@Override
	public int compareTo(PurchaseResult o) {
		String values1[],values2[];
		int value1,value2;
		switch (type) {
		case YEAR:
			return Integer.parseInt(date) - Integer.parseInt(o.date); 
		case SEASON:
			values1 = date.split("-");
			values2 = o.date.split("-");
			value1 = Integer.parseInt(values1[0]) * 100 + Integer.parseInt(values1[1]);
			value2 = Integer.parseInt(values2[0]) * 100 + Integer.parseInt(values2[1]);
			return value1 - value2;
		case DAY:
			values1 = date.split("-");
			values2 = o.date.split("-");
			value1 = Integer.parseInt(values1[0]) * 10000 + Integer.parseInt(values1[1]) * 100 + Integer.parseInt(values1[2]);
			value2 = Integer.parseInt(values2[0]) * 10000 + Integer.parseInt(values2[1]) * 100 + Integer.parseInt(values2[2]);
			return value1 - value2;
		default:
			break;
		}
		return 0;
	}

}
