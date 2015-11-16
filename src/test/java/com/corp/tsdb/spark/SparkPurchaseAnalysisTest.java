package com.corp.tsdb.spark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

public class SparkPurchaseAnalysisTest extends TestCase {

	private BufferedReader bufferedReader;

	public void testAnalyzeRangeByYear() throws IOException {

		
	}

	public void testAnalyzeRangeBySeason() {
	}

	public void testAnalyzeRangeByDay() throws IOException {
		bufferedReader = new BufferedReader(new FileReader("/Users/xuyi/Documents/Eclipse_WorkSpace/spark-hadoop-tool/src/test/resources/PurchaseSmall.csv"));
		String stringLine = "";
		String values[];
		Map<String, Integer> dayPurchaseNumber = new HashMap<String, Integer>();
		while((stringLine = bufferedReader.readLine()) != null){
			values = stringLine.trim().split(",");
			String day = values[11].split(" ")[0];
			if(dayPurchaseNumber.get(day) == null){
				dayPurchaseNumber.put(day, 1);
			}
			else{
				int num = dayPurchaseNumber.get(day);
				num++;
				dayPurchaseNumber.put(day,num);
			}
		}
		System.out.println(dayPurchaseNumber);
//		
//		SparkPurchaseAnalysis analysis = SparkPurchaseAnalysis.getInstance();
//		List<PurchaseResult> results = analysis.AnalyzeRangeByDay();
//		System.out.println(results);
//		analysis.shutdown();
	}

}
