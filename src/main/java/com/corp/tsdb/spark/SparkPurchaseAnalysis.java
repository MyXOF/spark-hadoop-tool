package com.corp.tsdb.spark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.corp.tsdb.utils.PurchaseResult;

import scala.Tuple2;

public class SparkPurchaseAnalysis extends AbstractSpark{
	private static final SparkPurchaseAnalysis ANALYSIS = new SparkPurchaseAnalysis();
	
	private JavaPairRDD<String,Tuple2<Integer, Double>> info;
	private JavaPairRDD<String,Tuple2<Integer, Double>> dataSumRangeByDay;
	private JavaPairRDD<String,Tuple2<Integer, Double>> dataSumRangeBySeason;
	private JavaPairRDD<String,Tuple2<Integer, Double>> dataSumRangeByYear;
	
	private List<PurchaseResult> resultRangeByDay;
	private List<PurchaseResult> resultRangeBySeason;
	private List<PurchaseResult> resultRangeByYear;
	
	private boolean isDayCached;
	private boolean isSeasonCached;
	private boolean isYearCached;
	
	public SparkPurchaseAnalysis getInstance(){
		return ANALYSIS;
	}
	
	private SparkPurchaseAnalysis(){
		super();
		isDayCached = false;
		isSeasonCached = false;
		isYearCached = false;
		
		lines = sc.textFile(Purchase_Path);

	}
	
	public List<PurchaseResult> AnalyzeRangeByYear(){
		if(isYearCached){
			return resultRangeByYear;
		}
		
		AnalyzeRangeBySeason();
		JavaPairRDD<String,Tuple2<Integer, Double>> yearInfo = dataSumRangeBySeason.mapToPair(new PairFunction<Tuple2<String,Tuple2<Integer,Double>>, String, Tuple2<Integer, Double>>() {

			@Override
			public Tuple2<String, Tuple2<Integer, Double>> call(
					Tuple2<String, Tuple2<Integer, Double>> t) throws Exception {
				// TODO Auto-generated method stub
				String values[] = t._1.split("-");
				String year = values[0];
				
				return new Tuple2<String, Tuple2<Integer, Double>>(year,t._2);
			}
		});
		
		dataSumRangeByYear = yearInfo.reduceByKey(new Function2<Tuple2<Integer,Double>, Tuple2<Integer,Double>, Tuple2<Integer,Double>>() {

			@Override
			public Tuple2<Integer, Double> call(Tuple2<Integer, Double> v1,
					Tuple2<Integer, Double> v2) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Integer, Double>(v1._1+v2._1,v1._2+v2._2);
			}
		});
		resultRangeByYear = transform(dataSumRangeByYear, DateType.YEAR);
		isYearCached = true;
		return resultRangeByYear;
	}
	
	public List<PurchaseResult> AnalyzeRangeBySeason(){
		if(isSeasonCached){
			return resultRangeBySeason;
		}
		AnalyzeRangeByDay();
		
		JavaPairRDD<String,Tuple2<Integer, Double>> seasonInfo = dataSumRangeByDay.mapToPair(new PairFunction<Tuple2<String,Tuple2<Integer,Double>>, String, Tuple2<Integer, Double>>() {

			@Override
			public Tuple2<String, Tuple2<Integer, Double>> call(
					Tuple2<String, Tuple2<Integer, Double>> t) throws Exception {
				// TODO Auto-generated method stub
				String values[] = t._1.split("-");
				String season = values[0] + "-" + ((Integer.parseInt(values[1]) - 1)/3+1);
				
				return new Tuple2<String, Tuple2<Integer, Double>>(season,t._2);
			}
		});
		
		dataSumRangeBySeason = seasonInfo.reduceByKey(new Function2<Tuple2<Integer,Double>, Tuple2<Integer,Double>, Tuple2<Integer,Double>>() {

			@Override
			public Tuple2<Integer, Double> call(Tuple2<Integer, Double> v1,
					Tuple2<Integer, Double> v2) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Integer, Double>(v1._1+v2._1,v1._2+v2._2);
			}
		});
		resultRangeBySeason = transform(dataSumRangeBySeason, DateType.SEASON);
		isSeasonCached = true;
		return resultRangeBySeason;
	}
	
	public List<PurchaseResult> AnalyzeRangeByDay(){
		if(isDayCached){
			return resultRangeByDay;
		}
		info = lines.mapToPair(new PairFunction<String, String,Tuple2<Integer, Double>>() {

			@Override
			public Tuple2<String, Tuple2<Integer, Double>> call(String str)
					throws Exception {
				// TODO Auto-generated method stub
				String values[] = str.split(",");
				String day = values[11].split(" ")[0];
				Tuple2<Integer,Double> valueTuple2 = new Tuple2<Integer,Double>(1, Double.parseDouble(values[5]));
				return new Tuple2<String, Tuple2<Integer, Double>>(day,valueTuple2);
			}
		});
		
		dataSumRangeByDay = info.reduceByKey(new Function2<Tuple2<Integer,Double>, Tuple2<Integer,Double>, Tuple2<Integer,Double>>() {
			
			@Override
			public Tuple2<Integer, Double> call(Tuple2<Integer, Double> v1,
					Tuple2<Integer, Double> v2) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Integer, Double>(v1._1+v2._1,v1._2+v2._2);
			}
		});
		
		isDayCached = true;
		resultRangeByDay = transform(dataSumRangeByDay, DateType.DAY);
		return resultRangeByDay;
	}
	
	private List<PurchaseResult> transform(JavaPairRDD<String,Tuple2<Integer, Double>> dataOld,DateType type){
		if(dataOld == null) return null;
		List<PurchaseResult> dataNew = new ArrayList<PurchaseResult>();
		for(Tuple2<String, Tuple2<Integer, Double>> item: dataOld.collect()){
			dataNew.add(new PurchaseResult(type,item._1,item._2._1,item._2._2));
		}
		Collections.sort(dataNew);
		return dataNew;
	}
	
	
	public enum DateType{
		YEAR,SEASON,DAY
	}
}
