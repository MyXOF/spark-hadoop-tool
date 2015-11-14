package com.corp.tsdb.spark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.corp.tsdb.utils.TypeWithWeight;

import scala.Tuple2;

public class SparkEventClientProgramWatchedAnalysis extends AbstractSpark{
	private static final SparkEventClientProgramWatchedAnalysis ANALYSIS = new SparkEventClientProgramWatchedAnalysis();
	
	private boolean isWatchTimeCached;
	private boolean isChangeChannelCached;
	
	private List<TypeWithWeight<Long>> watchTimeResult;
	private List<TypeWithWeight<Integer>> changeChannelResult;
	
	private SparkEventClientProgramWatchedAnalysis(){
		super();
		isChangeChannelCached = false;
		isWatchTimeCached = false;
		lines = sc.textFile(EventClientProgramWatched_Path);
	}
	
	public List<TypeWithWeight<Long>> AnalyzeWathcTime(){
		if(isWatchTimeCached){
			return watchTimeResult;
		}
		
		JavaPairRDD<String, Long> wathcTimeInfo = lines.mapToPair(new PairFunction<String, String, Long>() {
			@Override
			public Tuple2<String, Long> call(String t) throws Exception {
				String values[] = t.split(",");
				Long time = Long.parseLong(values[5]) / 1000; 
				return new Tuple2<String, Long>(values[1], time);
			}
		});
		
		JavaPairRDD<String, Long> watchTime = wathcTimeInfo.reduceByKey(new Function2<Long, Long, Long>() {
			@Override
			public Long call(Long v1, Long v2) throws Exception {
				return v1+v2;
			}
		});
		
		watchTimeResult = transformByKey(watchTime);
		isWatchTimeCached = true;
		return watchTimeResult;
	}
	
	public List<TypeWithWeight<Integer>> AnalyzeChannelChange(){
		if(isChangeChannelCached){
			return changeChannelResult;
		}
		JavaPairRDD<Tuple2<String, String>, Integer> changeChannelInfo = lines.mapToPair(new PairFunction<String, Tuple2<String, String>, Integer>() {
			@Override
			public Tuple2<Tuple2<String, String>, Integer> call(String t)
					throws Exception {
				String values[] = t.split(",");
				return new Tuple2<Tuple2<String,String>, Integer>(new Tuple2<String,String>(values[1], values[3]),1);
			}
		});
		
		JavaPairRDD<Tuple2<String, String>, Integer> changeChannelTmp = changeChannelInfo.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		JavaPairRDD<String, Tuple2<Integer, Integer>> changeChannelSumInfo = changeChannelTmp.mapToPair(new PairFunction<Tuple2<Tuple2<String,String>,Integer>, String, Tuple2<Integer,Integer>>() {
			@Override
			public Tuple2<String, Tuple2<Integer, Integer>> call(
					Tuple2<Tuple2<String, String>, Integer> t) throws Exception {
				return new Tuple2<String, Tuple2<Integer,Integer>>(t._1._1, new Tuple2<Integer,Integer>(t._2, 1));
			}
		});
		
		JavaPairRDD<String, Tuple2<Integer, Integer>> changeChannel = changeChannelSumInfo.reduceByKey(new Function2<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>, Tuple2<Integer,Integer>>() {
			@Override
			public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1,
					Tuple2<Integer, Integer> v2) throws Exception {
				return new Tuple2<Integer, Integer>(v1._1+v2._1, v1._2+v2._2);
			}
		});
		
		changeChannelResult = transformByKeyAverage(changeChannel);
		isChangeChannelCached = true;
		return changeChannelResult;
	}
	
	private List<TypeWithWeight<Long>> transformByKey(JavaPairRDD<String, Long> dataOld){
		if(dataOld == null) return null;
		List<TypeWithWeight<Long>> dataNew = new ArrayList<TypeWithWeight<Long>>();
		for(Tuple2<String, Long> item : dataOld.collect()){
			dataNew.add(new TypeWithWeight<Long>(item._2, Integer.parseInt(item._1)));
		}
		Collections.sort(dataNew);
		return dataNew;
	}
	
	private List<TypeWithWeight<Integer>> transformByKeyAverage(JavaPairRDD<String, Tuple2<Integer, Integer>> dataOld){
		if(dataOld == null) return null;
		List<TypeWithWeight<Integer>> dataNew = new ArrayList<TypeWithWeight<Integer>>();
		for(Tuple2<String, Tuple2<Integer, Integer>> item : dataOld.collect()){
			dataNew.add(new TypeWithWeight<Integer>(item._2._1 / item._2._2,Integer.parseInt(item._1)));
		}
		Collections.sort(dataNew);
		return dataNew;
	}
}
