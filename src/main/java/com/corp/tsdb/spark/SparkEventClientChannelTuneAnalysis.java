package com.corp.tsdb.spark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.corp.tsdb.utils.TypeWithWeight;

import scala.Tuple2;

public class SparkEventClientChannelTuneAnalysis extends AbstractSpark{
	private static final SparkEventClientChannelTuneAnalysis ANALYSIS = new SparkEventClientChannelTuneAnalysis();
	private boolean isDeviceOnlineCached;
	private boolean isChannleCached;
	private List<TypeWithWeight<Integer>> resultDeviceOnline;
	private List<TypeWithWeight<String>> resultChannel;
	
	private SparkEventClientChannelTuneAnalysis(){
		super();
		isChannleCached = false;
		isDeviceOnlineCached = false;
		lines = sc.textFile(EventClientChannelTune_Path);
	}
	
	public List<TypeWithWeight<Integer>> AnalyzeDeviceOnline(){
		if(isDeviceOnlineCached){
			return resultDeviceOnline;
		}
		
		JavaPairRDD<Tuple2<String, String>, Integer> deviceDataInfo = lines.mapToPair(new PairFunction<String, Tuple2<String, String>, Integer>() {
			@Override
			public Tuple2<Tuple2<String, String>, Integer> call(String t)
					throws Exception {
				String values[] = t.split(",");
				return new Tuple2<Tuple2<String,String>, Integer>(new Tuple2<String,String>(values[1], values[3]), 1);
			}
		});
		
		JavaPairRDD<Tuple2<String, String>, Integer> deviceOnlineData = deviceDataInfo.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return 1;
			}
		});
		
		JavaPairRDD<String, Integer> deviceOnlineSumInfo = deviceOnlineData.mapToPair(new PairFunction<Tuple2<Tuple2<String,String>,Integer>, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(
					Tuple2<Tuple2<String, String>, Integer> t) throws Exception {				
				return new Tuple2<String, Integer>(t._1._1, 1);
			}
		});
		
		JavaPairRDD<String, Integer> deviceOnlineDataRangeByDay = deviceOnlineSumInfo.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		resultDeviceOnline = transformByKey(deviceOnlineDataRangeByDay);
		isDeviceOnlineCached = true;
		return resultDeviceOnline;
	}

	public List<TypeWithWeight<String>> AnalyzeChannel(){
		if(isChannleCached){
			return resultChannel;
		}
		
		JavaPairRDD<String, Integer> channleInfo = lines.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				String values[] = t.split(",");
				return new Tuple2<String, Integer>(values[4], 1);
			}
		});
		
		JavaPairRDD<String, Integer> channelTop = channleInfo.reduceByKey(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		});
		
		resultChannel = transformByValue(channelTop);
		isChannleCached = true;
		return resultChannel;
	}
	
	private List<TypeWithWeight<Integer>> transformByKey(JavaPairRDD<String, Integer> dataOld){
		if(dataOld == null) return null;
		List<TypeWithWeight<Integer>> dataNew = new ArrayList<TypeWithWeight<Integer>>();
		for(Tuple2<String, Integer> item : dataOld.collect()){
			dataNew.add(new TypeWithWeight<Integer>(item._2, Integer.parseInt(item._1)));
		}
		Collections.sort(dataNew);
		return dataNew;
	}
	
	private List<TypeWithWeight<String>> transformByValue(JavaPairRDD<String, Integer> dataOld){
		if(dataOld == null) return null;
		List<TypeWithWeight<String>> dataNew = new ArrayList<TypeWithWeight<String>>();
		for(Tuple2<String, Integer> item : dataOld.collect()){
			dataNew.add((new TypeWithWeight<String>(item._1, item._2)));
		}
		Collections.sort(dataNew);
		return dataNew;
	}
}
