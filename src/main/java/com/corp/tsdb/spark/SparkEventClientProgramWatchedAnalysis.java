package com.corp.tsdb.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.corp.tsdb.utils.ObjectCompareWithWeightInt;

import scala.Tuple2;

public class SparkEventClientProgramWatchedAnalysis implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3716424860947761439L;
	private boolean isWatchTimeCached;
	private boolean isChangeChannelCached;

	private transient SparkConf conf;
	private transient JavaSparkContext sc;
	private JavaRDD<String> lines;

	private JavaPairRDD<String, Long> watchTimeResult;
	private JavaPairRDD<String, Tuple2<Integer, Integer>> changeChannelResult;
	
	private String APP_NAME;
	private String MASTER_PATH;
	private String JAR_PATH;
	private String FILE_PATH;

	public SparkEventClientProgramWatchedAnalysis(String app_name, String master_path,
			String jar_path, String file_path) {
		this.APP_NAME = app_name;
		this.MASTER_PATH = master_path;
		this.JAR_PATH = jar_path;
		this.FILE_PATH = file_path;
		conf = new SparkConf()
				.setAppName(APP_NAME)
				.setMaster(MASTER_PATH)
				.setJars(
						new String[] { JAR_PATH })
				.set("spark.serializer",
						"org.apache.spark.serializer.KryoSerializer");
		sc = new JavaSparkContext(conf);
		lines = sc.textFile(FILE_PATH);

		isChangeChannelCached = false;
		isWatchTimeCached = false;
	}

	public List<Tuple2<String, Long>> AnalyzeWathcTime() {
		if (isWatchTimeCached) {
			watchTimeResult.collect();
		}

		JavaPairRDD<String, Long> wathcTimeInfo = lines
				.mapToPair(new PairFunction<String, String, Long>() {
					@Override
					public Tuple2<String, Long> call(String t) throws Exception {
						String values[] = t.split(",");
						Long time = Long.parseLong(values[5]) / 1000;
						return new Tuple2<String, Long>(values[1], time);
					}
				});

		watchTimeResult = wathcTimeInfo
				.reduceByKey(new Function2<Long, Long, Long>() {
					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});
		isWatchTimeCached = true;
		return watchTimeResult.collect();
	}

	public  List<Tuple2<String,Tuple2<Integer,Integer>>> AnalyzeChannelChange() {
		if (isChangeChannelCached) {
			return changeChannelResult.collect();
		}
		JavaPairRDD<Tuple2<String, String>, Integer> changeChannelInfo = lines
				.mapToPair(new PairFunction<String, Tuple2<String, String>, Integer>() {
					@Override
					public Tuple2<Tuple2<String, String>, Integer> call(String t)
							throws Exception {
						String values[] = t.split(",");
						return new Tuple2<Tuple2<String, String>, Integer>(
								new Tuple2<String, String>(values[1], values[3]),
								1);
					}
				});

		JavaPairRDD<Tuple2<String, String>, Integer> changeChannelTmp = changeChannelInfo
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer v1, Integer v2)
							throws Exception {
						return v1 + v2;
					}
				});

		JavaPairRDD<String, Tuple2<Integer, Integer>> changeChannelSumInfo = changeChannelTmp
				.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Tuple2<Integer, Integer>>() {
					@Override
					public Tuple2<String, Tuple2<Integer, Integer>> call(
							Tuple2<Tuple2<String, String>, Integer> t)
							throws Exception {
						return new Tuple2<String, Tuple2<Integer, Integer>>(
								t._1._1, new Tuple2<Integer, Integer>(t._2, 1));
					}
				});

		changeChannelResult = changeChannelSumInfo
				.reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
					@Override
					public Tuple2<Integer, Integer> call(
							Tuple2<Integer, Integer> v1,
							Tuple2<Integer, Integer> v2) throws Exception {
						return new Tuple2<Integer, Integer>(v1._1 + v2._1,
								v1._2 + v2._2);
					}
				});

		isChangeChannelCached = true;
		return changeChannelResult.collect();
	}
	
	public void shutdown(){
		sc.stop();
	}

	public static void main(String[] args) {
		SparkEventClientProgramWatchedAnalysis analysis = new SparkEventClientProgramWatchedAnalysis(
				"Spark Test",
				"spark://demo2:7077",
				"/Users/xuyi/Documents/Eclipse_WorkSpace/spark-hadoop-tool/target/spark-hadoop-tool.jar",
				"hdfs://demo2:9000/user/ubuntu/EventClientProgramWatchedSmall.csv");
		// SparkPurchaseAnalysis analysis = SparkPurchaseAnalysis.getInstance();
		System.out.println(analysis.AnalyzeChannelChange());
		System.out.println(analysis.AnalyzeWathcTime());
		System.out.println(analysis.AnalyzeChannelChange());
		System.out.println(analysis.AnalyzeWathcTime());
		analysis.shutdown();
	}
}
