package com.corp.tsdb.spark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.corp.tsdb.utils.ChannelName;
import com.corp.tsdb.utils.ObjectCompareWithDate;
import com.corp.tsdb.utils.ObjectCompareWithWeightInt;

import scala.Tuple2;

public class SparkResultCollector {
	private final static SparkResultCollector COLLECTOR = new SparkResultCollector();

	private SparkPurchaseAnalysis purchaseAnalysis;
	private SparkEventClientChannelTuneAnalysis eventClientChannelTuneAnalysis;
	private SparkEventClientProgramWatchedAnalysis eventClientProgramWatchedAnalysis;
	private ChannelName channelName;
	private SparkConfig config;

	private SparkResultCollector() {
		channelName = ChannelName.getInstance();
		config = SparkConfig.getInstance();
		purchaseAnalysis = new SparkPurchaseAnalysis(config.APP_NAME,
				config.MASTER_PATH, config.JAP_PATH, config.Purchase_Path);
		eventClientChannelTuneAnalysis = new SparkEventClientChannelTuneAnalysis(
				config.APP_NAME, config.MASTER_PATH, config.JAP_PATH,
				config.EventClientChannelTune_Path);
		eventClientProgramWatchedAnalysis = new SparkEventClientProgramWatchedAnalysis(
				config.APP_NAME, config.MASTER_PATH, config.JAP_PATH,
				config.EventClientProgramWatched_Path);
	}

	public static SparkResultCollector getInstance() {
		return COLLECTOR;
	}

	public JSONArray getPurchaseResultByYear() {
		return transfromToJSONArrayWithUnusedType(purchaseAnalysis.AnalyzeRangeByYear());
	}

	public JSONArray getPurchaseResultBySeason() {
		return transfromToJSONArrayWithUnusedType(purchaseAnalysis.AnalyzeRangeBySeason());
	}

	public JSONArray getPurchaseResultByDay() {
		return transfromToJSONArrayWithUnusedType(purchaseAnalysis.AnalyzeRangeByDay());
	}

	public JSONArray getChannelTuneResultDeviceOnline() {
		return transfromToJSONArray(eventClientChannelTuneAnalysis.AnalyzeDeviceOnline());
	}

	public JSONArray getChannelTuneResultChannelWatched() {
		List<Tuple2<String, Integer>> list = eventClientChannelTuneAnalysis.AnalyzeChannel();
		List<ObjectCompareWithWeightInt<String>> listToBeSort = new ArrayList<ObjectCompareWithWeightInt<String>>();
		for(Tuple2<String, Integer> item: list){
			if(channelName.getValue(item._1) == null){
				continue;
			}
			listToBeSort.add(new ObjectCompareWithWeightInt<String>(channelName.getValue(item._1), item._2));
		}
		Collections.sort(listToBeSort);
		JSONArray jsonArray = new JSONArray();
		for(ObjectCompareWithWeightInt<String> iter : listToBeSort){
			JSONObject json = new JSONObject();
			json.put("label", iter.key);
			json.put("value", iter.weight);
			jsonArray.add(json);
		}
		return jsonArray;
	}

	public JSONArray getProgramWatchedResultTime() {
		List<Tuple2<String, Long>> list = eventClientProgramWatchedAnalysis.AnalyzeWathcTime();
		List<ObjectCompareWithWeightInt<Long>> listToBeSort = new ArrayList<ObjectCompareWithWeightInt<Long>>();
		for(Tuple2<String, Long> item : list){
			listToBeSort.add(new ObjectCompareWithWeightInt<Long>(item._2, Integer.parseInt(item._1)));
		}
		Collections.sort(listToBeSort);
		JSONArray jsonArray = new JSONArray();
		for(ObjectCompareWithWeightInt<Long> iter : listToBeSort){
			JSONObject json = new JSONObject();
			json.put("label", iter.weight);
			json.put("value", iter.key);
			jsonArray.add(json);
		}
		return jsonArray;
	}

	public JSONArray getProgramWatchedResultChannelChange() {
		List<Tuple2<String, Tuple2<Integer, Integer>>> list=  eventClientProgramWatchedAnalysis.AnalyzeChannelChange();
		List<ObjectCompareWithWeightInt<Integer>> listToBeSort = new ArrayList<ObjectCompareWithWeightInt<Integer>>();
		for(Tuple2<String, Tuple2<Integer, Integer>> item : list){
			listToBeSort.add(new ObjectCompareWithWeightInt<Integer>(item._2._1 / item._2._2, Integer.parseInt(item._1)));
		}
		Collections.sort(listToBeSort);
		JSONArray jsonArray = new JSONArray();
		for(ObjectCompareWithWeightInt<Integer> iter : listToBeSort){
			JSONObject json = new JSONObject();
			json.put("label", iter.weight);
			json.put("value", iter.key);
			jsonArray.add(json);
		}
		return jsonArray;
	}

	private JSONArray transfromToJSONArrayWithUnusedType(
			List<Tuple2<String, Tuple2<Integer, Double>>> list) {
		List<ObjectCompareWithDate<Integer>> listToBeSort = new ArrayList<ObjectCompareWithDate<Integer>>();
		for(Tuple2<String, Tuple2<Integer, Double>> item : list){
			listToBeSort.add(new ObjectCompareWithDate<Integer>(item._1,item._2._1));
		}
		Collections.sort(listToBeSort);
		JSONArray jsonArray = new JSONArray();
		for(ObjectCompareWithDate<Integer> iter : listToBeSort){
			JSONObject json = new JSONObject();
			json.put("label", iter.date);
			json.put("value", iter.value);
			jsonArray.add(json);
		}
		return jsonArray;

	}
	
	private JSONArray transfromToJSONArray(List<Tuple2<String, Integer>> list) {
		List<ObjectCompareWithDate<Integer>> listToBeSort = new ArrayList<ObjectCompareWithDate<Integer>>();
		for(Tuple2<String, Integer> item : list){
			listToBeSort.add(new ObjectCompareWithDate<Integer>(item._1,item._2));
		}
		Collections.sort(listToBeSort);
		JSONArray jsonArray = new JSONArray();
		for(ObjectCompareWithDate<Integer> iter : listToBeSort){
			JSONObject json = new JSONObject();
			json.put("label", iter.date);
			json.put("value", iter.value);
			jsonArray.add(json);
		}
		return jsonArray;

	}
	
	public static void main(String[] args) {
//		SparkResultCollector collector = SparkResultCollector.getInstance();
//		System.out.println(collector.getPurchaseResultByDay());
//		System.out.println(collector.getPurchaseResultBySeason());
//		System.out.println(collector.getPurchaseResultByYear());
//		System.out.println(collector.getProgramWatchedResultChannelChange());
//		System.out.println(collector.getProgramWatchedResultTime());
//		System.out.println(collector.getChannelTuneResultChannelWatched());
//		System.out.println(collector.getChannelTuneResultDeviceOnline());
	}

}
