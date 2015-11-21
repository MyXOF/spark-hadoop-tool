package com.corp.tsdb.spark;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
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

	public ChannelName channelName;
	public SparkConfig config;

	private JSONArray resultYearArray;
	private JSONArray resultSeasonArray;
	private JSONArray resultDayArray;
	private boolean isYearCached;
	private boolean isSeasonCached;
	private boolean isDayCached;

	private JSONArray resultDeviceOnline;
	private JSONArray resultChannelWatched;
	private boolean isDeviceOnline;
	private boolean isChannelWatched;

	private JSONArray resultTime;
	private JSONArray resultChangeChannel;
	private boolean isTime;
	private boolean isChangeChannel;

	private SparkResultCollector() {
		channelName = ChannelName.getInstance();
		config = SparkConfig.getInstance();
		isYearCached = false;
		isSeasonCached = false;
		isDayCached = false;
		isDeviceOnline = false;
		isChannelWatched = false;
		isTime = false;
		isChangeChannel = false;
	}

	public static SparkResultCollector getInstance() {
		return COLLECTOR;
	}

	public JSONArray getPurchaseResultByYear() {
		if (isYearCached) {
			return resultYearArray;
		}
		SparkPurchaseAnalysis purchaseAnalysis = new SparkPurchaseAnalysis(
				config.APP_NAME, config.MASTER_PATH, config.JAP_PATH,
				config.Purchase_Path);
		resultYearArray = transfromToJSONArrayWithUnusedType(purchaseAnalysis
				.AnalyzeRangeByYear());
		purchaseAnalysis.shutdown();
		isYearCached = true;
		return resultYearArray;
	}

	public JSONArray getPurchaseResultBySeason() {
		if (isSeasonCached) {
			return resultSeasonArray;
		}
		SparkPurchaseAnalysis purchaseAnalysis = new SparkPurchaseAnalysis(
				config.APP_NAME, config.MASTER_PATH, config.JAP_PATH,
				config.Purchase_Path);
		resultSeasonArray = transfromToJSONArrayWithUnusedType(purchaseAnalysis
				.AnalyzeRangeBySeason());
		purchaseAnalysis.shutdown();
		isSeasonCached = true;
		return resultSeasonArray;
	}

	public JSONArray getPurchaseResultByDay() {
		if (isDayCached) {
			return resultDayArray;
		}
		SparkPurchaseAnalysis purchaseAnalysis = new SparkPurchaseAnalysis(
				config.APP_NAME, config.MASTER_PATH, config.JAP_PATH,
				config.Purchase_Path);
		resultDayArray = transfromToJSONArrayWithUnusedType(purchaseAnalysis
				.AnalyzeRangeByDay());
		purchaseAnalysis.shutdown();
		isDayCached = true;
		return resultDayArray;
	}

	public JSONArray getChannelTuneResultDeviceOnline() {
		if (isDeviceOnline) {
			return resultDeviceOnline;
		}
		SparkEventClientChannelTuneAnalysis eventClientChannelTuneAnalysis = new SparkEventClientChannelTuneAnalysis(
				config.APP_NAME, config.MASTER_PATH, config.JAP_PATH,
				config.EventClientChannelTune_Path);
		resultDeviceOnline = transfromToJSONArray(eventClientChannelTuneAnalysis
				.AnalyzeDeviceOnline());
		isDeviceOnline = true;
		eventClientChannelTuneAnalysis.shutdown();
		return resultDeviceOnline;
	}

	public JSONArray getChannelTuneResultChannelWatched() {
		if (isChannelWatched) {
			return resultChannelWatched;
		}
		SparkEventClientChannelTuneAnalysis eventClientChannelTuneAnalysis = new SparkEventClientChannelTuneAnalysis(
				config.APP_NAME, config.MASTER_PATH, config.JAP_PATH,
				config.EventClientChannelTune_Path);
		List<Tuple2<String, Integer>> list = eventClientChannelTuneAnalysis
				.AnalyzeChannel();
		List<ObjectCompareWithWeightInt<String>> listToBeSort = new ArrayList<ObjectCompareWithWeightInt<String>>();
		for (Tuple2<String, Integer> item : list) {
			if (channelName.getValue(item._1) == null) {
				continue;
			}
			listToBeSort.add(new ObjectCompareWithWeightInt<String>(channelName
					.getValue(item._1), item._2));
		}
		Collections.sort(listToBeSort);
		Collections.reverse(listToBeSort);
		resultChannelWatched = new JSONArray();
		for (ObjectCompareWithWeightInt<String> iter : listToBeSort) {
			JSONObject json = new JSONObject();
			json.put("label", iter.key);
			json.put("value", iter.weight);
			resultChannelWatched.add(json);
		}
		isChannelWatched = true;
		eventClientChannelTuneAnalysis.shutdown();
		return resultChannelWatched;
	}

	public JSONArray getProgramWatchedResultTime() {
		if (isTime) {
			return resultTime;
		}
		SparkEventClientProgramWatchedAnalysis eventClientProgramWatchedAnalysis = new SparkEventClientProgramWatchedAnalysis(
				config.APP_NAME, config.MASTER_PATH, config.JAP_PATH,
				config.EventClientProgramWatched_Path);
		List<Tuple2<String, Long>> list = eventClientProgramWatchedAnalysis
				.AnalyzeWathcTime();

		List<ObjectCompareWithWeightInt<Long>> listToBeSort = new ArrayList<ObjectCompareWithWeightInt<Long>>();
		for (Tuple2<String, Long> item : list) {
			listToBeSort.add(new ObjectCompareWithWeightInt<Long>(item._2,
					Integer.parseInt(item._1)));
		}
		Collections.sort(listToBeSort);
		resultTime = new JSONArray();
		for (ObjectCompareWithWeightInt<Long> iter : listToBeSort) {
			JSONObject json = new JSONObject();
			json.put("label", iter.weight);
			json.put("value", iter.key / 3600);
			resultTime.add(json);
		}
		isTime = true;
		eventClientProgramWatchedAnalysis.shutdown();
		return resultTime;
	}

	public JSONArray getProgramWatchedResultChannelChange() {
		if (isChangeChannel) {
			return resultChangeChannel;
		}
		SparkEventClientProgramWatchedAnalysis eventClientProgramWatchedAnalysis = new SparkEventClientProgramWatchedAnalysis(
				config.APP_NAME, config.MASTER_PATH, config.JAP_PATH,
				config.EventClientProgramWatched_Path);
		List<Tuple2<String, Tuple2<Integer, Integer>>> list = eventClientProgramWatchedAnalysis
				.AnalyzeChannelChange();
		List<ObjectCompareWithWeightInt<Integer>> listToBeSort = new ArrayList<ObjectCompareWithWeightInt<Integer>>();
		for (Tuple2<String, Tuple2<Integer, Integer>> item : list) {
			listToBeSort.add(new ObjectCompareWithWeightInt<Integer>(item._2._1
					/ item._2._2, Integer.parseInt(item._1)));
		}
		Collections.sort(listToBeSort);
		resultChangeChannel = new JSONArray();
		for (ObjectCompareWithWeightInt<Integer> iter : listToBeSort) {
			JSONObject json = new JSONObject();
			json.put("label", iter.weight);
			json.put("value", iter.key);
			resultChangeChannel.add(json);
		}
		isChangeChannel = true;
		eventClientProgramWatchedAnalysis.shutdown();
		return resultChangeChannel;
	}

	private JSONArray transfromToJSONArrayWithUnusedType(
			List<Tuple2<String, Tuple2<Integer, Double>>> list) {
		List<ObjectCompareWithDate<Integer>> listToBeSort = new ArrayList<ObjectCompareWithDate<Integer>>();
		for (Tuple2<String, Tuple2<Integer, Double>> item : list) {
			listToBeSort.add(new ObjectCompareWithDate<Integer>(item._1,
					item._2._1));
		}
		Collections.sort(listToBeSort);
		JSONArray jsonArray = new JSONArray();
		for (ObjectCompareWithDate<Integer> iter : listToBeSort) {
			JSONObject json = new JSONObject();
			json.put("label", iter.date);
			json.put("value", iter.value);
			jsonArray.add(json);
		}
		return jsonArray;

	}

	private JSONArray transfromToJSONArray(List<Tuple2<String, Integer>> list) {
		List<ObjectCompareWithDate<Integer>> listToBeSort = new ArrayList<ObjectCompareWithDate<Integer>>();
		for (Tuple2<String, Integer> item : list) {
			listToBeSort.add(new ObjectCompareWithDate<Integer>(item._1,
					item._2));
		}
		Collections.sort(listToBeSort);
		JSONArray jsonArray = new JSONArray();
		for (ObjectCompareWithDate<Integer> iter : listToBeSort) {
			JSONObject json = new JSONObject();
			json.put("label", iter.date);
			json.put("value", iter.value);
			jsonArray.add(json);
		}
		return jsonArray;

	}

	public static void dumpToFile(JSONArray dataArray, String filePath) {
		try {
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(
					filePath));
			String line = "";
			for (Object data : dataArray) {
				JSONObject object = (JSONObject) data;
				line = object.getString("label") + ","
						+ object.getString("value") + "\n";
				bufferedWriter.write(line);
				bufferedWriter.flush();
			}
			bufferedWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		 SparkResultCollector collector = SparkResultCollector.getInstance();

		 SparkResultCollector.dumpToFile(collector.getPurchaseResultByYear(), "PurchaseResultByYear");
		 SparkResultCollector.dumpToFile(collector.getPurchaseResultBySeason(), "PurchaseResultBySeason");
		 SparkResultCollector.dumpToFile(collector.getPurchaseResultByDay(), "PurchaseResultByDay");
		 SparkResultCollector.dumpToFile(collector.getProgramWatchedResultChannelChange(), "ProgramWatchedResultChannelChange");
		 SparkResultCollector.dumpToFile(collector.getProgramWatchedResultTime(), "ProgramWatchedResultTime");
		 SparkResultCollector.dumpToFile(collector.getChannelTuneResultChannelWatched(), "ChannelTuneResultChannelWatched");
		 SparkResultCollector.dumpToFile(collector.getChannelTuneResultDeviceOnline(), "ChannelTuneResultDeviceOnline");
		 
	}

}
