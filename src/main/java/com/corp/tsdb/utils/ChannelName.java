package com.corp.tsdb.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ChannelName {
	private static final ChannelName CHANNEL_NAME = new ChannelName();
	private Map<String, String> channleMap = new HashMap<String, String>();
	private final String CHANNLE_NAME_PATH = "/Users/xuyi/Documents/Eclipse_WorkSpace/spark-hadoop-tool/src/main/resources/channel";
	private BufferedReader bufferedReader;
		
	private ChannelName(){
		readChannleList();
	}
	
	public static ChannelName getInstance(){
		return CHANNEL_NAME;
	}
	
	private void readChannleList(){
		try {
			bufferedReader = new BufferedReader(new FileReader(CHANNLE_NAME_PATH));
			String stringLine = "";
			String values[]; 
			while((stringLine = bufferedReader.readLine()) != null){
				values = stringLine.trim().split("\t");
				channleMap.put(values[0], values[1]);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String getValue(String key){
		if(channleMap == null) return "";
		return channleMap.get(key);
	}
	
//	public static void main(String[] args) {
//		ChannelName service = ChannelName.getInstance();
//	}
}
