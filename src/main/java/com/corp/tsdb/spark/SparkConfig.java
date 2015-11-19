package com.corp.tsdb.spark;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkConfig{
	private final static SparkConfig CONFIG = new SparkConfig();
	
	public static SparkConfig getInstance(){
		return CONFIG;
	}
	
	private final String CONF_FILE_PATH = "/config.properties";
	private final static Logger logger = LoggerFactory.getLogger(SparkConfig.class);
	public String APP_NAME;
	public String MASTER_PATH;
	public String JAP_PATH;
	
	public String Purchase_Path;
	public String EventClientProgramWatched_Path;
	public String EventClientChannelTune_Path;
	
	private SparkConfig() {
		readConfig();
	}

	private void readConfig() {		
		try (InputStream in = SparkConfig.class.getResourceAsStream(CONF_FILE_PATH)) {
			Properties props = new Properties();
			if(in == null){
				return;
			}
			props.load(in);
			APP_NAME = props.getProperty("app_name");
			MASTER_PATH = props.getProperty("master_path");
			JAP_PATH = props.getProperty("jar_path");

			Purchase_Path = props.getProperty("purchase_path");
			EventClientChannelTune_Path = props
					.getProperty("eventClientChannelTune_path");
			EventClientProgramWatched_Path = props
					.getProperty("eventClientProgramWatched_path");
		} catch (IOException e) {
			logger.error("AbstractSpark : failed to load configuration in {}",
					CONF_FILE_PATH, e);
		}
	}
	
	public static void main(String[] args) {
		SparkConfig config = SparkConfig.getInstance();
		
		System.out.print(config.APP_NAME);
	}
}
