package com.corp.tsdb.spark;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSpark {
	private final String CONF_FILE_PATH = "/config.properties";
	private final static Logger logger = LoggerFactory
			.getLogger(AbstractSpark.class);
	private String APP_NAME;
	private String MASTER_PATH;
	private String JAP_PATH;

	protected String Purchase_Path;
	protected String EventClientProgramWatched_Path;
	protected String EventClientChannelTune_Path;
	
	protected SparkConf conf;
	protected JavaSparkContext sc;
	protected JavaRDD<String> lines;
	
	public AbstractSpark() {
		readConfig();
		conf = new SparkConf().setAppName(APP_NAME)
				.setMaster(MASTER_PATH).setJars(new String[] { JAP_PATH });
		sc = new JavaSparkContext(conf);
	}

	private void readConfig() {
		Properties props = new Properties();
		try (InputStream in = AbstractSpark.class
				.getResourceAsStream(CONF_FILE_PATH)) {
			props.load(in);
			APP_NAME = props.getProperty("app_name");
			MASTER_PATH = props.getProperty("master_path");
			JAP_PATH = props.getProperty("jar_path");

			Purchase_Path = props.getProperty("purchase_path");
			EventClientChannelTune_Path = props
					.getProperty("eventClientProgramWatched_path");
			EventClientProgramWatched_Path = props
					.getProperty("eventClientChannelTune_path");
		} catch (IOException e) {
			logger.error("AbstractSpark : failed to load configuration in {}",
					CONF_FILE_PATH, e);
		}
	}
}
