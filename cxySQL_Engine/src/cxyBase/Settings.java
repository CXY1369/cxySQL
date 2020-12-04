package cxyBase;

import java.io.File;
import java.io.RandomAccessFile;

public class Settings {
	static String prompt = "CXYsql> ";
	static String version = "v1.0";
	static String copyright = "@2020 Xingyu Chen";
	static boolean isExit = false;
	
	/*
	 * Page size for all files is 512*n bytes by default.
	 * You may choose to make it user modifiable
	 */
	static int pageSize = 512*63;
	
	/** ***********************************************************************
	 *  Static method definitions
	 */

	
	public static boolean isExit() {
		return isExit;
	}

	public static void setExit(boolean e) {
		isExit = e;
	}
	
	public static String getPrompt() {
		return prompt;
	}

	public static void setPrompt(String s) {
		prompt = s;
	}

	public static String getVersion() {
		return version;
	}

	public static void setVersion(String version) {
		Settings.version = version;
	}

	public static String getCopyright() {
		return copyright;
	}

	public static void setCopyright(String copyright) {
		Settings.copyright = copyright;
	}

	public static int getPageSize() {
		return pageSize;
	}

	public static void setPageSize(int pageSize) {
		Settings.pageSize = pageSize;
	}
	
	/**
	 * The following static method creates the DavisBase data storage container
	 * and then initializes two system tables:
	 * davisbase_tables.tbl and davisbase_columns.tbl
	 * 
	 *  WARNING! Calling this method will destroy the system database
	 *           catalog files if they already exist.
	 */
	public static void initializeDataStore() {

		/** Create data directory at the current OS location to hold */
		try {
			File dataDir = new File("data");
			if(!dataDir.exists()) {
				dataDir.mkdir();// create a folder along the path dataDir
			}
			dataDir = new File("data"+File.separator+"catalog");
			if(!dataDir.exists()) {
				dataDir.mkdir();// create a folder along the path dataDir
			}
		}
		catch (SecurityException se) {
			System.out.println("Unable to create data container directory");
			System.out.println(se);
		}
		
		/** Create davisbase_tables catalog */
		String davisbase_tables_path = "data"+File.separator+"catalog"+File.separator+"davisbase_tables.tbl";
		if(!Commands.tableExists(davisbase_tables_path)) {
			createTableCatalog.initializeTablePage();
		}

		
		/** Create davisbase_columns systems catalog */
		String davisbase_columns_path = "data"+File.separator+"catalog"+File.separator+"davisbase_columns.tbl";
		if(!Commands.tableExists(davisbase_columns_path)) {
			createColumnCatalog.initializeColumnCatalog();
		}
		
	}


	

	/** ***********************************************************************/
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
}