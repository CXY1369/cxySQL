package cxyBase;

import static java.lang.System.out;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Commands {
	
	/* This method determines what type of command the userCommand is and
	 * calls the appropriate method to parse the userCommand String. 
	 */
	public static void parseUserCommand (String userCommand) throws IOException, ParseException {
		
		/* commandTokens is an array of Strings that contains one lexical token per array
		 * element. The first token can be used to determine the type of command 
		 * The other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement 
		 */
		
		userCommand = userCommand.toLowerCase();
		ArrayList<String> commandTokens = commandStringToTokenList(userCommand);

		/*
		*  Following switch handles a very small list of hard-coded commands from SQL syntax.
		*  You will want to rewrite this method to interpret more complex commands. 
		*/
		switch (commandTokens.get(0).toLowerCase()) {
			case "show":		
				show(commandTokens);
				break;
			case "select":
				parseQuery(commandTokens);
				break;
			case "create":
				parseCreateTable(userCommand);
				break;
			case "insert":
				parseInsert(commandTokens);
				break;
			case "delete":
				parseDelete(commandTokens);
				break;
			case "update":
				parseUpdate(commandTokens);
				break;
			case "drop":
				dropTable(commandTokens);
				break;
			case "help":
				help();
				break;
			case "version":
				displayVersion();
				break;
			case "exit":
				Settings.setExit(true);
				break;
			case "quit":
				Settings.setExit(true);
				break;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}

	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + Settings.getVersion());
		System.out.println(Settings.getCopyright());
	}
	
	public static boolean tableExists(String tableName)
	{
		File dataDir = new File("data"+File.separator+"user_data");
		if(!dataDir.exists()) {
			dataDir.mkdir();// create a folder along the path dataDir
		}
		
		String[] oldTableFiles; // String list of table file names under the folder "data"
		oldTableFiles = dataDir.list();
		
		for (int i=0; i<oldTableFiles.length; i++) {
			if(oldTableFiles[i].equals(tableName))
			{
				return true;
			}
		}
		
		dataDir = new File("data"+File.separator+"catalog");
		if(!dataDir.exists()) {
			dataDir.mkdir();// create a folder along the path dataDir
		}
		oldTableFiles = dataDir.list();
		
		for (int i=0; i<oldTableFiles.length; i++) {
			if(oldTableFiles[i].equals(tableName))
			{
				return true;
			}
		}
		
		return false;
	}

	/**********************************************************************************************************
	 *  Stub method for showing current tables in storage.
	 */
	public static void show(ArrayList<String> commandTokens) {
		
		File catalogDir = new File("data"+File.separator+"catalog");
		File dataDir = new File("data"+File.separator+"user_data");
		if(!dataDir.exists()) {
			dataDir.mkdir();// create a folder along the path dataDir
		}
		
		// String list of table file names under the folder "data/catalog" and "data/user_data"
		String[] oldTableFiles;
		
		int arryLen1=catalogDir.list().length;// get the length of catalogDir.list()
		int arryLen2=dataDir.list().length;// get the length of dataDir.list()
		oldTableFiles= Arrays.copyOf(catalogDir.list(),arryLen1+ arryLen2);
		System.arraycopy(dataDir.list(), 0, oldTableFiles, arryLen1,arryLen2 );// merge the name lists from two directory
		
		System.out.println("Table_Names");
		System.out.println(Utils.printSeparator("-",20));
		
		for (int i=0; i<oldTableFiles.length; i++) {
			if(oldTableFiles[i].contains(".tbl")) {
				System.out.println(oldTableFiles[i]);
			}
		}
		
	}

	/**********************************************************************************************************
	 *  Stub method for creating tables.
	 * @throws IOException 
	 */
	public static void parseCreateTable(String command) throws IOException{
				
		ArrayList<String> commandTokens = commandStringToTokenList(command);

		/* Extract the table name from the command string token list */
		String tableFileName = commandTokens.get(2).toLowerCase() + ".tbl";
		

		/* Check if the table already exists, before attempting to create new table file */
		if(tableExists(tableFileName)) {
			System.out.println("This table Already exists, please change a name !");
			return;
		}
		
		/** Check if there is an open bracket after table name. If not, then it is a syntax error*/
		if(!commandTokens.get(3).equals("("))
		{
			System.out.println("incorrect query statement.\n "
					+ "create table tablename ( col_name data_type [NOT NULL],*);");
			return;
		}
		
		
		List<Object> columnInfoSet = new ArrayList<>();// this is the info-set to insert in column Metadata
		List<String> columnDescription = new ArrayList<String>();
		ArrayList<String> columnInfoTokens = new ArrayList<String>();
		
		for(int i=4;i<commandTokens.size();i++){
			
			if( commandTokens.get(i).equals(",") || commandTokens.get(i).equals(")") ) {
				
				if(columnInfoTokens.size()<2) {
					System.out.println("Incorrect statement, at least include the column_name and data type.\n"
							+ "Example:\n"
							+ "create table tablename ( col_name data_type [NOT NULL] [UNIQUE] [PRIMARY KEY]);\n"
							+ "Column constraints are optional");
					return;
				}
				
				columnDescription.add(commandTokens.get(2).toLowerCase());   //get the table name				
				columnDescription.add(columnInfoTokens.get(0));//get the column name
				columnDescription.add(columnInfoTokens.get(1));//get the column's data type
				if(columnInfoTokens.contains("not") && columnInfoTokens.contains("null")) {
					columnDescription.add("not null");
				}
				if(columnInfoTokens.contains("unique")) {
					columnDescription.add("unique");
				}
				if(columnInfoTokens.contains("primary") && columnInfoTokens.contains("key")) {
					columnDescription.add("primary key");
				}
				
				
				columnInfoSet.add(columnDescription);
				/**
				 * Notice: The columnDescription is List, a changeable object
				 * When columnDescription changes, the columnInfoSet changes as well
				 * so cannot clear columnInfoSet, we should just create a new one
				 * **/
//				columnDescription.clear();// clear the current column definition and prep for next one (Which is wrong operation here)
				columnDescription = new ArrayList<String>();// correct operation
				
				columnInfoTokens.clear();// clear the current column definition and prep for next one
				continue;
			}
			columnInfoTokens.add(commandTokens.get(i).toLowerCase());
		}
		
		/** Code to insert an entry in the TABLES meta-data for this new table.
		 *  i.e. New row in davisbase_tables if you're using that mechanism for meta-data.
		 */
		RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data"+ File.separator + "catalog"+ File.separator+"davisbase_tables.tbl", "rw");
		createTableCatalog.insertTableCatlog(commandTokens.get(2).toLowerCase(), davisbaseTablesCatalog);
		davisbaseTablesCatalog.close();
				
		/** Code to insert entries in the COLUMNS meta data for each column in the new table.
		 *  i.e. New rows in davisbase_columns if you're using that mechanism for meta-data.
		 */
		RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile("data"+ File.separator + "catalog"+ File.separator+"davisbase_columns.tbl", "rw");
		createColumnCatalog.insertColumnCatalog(columnInfoSet, davisbaseColumnsCatalog);
		davisbaseColumnsCatalog.close();
		
		/*  Code to create a .tbl file under data/user_data  */
		try {
			/*  Create RandomAccessFile tableFile in read-write mode.
			 *  Note that this doesn't create the table file in the correct directory structure
			 */

			/* Create a new table file whose initial size is one page (i.e. page size number of bytes) */
			RandomAccessFile tableFile = new RandomAccessFile("data"+ File.separator +"user_data"+File.separator+ tableFileName, "rw");
			tableFile.setLength(Settings.getPageSize());

			PageFormat.initializeHeader(tableFile);
			
			tableFile.close();
		}
		catch(Exception e) {
			System.out.println(e);
		}	
		
		System.out.println("Table created !");
		return;
	}
	

	/***********************************************************************************************************
	 *  Stub method for inserting a new record into a table.
	 */
	public static void parseInsert (ArrayList<String> commandTokens) throws IOException, ParseException {
		
		int valueStartingPosition=0;
		ArrayList<String> columns = new ArrayList<String>();
		ArrayList<String> values = new ArrayList<String>();
		String tableName = commandTokens.get(2).toLowerCase();
		
		if(!tableExists(tableName+".tbl"))
		{
			System.out.println("Table does not exist in the catalog");
			return;
		}
		
		if(!commandTokens.get(3).equals("("))
		{
			// make it easier to parse the command
			System.out.println("Incorrect query statement. Please follow the format below.\n"
					+ "Example:\n"
					+ "insert into table_name   (col_1,col_2,... ) \n"
					+ "                  values (val_1,val_2,...)");
			return;
		}
		
		for(int i = 4; !commandTokens.get(i).equals(")"); i++)
		{
			if( commandTokens.get(i).equals(",") ) {
				continue;// ignore the "," in the command
			}
			columns.add(commandTokens.get(i).toLowerCase());// get the mentioned columns in lower case
			
			/** after the last column, there are 3 elements in the query statement before starting value
			 *  ), 
			 *  values, 
			 *  (  
			 *  */
			valueStartingPosition = i+4;// do it in this for-loop is easier for writing the code
		}
		
		for(int i = valueStartingPosition; !commandTokens.get(i).equals(")");i++) {
			if(commandTokens.get(i).equals(",")) {
				continue;
			}
			values.add(commandTokens.get(i).toLowerCase()); //// get the inserting values
		}
		
		try {
			if(values.size()!=columns.size())
			{
				System.out.println("Value-Column size mismatch,please check");
				return;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		
		/*insert the cell to the page*/
		PageFormat.pageInsertUpdate(tableName,columns,values);
			
		System.out.println("Insertion finished !");
		return;
	}
	
	
	
	/*************************************************************************************************************
	 *  Stub method for deleting a new record into a table.
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void parseDelete(ArrayList<String> commandTokens) throws IOException, ParseException {
		
		String targetTableName = commandTokens.get(2);
		if(!tableExists(targetTableName+".tbl")) {
			System.out.println("Table does not exist !");
			return;
		}
		
		ArrayList<String> conditionTokens = new ArrayList<String>();// token part of conditional statement
		
		for(int i=4;i<commandTokens.size();i++) {
			conditionTokens.add(commandTokens.get(i));
		}
				
		PageFormat.pageDeleteUpdate(targetTableName, conditionTokens);
		
		System.out.println("Deletion finished !");
		return;
	}
	

	/*************************************************************************************************************
	 *  Stub method for dropping tables
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public static void dropTable(ArrayList<String> commandTokens) throws IOException, ParseException {
		
		String targetTableName = commandTokens.get(2);
		
		if(!tableExists(targetTableName+".tbl")) {
			System.out.println("Table does not exist !");
			return;
		}
		
		if( targetTableName.equals("davisbase_tables") || targetTableName.equals("davisbase_columns") ) {
			System.out.println("Drop Table failed, this is system catalog file !");
			return;
		}
		
		
		File targetTable = new File("data"+ File.separator +"user_data"+File.separator+ targetTableName+".tbl");
		
		System.gc(); //draw the file back from JVM
		targetTable.delete();
		
		if( tableExists(targetTableName+".tbl") ){
			System.out.println("Drop Table failed !");
			return;
		}
		
		System.out.println("Drop successfully !");
		
		/**delete table-info from catalog system files
		 * **/
		createTableCatalog.deleteFromTableCatalog( targetTableName );// remove info from davisbase_tables
		createColumnCatalog.deleteFromColumnCatalog(targetTableName);//remove info from davisbase_columns
		
	}

	/*************************************************************************************************************
	 *  Stub method for executing queries
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void parseQuery(ArrayList<String> commandTokens) throws IOException, ParseException {
		
		ArrayList<String> targetColumns = new ArrayList<String>();
		String targetTableName = "";
		ArrayList<String> conditions = new ArrayList<String>();
		
		int tableNamePosition = 0;
		for(int i=1;i<commandTokens.size() ;i++) {
			
			if(commandTokens.get(i).equals("from")) {break;}
			
			if( !commandTokens.get(i).equals(",") ) {
				targetColumns.add(commandTokens.get(i));
			}
			tableNamePosition = i+2;
		}
		
		
		targetTableName = commandTokens.get(tableNamePosition).toLowerCase();
		if(targetTableName.equals("where") ) {
			System.out.println("Please specify a table.");
			return;
		}
		
		
		if(!tableExists(targetTableName+".tbl")) {
			System.out.println("Table doesn't exist, please call the correct table or create a new table.");
			return;
		}
				
		/**check if there are conditions need to meet
		 * */
		if(tableNamePosition+2 < commandTokens.size()) {
			int conditionPosition = tableNamePosition+2;
			while(conditionPosition < commandTokens.size() ) {
				conditions.add(commandTokens.get(conditionPosition));
				conditionPosition += 1;
			}
		}
		
		if(conditions.size()!=0 && (conditions.size()+1)%4 != 0 ) {
			System.out.println("Incomplete conditions !");
			return;
		}
		
		//debug
		//System.out.println("Debug Info: The size of condition token is: "+conditions.size());//debug
		
		/**Read responding records
		 * */
		CellFormat.readRecord(targetTableName, targetColumns, conditions);
		
		return;
	}

	/*************************************************************************************************************
	 *  Stub method for updating records
	 *  @param updateString is a String of the user input
	 */
	public static void parseUpdate(ArrayList<String> commandTokens) {
		System.out.println("Command: " + tokensToCommandString(commandTokens));
		System.out.println("Stub: This is the parseUpdate method");
	}

	
	/**********************************************************************************************************
	 * Process the command
	 * */
	
	public static String tokensToCommandString (ArrayList<String> commandTokens) {
		String commandString = "";
		for(String token : commandTokens)
			commandString = commandString + token + " ";
		return commandString;
	}
	
	public static ArrayList<String> commandStringToTokenList (String command) {
		
		/* Clean up command string so that each token is separated by a single space */ 		
		command = command.replaceAll("\n", " ");    // Remove newlines
		command = command.replaceAll("\r", " ");    // Remove carriage returns
		command = command.replaceAll( "," , " , ");   // Tokenize commas
		command = command.replaceAll("\\(", " ( "); // Tokenize left parentheses
		command = command.replaceAll("\\)", " ) "); // Tokenize right parentheses
		command = command.replaceAll("( )+", " ");  // Reduce multiple spaces to a single space
		
		ArrayList<String> tokenizedCommand = new ArrayList<String>(Arrays.asList(command.split(" ")));// split by space
		return tokenizedCommand;
	}

	/**
	 *  Help: Display supported commands
	 */
	public static void help() {
		out.println(Utils.printSeparator("*",80));
		out.println("SUPPORTED COMMANDS\n");
		out.println("All commands below are case insensitive\n");
		out.println("SHOW TABLES;");
		out.println("\tDisplay the names of all tables.\n");
		out.println("SELECT ⟨column_list⟩ FROM table_name [WHERE condition];\n");
		out.println("\tDisplay table records whose optional condition");
		out.println("\tis <column_name> = <value>.\n");
		out.println("INSERT INTO (column1, column2, ...) table_name VALUES (value1, value2, ...);\n");
		out.println("\tInsert new record into the table.");
		out.println("UPDATE <table_name> SET <column_name> = <value> [WHERE <condition>];");
		out.println("\tModify records data whose optional <condition> is\n");
		out.println("DROP TABLE table_name;");
		out.println("\tRemove table data (i.e. all records) and its schema.\n");
		out.println("VERSION;");
		out.println("\tDisplay the program version.\n");
		out.println("HELP;");
		out.println("\tDisplay this help information.\n");
		out.println("EXIT;");
		out.println("\tExit the program.\n");
		out.println(Utils.printSeparator("*",80));
	}
	
}

