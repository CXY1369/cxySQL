package cxyBase;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class CellFormat {
			
	/*****
	 * get the inserting Cell info from the Metadata
	 * ****/
	public static List<Object> getPayloadInfo(String targetTableName,ArrayList<String> columns,ArrayList<String> values) throws IOException {
		
		List<Object> payloadInfo = new ArrayList<>();
		
		int payloadSize = 1;
		
		/*To store the data type of each column defined in target table*/
		ArrayList<Integer> columnTypeCodeInTargetTable = new ArrayList<Integer>();
		/*reorder the inserting values based on the ordinal position of the columns defined in target table*/
		String[] reorderValues =new String[values.size()];
		
		/**User may not follow the original order
		 * the ordinal position of inserting values*/
		int[] ordinalOfInsert = new int[ values.size() ];
		
		
		/********************************************
		 * 
		 * 
		 * *
		 * *******************************************************/
		
		// open the davisbase_columns.tbl to check the column Meta data
		RandomAccessFile columnMeta = new RandomAccessFile("data"+File.separator+"catalog"+File.separator+"davisbase_columns.tbl", "rw");
		ArrayList<Integer> cellPos_list = PageFormat.getCellStartPos(columnMeta);
		
		
		ArrayList<Integer> columnDataTypes = new ArrayList<Integer>();// column data types code in davisbase_columns
		int cellPos;
		int cellHeaderSize=6;// cell header size
		int recordHeaderSize =8;// record header size in davisbase_columns
		for(int i=0;i<cellPos_list.size();i++) {
			
			cellPos = cellPos_list.get(i);
			
			columnMeta.seek(cellPos+cellHeaderSize+recordHeaderSize);// pointer to record Body in the page of davisbase_columns
			
			/** get the table name from record of davisbase_columns
			 * */
			byte[] b = new byte[targetTableName.length()];
			columnMeta.read(b);
			String tableName = new String(b); 
			
			// check if we find the target table in the column Metadata
			if( !tableName.equals(targetTableName) ) {
				continue;
			}
			
			/**In column Metadata of davaisbase_columns.tbl 
			 * The table_name repeating-times equals number of columns defined in the target table
			 * one column takes 1 byte in record Header
			 * */
			payloadSize += 1;
			
			columnMeta.seek(cellPos+6);// pointer to record Header of davaisbase_columns.tbl
			int numCol = columnMeta.read();// read one byte, the number of columns in davaisbase_columns.tbl
			
			/* get all the data types of each column in Metadata davaisbase_columns.tbl*/
			columnDataTypes.clear();// clear the data type codes from last record
			
			for(int j=0;j<numCol;j++) {
				/*data type of current column in this record in davaisbase_columns.tbl*/
				int dataType = columnMeta.read();// one byte
				columnDataTypes.add(dataType);
			}
			
			/* check if the inserting columns match the columns from Metadata
			 * pointer to column_name position
			 * by skipping the record Header & table_name bytes
			 * cell header takes 6 bytes
			 * record header takes (1+numCol) bytes
			 * */
			columnMeta.seek(cellPos+ cellHeaderSize + recordHeaderSize + targetTableName.length()); 
			
			/*get string size of this column_name, which is after table_name in Metadata*/
			int stringsize_colName = columnDataTypes.get(1)-12;// column_name is TEXT type
			b = new byte[stringsize_colName];
			columnMeta.read(b);
			String columnName = new String(b);
			
			/**
			 * get the data-type code
			 * */
			int dataType_nameLength = columnDataTypes.get(2)-12;// data_type name is TEXT type
			byte[] buf = new byte[dataType_nameLength];
			columnMeta.read(buf);
			String dataType_name = new String(buf);// get data type name of this column
			int datatypecode = DataTypeFormat.getDatatypeCode(dataType_name) ;//one byte, get data type of inserting column
			
			//debug debug debug debug
			//System.out.println("Debug Info: Checked data type of "+columnName+" in "+targetTableName+" is "+dataType_name +" from Metadata");//debug
			//debug debug debug debug
			
			/**if columnName is mentioned in query statement
			 * */
			if(columns.contains(columnName)) {
								
				/*If this inserting column is not TEXT, then the data size is fixed and can be found easily*/
				if(datatypecode!=12) {
					columnTypeCodeInTargetTable.add(datatypecode);
					payloadSize += DataTypeFormat.datatypeCode_toSize(datatypecode);
				}
				
				
				int ordinalPos = columnMeta.read();//one byte, the ordinal position of this inserting column
				//debug
				//System.out.println("The ordinal position of "+ columnName +" is: "+ordinalPos);//debug
				//debug
				
				int isNotNull = columnMeta.read();// one byte
				int isUnique = columnMeta.read();// one byte
				
				int isPrimKey = columnMeta.read();// one byte
				
				
				/**find the position of this column mentioned in the statement
				 * */
				for(int k=0;k<columns.size();k++) {
					if(columns.get(k).equals(columnName) ) {
						// check if violate primary key constraints
						if(isPrimKey==1) {
							if(  checkPrimkey(targetTableName, values.get(k), ordinalPos)  ) {
								System.out.println("Insertion Failed!\n"
										+ "Violate Primary Key constraint, please check value of "+columnName);
								payloadInfo.add(-1);
								return payloadInfo;
							}
						}
						
						ordinalOfInsert[k] = ordinalPos;
						// if this inserting column is TEXT, 
						// then the data size is the length of responding string in stated values
						if(datatypecode==12) {
							datatypecode = 12+ values.get(k).length();
							columnTypeCodeInTargetTable.add(datatypecode);
							payloadSize += values.get(k).length();
						}
						break;
					}
				}
				
			}
			else {
				columnMeta.skipBytes(1);// skip 1 byte of the ordinal position
				int isNotNull = columnMeta.read();// one byte
				
				/*if this column cannot be null, but now it's missing in the query statement, reject!*/
				if(isNotNull==1) {
					System.out.println("Violate NOT NULL constraint, please double check!\n"
							+ "You need add value for "+ columnName+".");
					
					payloadInfo.add(-1);
					return payloadInfo;
				}
				
				/*if this column Nullable and is not in the inserting statement*/
				columnTypeCodeInTargetTable.add(0);
			}
			
			
		}
		
		/**
		 * Reorder the values according to the ordinal position defined in Metadata
		 * In case users may randomly call the columns
		 * */
		for(int i=0;i<values.size();i++) {
			int adaptedPos = ordinalOfInsert[i]-1;// Notice: ordinal position starts from 1
			reorderValues[adaptedPos]=values.get(i);
		}
		
		
		payloadInfo.add(payloadSize);
		payloadInfo.add(columnTypeCodeInTargetTable);
		payloadInfo.add(reorderValues);
		
		
		return payloadInfo;
		
	}
	
	/*********************************************************************************************************************/
	/*********************************************************************************************************************/

	public static void writeRecord(RandomAccessFile targetTable, int filePointer,int datatypeCode,String value) throws IOException, ParseException {
		
		targetTable.seek(filePointer);
		
		/*Just write in the string text*/
		if(datatypeCode>=12) {
			targetTable.writeBytes(value);
			return;
		}
		
		/*If the data type is YEAR*/
		if(datatypeCode==8) {
			int val = Integer.valueOf(value).intValue()-2000;
			if(val<-128 || val>127) {
				System.out.println("Invalid YEAR, should be within 1872~2127.");
				return;
			}
			targetTable.writeByte(val);
			return;
		}
		
		/*if the data type is epoch Datetime*/
		if(datatypeCode==10) {
			if(value.length()!=19) {
				System.out.println("Invalid Datetime, should be yyyy-MM-dd HH:mm:ss ");
				return;
			}
			
			long epoch = new java.text.SimpleDateFormat(value).parse("1970-01-01 00:00:00").getTime();
			targetTable.writeLong(epoch);
			return;
		}
		
		/*if data type is date*/
		if(datatypeCode==11) {
			int Hour= Integer.valueOf(value.substring(0, 2)).intValue();
			int Min= Integer.valueOf(value.substring(3, 5)).intValue();
			int Sec= Integer.valueOf(value.substring(6, 8)).intValue();
			
			long time = (long) (Sec+Min*60+Hour*3600)*1000;
			targetTable.writeLong(time);
			return;
		}
				
		/***
		 * if the data type is byte, short, integer, long, float or double
		 * */		
		
		switch(datatypeCode) {
		
		case 1:
			byte byteVal = (byte) Integer.valueOf(value).longValue();
			targetTable.write(byteVal);;
			return;
		case 2:
			short shortVal = (short) Integer.valueOf(value).longValue();
			targetTable.writeShort(shortVal);
			return;
		case 3:
			int intVal = (int) Integer.valueOf(value).longValue();
			targetTable.writeInt(intVal);
			return;
		case 4:
			targetTable.writeLong( Integer.valueOf(value).longValue() );
			return;
		case 5:
			float VAL = Float.parseFloat(value);
			targetTable.writeFloat(VAL);
			return;
		case 6:
			double dbVal = Double.parseDouble(value);
			targetTable.writeDouble(dbVal);
			return;
			
		case 9:
			int Time = (int) Integer.valueOf(value).longValue();
			if(Time<0 || Time>=86400000) {
				System.out.println("Invalid Time value.\n "
						+ "Time value should be within range [0 86,400,000)");
			}
			else{targetTable.writeInt(Time);}
			
			return;
			
		}
		
	}
	
	/**
	 * @throws IOException *******************************************************************************************************************/
	
	public static Boolean checkPrimkey(String targetTableName, String targetValue, int ordinalPos) throws IOException {
		
		RandomAccessFile targetTableFile = new RandomAccessFile("data"+File.separator+"user_data"+File.separator+targetTableName+".tbl", "rw");
		
		ArrayList<Integer> cellPos_list = PageFormat.getCellStartPos(targetTableFile);// get all the cell beginning offset on page
		
		//if no records in the table, then will not violate the primary key constraint
		if(cellPos_list.size()==0) {
			return false;
		}
		
		
		int cellPos = Settings.getPageSize();
		int cellHeaderSize=6;// cell header size
		
		for(int i=0;i<cellPos_list.size();i++) {
			
			cellPos = cellPos_list.get(i);
			targetTableFile.seek(cellPos+cellHeaderSize);// pointer to the byte of column counts in target table
			int numColumn = targetTableFile.read(); // 1 byte, column counts
			
			int previousTypeCode;// type code of column before the primary key
			int previousSize=0;//byte length before the primary key in the record body
			for(int j = 0;j<ordinalPos-1;j++) {
				previousTypeCode = targetTableFile.read(); // 1 byte
				previousSize += DataTypeFormat.datatypeCode_toSize(previousTypeCode);
			}
			// pointer is now at the byte of datatype of primary key column
			
			int primTypeCode = targetTableFile.read(); // 1 byte
			
			int prim_Offset = cellPos+cellHeaderSize + 1 + numColumn + previousSize;
			
			String primValue = DataTypeFormat.showData(primTypeCode, targetTableFile, prim_Offset);
			
			if(primValue.equals(targetValue)) {
				return true;
			}
			
		}
		
		targetTableFile.close();
		return false;
	}


	
	/*********************************************************************************************************************/
	/**
	 * @throws ParseException *******************************************************************************************************************/
	
	public static void readRecord(String targetTableName, ArrayList<String> targetColumns, ArrayList<String> conditions ) throws IOException, ParseException {	
		/**
		 * check which of the records are qualified
		 * **/
		ArrayList<Boolean> ConditionResults_InPage = new ArrayList<Boolean>();
		if(!conditions.isEmpty()) {
			ConditionResults_InPage = conditionProcess.getConditionResultsList(targetTableName, conditions);
		}
		
		/**if user doesn't want every column of the target table
		 * then we need find out the ordinal positions of the mentioned columns
		 * */
		List<Object> targetInfo = getQueryTarget_Info(targetTableName, targetColumns);
		ArrayList<String> columnsInTargetTable = (ArrayList<String>) targetInfo.get(1);
		int[] ordinalOf_targetColumns;
		if(!targetColumns.contains("*")) {
			ordinalOf_targetColumns = (int[]) targetInfo.get(0);
			if( ordinalOf_targetColumns[0]==-1 && !targetColumns.contains("rowid") ) {
				System.out.println("Invalid query statement !");
				return;
			}
		}
		else {
			ordinalOf_targetColumns = new int[columnsInTargetTable.size()];
			for(int i=0;i<columnsInTargetTable.size();i++) {
				ordinalOf_targetColumns[i] = i+1;
			}
		}
		
		/**
		 * remove the -1 from ordinalOf_targetColumns when "rowid" is mentioned
		 * int[] can not remove element
		 * so copy valid value to a list, then copy value back to int[]
		 * **/
		ArrayList<Integer> ordinalSet = new ArrayList<Integer>();
		for(int i=0;i<ordinalOf_targetColumns.length;i++) {
			if(ordinalOf_targetColumns[i]!=-1) {
				ordinalSet.add(ordinalOf_targetColumns[i]);
			}
		}
		int ordinalSize = ordinalSet.size();// ordinalSize is also the number of user-required columns
		ordinalOf_targetColumns = new int[ordinalSize];
		for(int i=0;i<ordinalSize;i++) {
			ordinalOf_targetColumns[i]=ordinalSet.get(i);
		}
		
		
		/**
		 * print content separator
		 * **/
		if(targetColumns.contains("rowid")) {
			System.out.printf("%-25s", "++++++++++++++++");// keep in the same line
		}
		for(int i=0;i<ordinalSize;i++) {
			System.out.printf("%-25s", "++++++++++++++++");// keep in the same line
		}
		System.out.print("\n");//new line
		
		/**
		 * display the header of result
		 * **/
		
		if(targetColumns.contains("rowid")) {
			System.out.printf("%-25s", "rowid");// keep in the same line
		}
		for(int i=0;i<ordinalSize;i++) {
			int columnOrder = ordinalOf_targetColumns[i] -1;
			System.out.printf("%-25s", columnsInTargetTable.get(columnOrder));// keep in the same line
		}
		System.out.print("\n");//new line
		
		/**
		 * print content separator
		 * **/
		if(targetColumns.contains("rowid")) {
			System.out.printf("%-25s", "++++++++++++++++");// keep in the same line
		}
		for(int i=0;i<ordinalSize;i++) {
			System.out.printf("%-25s", "++++++++++++++++");// keep in the same line
		}
		System.out.print("\n");
		
		
		/**open the target table file
		 * read the content
		 * **/
		
		RandomAccessFile targetTableFile;
		if( targetTableName.equals("davisbase_columns") || targetTableName.equals("davisbase_tables") ) {
			targetTableFile = new RandomAccessFile("data"+File.separator+"catalog"+File.separator+targetTableName+".tbl", "rw");
		}
		else {
			targetTableFile = new RandomAccessFile("data"+File.separator+"user_data"+File.separator+targetTableName+".tbl", "rw");
		}
		
		
		ArrayList<Integer> cellPos_list = PageFormat.getCellStartPos(targetTableFile);// get all the cell beginning offset on Metadata page
		if(cellPos_list.size()==0) {
			System.out.println("Empty Table !");
			return;
		}
		
		//debug
		//System.out.println( "Debug Info: Current cell number is "+ cellPos_list.size() );//debug
		//debug
		
		int cellPos = Settings.getPageSize();
		int cellHeaderSize=6;// cell header size
		int colNum = columnsInTargetTable.size();// number of columns in davisbase_columns
		int recordHeaderSize =1+colNum;// record header size in davisbase_columns
		
		ArrayList<Integer> columnTypeCodes = new ArrayList<Integer>();
		ArrayList<Integer> columnTypeSizes = new ArrayList<Integer>();
		
		Boolean curRecordCheck;
		for(int i=0;i<cellPos_list.size();i++) {
			
			if(!conditions.isEmpty()) {
				curRecordCheck = ConditionResults_InPage.get(i);// check if current record should be displayed
				if(!curRecordCheck ) {
					continue;
				}
			}
			
			cellPos = cellPos_list.get(i);// current cell begin offset
			//debug
			//System.out.println("Debug Info: We get the current cell offset at "+cellPos);//debug
			//debug
			targetTableFile.seek(cellPos+2);//pointer to rowid
			int rowid = targetTableFile.readInt();
			/**check if to display rowid**/
			if(targetColumns.contains("rowid")) {
				System.out.printf("%-25s", rowid);
			}
			
			targetTableFile.seek(cellPos+cellHeaderSize+1);// pointer to datatype-bytes in the page of target table file
			for(int j=0;j<colNum;j++) {
				int typeCode = targetTableFile.read();//read 1 byte, data type code
				columnTypeCodes.add(typeCode);
				
				int typeSize = DataTypeFormat.datatypeCode_toSize(typeCode);
				columnTypeSizes.add(typeSize);
			}
			
			/**display record body
			 * **/
			for(int k=0;k<ordinalOf_targetColumns.length;k++) {
				
				int cur_ordinalPos = ordinalOf_targetColumns[k];
				int cur_typeCode = columnTypeCodes.get(cur_ordinalPos-1);
				int cur_typeSize = columnTypeSizes.get(cur_ordinalPos-1);
				
				if(cur_typeSize==0) {
					System.out.printf("%-25s", "NULL");
					continue;
				}
				
				int pointer = cellPos + cellHeaderSize + recordHeaderSize;
				for(int m=1;m<cur_ordinalPos;m++) {
					pointer += columnTypeSizes.get(m-1);
				}
				
				/**get the content from the target table page**/
				String showContent = DataTypeFormat.showData(cur_typeCode, targetTableFile, pointer);
				System.out.printf("%-25s", showContent);		
				
			}
			
			System.out.print("\n");// current record finish
			columnTypeCodes.clear();
			columnTypeSizes.clear();
		}
		
		
		targetTableFile.close();
	}
	

	/*********************************************************************************************************************/
	/*********************************************************************************************************************/
	
	public static List<Object> getQueryTarget_Info(String targetTableName, ArrayList<String> targetColumns) throws IOException {
				
		List<Object> targetInfo = new ArrayList<>();// return result
		
		RandomAccessFile columnMeta = new RandomAccessFile("data"+File.separator+"catalog"+File.separator+"davisbase_columns.tbl", "rw");
		
		ArrayList<String> columnsInTargetTable = new ArrayList<String>();
		ArrayList<Integer> cellPos_list = PageFormat.getCellStartPos(columnMeta);// get all the cell beginning offset on Metadata page
		
		/**initialize the list of ordinal positions
		 * of the target columns
		 * */
		int[] ordinalOf_targetColumns = new int[targetColumns.size()];
		for(int i=0;i<targetColumns.size();i++) {
			ordinalOf_targetColumns[i] = -1;
		}
		
		
		int cellPos = Settings.getPageSize();
		int cellHeaderSize=6;// cell header size
		int colNum = 7;// number of columns in davisbase_columns
		int recordHeaderSize =1+colNum;// record header size in davisbase_columns
		
		/**
		 * purpose of following part is to 
		 * get ordinal position of required columns from Metadata in davisbase_columns,
		 * which helps read responding data in target table
		 * **/
		
		for(int i=0;i<cellPos_list.size();i++) {
			
			cellPos = cellPos_list.get(i);
			
			columnMeta.seek(cellPos+cellHeaderSize+recordHeaderSize);// pointer to record Body in the page of davisbase_columns
			/** get the table name from record of davisbase_columns
			 * */
			byte[] b = new byte[targetTableName.length()];
			columnMeta.read(b);
			String tableName = new String(b); 
									
			/*****
			 * check if we find the target table in the column Metadata
			 * **/ 
			if(!tableName.equals(targetTableName)) {
				continue;
			}
			
			/**if the target exists
			 * then we check if the column is required by user
			 * **/
						
			columnMeta.seek(cellPos+cellHeaderSize+1+1);
			int colNameSize = columnMeta.read()-12; // get the column_name size of this metadata record in davisbase_columns
			int datatypeName_size = columnMeta.read()-12;// get the datatype-name string size of this metadata record in davisbase_columns
			
			/** get the column_name column offset of this metadata record in davisbase_columns*/
			int colName_offset = cellPos+cellHeaderSize+recordHeaderSize+ targetTableName.length();
			columnMeta.seek(colName_offset);
			b = new byte[colNameSize];
			columnMeta.read(b);
			String columnName = new String(b);
			
			columnsInTargetTable.add(columnName);		
			
			/**if this column is not required by user
			 * if user writes *, meaning all columns of target table are wanted
			 * */
			if(!targetColumns.contains(columnName)) {
				if( !targetColumns.contains("*") ) {
					continue;
				}
			}
			
			//System.out.println("Debug Info: The column called "+targetColumns.get(i)+" is found in "+ targetTableName+"!");//debug
			
			/**if the column is mentioned by user statement
			 * get the ordinal_position column offset of this metadata record in davisbase_columns
			 * and get the ordinal position of this mentioned column in the target table
			 * */
			int ordinalPos_offset = cellPos+cellHeaderSize+recordHeaderSize+ targetTableName.length() +colNameSize +datatypeName_size;
			columnMeta.seek(ordinalPos_offset);
			int ordinalPos = columnMeta.read();//1 byte, get the ordinal position of this mentioned column in the target table
			
			//System.out.println("Debug Info: The ordinal position of "+targetColumns.get(i)+" checked is "+ ordinalPos);//debug
			
			/**usually users don't mention the columns 
			 * according to the original order as defined in the Metadata
			 * so we need to find out which of the user-mentioned column
			 * the ordinal-position belongs to
			 * **/
			if(targetColumns.contains("*")) {
				continue;// if * is used, then no need to get ordinal Positions here
			}
			
			for(int j=0;j<targetColumns.size();j++) {
				if( targetColumns.get(j).equals(columnName) ) {
					ordinalOf_targetColumns[j] = ordinalPos;
				}
			}
			
			
		}
		
		/******************************************************************/
		columnMeta.close();
		
		/******************************************************************/
		
		/**if there's still -1 in the list of ordinalOf_targetColumns
		*  indicates that there's a user-mentioned column 
		*  does not belong to target table
		**/
		
		for(int i=0;i<ordinalOf_targetColumns.length;i++) {
			if(targetColumns.contains("*")) {
				break;// if * is used, all columns wanted from target table
			}
			if(ordinalOf_targetColumns[i]==-1 && !targetColumns.get(i).equals("rowid")) {				
				
				System.out.println("The column called "+targetColumns.get(i)+" does not exist in "+ targetTableName+" catalog!");
				ordinalOf_targetColumns = new int[] {-1};
				targetInfo.add(ordinalOf_targetColumns);
				targetInfo.add(columnsInTargetTable);
				return targetInfo;
			}
		}
		
		targetInfo.add(ordinalOf_targetColumns);
		targetInfo.add(columnsInTargetTable);
		return targetInfo;
		
	}
	
}

