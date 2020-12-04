package cxyBase;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class conditionProcess {
	
	public static ArrayList<Boolean> getConditionResultsList(String targetTableName, ArrayList<String> conditionTokens) throws IOException, ParseException {
		
		ArrayList<Boolean> ConditionResults_InPage= new ArrayList<Boolean>();// result to return
		/***
		 * parse the conditions
		 * ***/
		ArrayList<String> targetColumns = new ArrayList<String>();// user mentioned columns
		ArrayList<String> boolOperators = new ArrayList<String>();// user mentioned operators like > = <
		ArrayList<String> targetValues = new ArrayList<String>(); // user mentioned values to compare
		ArrayList<String> connectors = new ArrayList<String>();// user mentioned connectors like "and" "or"
		
		ArrayList<String> conditionPart = new ArrayList<String>();// buffer of a single complete condition
		for(int i=0;i<conditionTokens.size();i++) {
			
			//every time we get to connector or reach the end of condition-token list
			//we split the condition part
			if( conditionTokens.get(i).equals("and") || conditionTokens.get(i).equals("or") ) {
								
				connectors.add(conditionTokens.get(i));
				
				targetColumns.add(conditionPart.get(0));
				boolOperators.add(conditionPart.get(1));
				targetValues.add(conditionPart.get(2));
				
				conditionPart = new ArrayList<String>();// create a new buffer, do not use .clear()
				continue;
			}
			
			conditionPart.add(conditionTokens.get(i));
			
			if(i==conditionTokens.size()-1) {
				targetColumns.add(conditionPart.get(0));
				boolOperators.add(conditionPart.get(1));
				targetValues.add(conditionPart.get(2));
			}
		}
		
		
		/*****
		 * get the info of mentioned target columns, 
		 * especially the ordinal positions
		 * ****/
		List<Object> targetInfo = CellFormat.getQueryTarget_Info(targetTableName, targetColumns);		
		
		int[] ordinalOf_targetColumns = (int[]) targetInfo.get(0);
		
		if( ordinalOf_targetColumns[0]==-1 && !targetColumns.contains("rowid") ) {
			System.out.println("Invalid query statement, column not exist !");
			mainCXYsqlPrompt.promptLoop();// back to a new prompt
		}
		
		/**
		 * open the target file
		 * **/
		RandomAccessFile targetTableFile;
		if(targetTableName.equals("davisbase_tables") || targetTableName.equals("davisbase_columns") ) {
			targetTableFile = new RandomAccessFile("data"+File.separator+"catalog"+File.separator+ targetTableName +".tbl", "rw");
		}
		else {targetTableFile = new RandomAccessFile("data"+File.separator+"user_data"+File.separator+ targetTableName +".tbl", "rw");}
		
		ArrayList<Integer> cellPos_list = PageFormat.getCellStartPos(targetTableFile);// get all the cell beginning offset on Metadata page
		if(cellPos_list.size()==0) {
			System.out.println("Empty Table, do not support deletion !");
			mainCXYsqlPrompt.promptLoop();// back to a new prompt
		}
		
		/***
		 * get the compare results of each record in the page
		 * **/
		int cellPos;
		for(int i=0;i<cellPos_list.size();i++) {
			cellPos = cellPos_list.get(i);
			Boolean curRecordResult = compareInRecord(targetTableFile,cellPos,targetColumns, boolOperators,targetValues,ordinalOf_targetColumns, connectors);// boolean result of the current record	
			
			ConditionResults_InPage.add(curRecordResult);
		}
		
		targetTableFile.close();
		
		return ConditionResults_InPage;
	}
	
	
	/**
	 * @throws ParseException *************************************************************
	 * 
	 * */
	public static Boolean compareInRecord(RandomAccessFile targetTableFile,int cellPos, ArrayList<String> targetColumns,
		                                  ArrayList<String> boolOperators,ArrayList<String> targetValues,int[] ordinalOf_targetColumns,
		                                  ArrayList<String> connectors) throws IOException, ParseException {
		
		Boolean curRecordResult;// boolean result of the current record
		
		ArrayList<Boolean> columnResults = new ArrayList<Boolean>();// the boolean result of each mentioned column in the condition
		
		int cellHeaderSize=6;// cell header size
		targetTableFile.seek(cellPos + cellHeaderSize);//pointer the the byte that stores the number of columns
		int colNum = targetTableFile.read();// number of columns in davisbase_columns
		int recordHeaderSize =1+colNum;// record header size in davisbase_columns
		
		targetTableFile.seek(cellPos+2);//pointer to rowid
		int rowid = targetTableFile.readInt();
		
		for(int j=0;j<targetColumns.size();j++) {
			
			String cur_column = targetColumns.get(j);
			String curOperator= boolOperators.get(j);
			String curtargetValue= targetValues.get(j);
			int curOrdianl = ordinalOf_targetColumns[j];
			targetTableFile.seek(cellPos + cellHeaderSize+ curOrdianl);
			int curColumnTypeCode = targetTableFile.read();
//			int curColumnDataSize = DataTypeFormat.datatypeCode_toSize(curColumnTypeCode);
			
			int curColumnDataOffset = cellPos + cellHeaderSize + recordHeaderSize;
			for(int k=1;k<curOrdianl;k++) {
				targetTableFile.seek(cellPos + cellHeaderSize+ k);
				int previousColumnTypeCode = targetTableFile.read();
				int previousColumnDataSize = DataTypeFormat.datatypeCode_toSize(previousColumnTypeCode);
				curColumnDataOffset += previousColumnDataSize;
			}
			
			String str_ofColumnValue = DataTypeFormat.showData(curColumnTypeCode, targetTableFile, curColumnDataOffset);
			
			/**check if current column mentioned is the row id
			 * **/
			if(cur_column.equals("rowid")) {
				
				int cur_value = Integer.valueOf(curtargetValue);
				switch(curOperator) {
				
				case ">":
					columnResults.add( rowid>cur_value );
					break;
				case ">=":
					columnResults.add( rowid>=cur_value );
					break;
				case "<":
					columnResults.add( rowid<cur_value );
					break;
				case "<=":
					columnResults.add( rowid<=cur_value );
					break;
				case "=":
					columnResults.add( rowid==cur_value );
					break;
				case "!=":
					columnResults.add( rowid!=cur_value );
					break;
					
				default:
					System.out.println("Wrong Boolean Operator, please use >, <, =, >=, <=, !=");
					mainCXYsqlPrompt.promptLoop();// back to a new prompt
					break;
				}
				continue;
			}
			
			//debug
//			System.out.println("Debug Info: We have the value of "+cur_column+" is: "+str_ofColumnValue);//debug
//			System.out.println("Debug Info: We have the target value is: "+ curtargetValue);//debug
//			System.out.println("Debug Info: We have the operator is: "+ curOperator);//debug
//			System.out.println("Debug Info: We have column data type code is: "+ curColumnTypeCode);//debug
			//debug
			
			/**Add the compare result of each mentioned column
			 * */
			
			columnResults.add( compareSingleColumn(str_ofColumnValue, curOperator, curtargetValue, curColumnTypeCode) );
		}
		
		
		/****
		 * get the boolean result of current record
		 * based on the boolean results of the mentioned columns in this record
		 *          and the connectors
		 * **/
		curRecordResult=columnResults.get(0);
		
		for(int i=0;i<connectors.size();i++) {
			String curConnector = connectors.get(i);
			//debug
			//System.out.println("Debug Info: The connector just taken is: "+ curConnector );//debug
			//debug
			
			switch(curConnector) {
			
			case "and":
				curRecordResult = curRecordResult && columnResults.get(i+1);
				break;
			case "or":
				curRecordResult = curRecordResult || columnResults.get(i+1);
				break;
			default:
				System.out.println("Invalid Syntax, please use and/or to connect the conditions.");
				mainCXYsqlPrompt.promptLoop();// back to a new prompt
				break;
			}
		}
		//debug
		//System.out.println("Debug Info: The compare result is: "+ curRecordResult );//debug
		//debug
		return curRecordResult;
	}
	
	
	/*************************************************************************************************************
	 * compare a single column value to the user-required target value
	 * @throws ParseException 
	 * @throws IOException *************************************************************
	 * 
	 * */
	public static Boolean compareSingleColumn(String str_ofColumnValue,String curOperator,String curtargetValue, int curColumnTypeCode) throws IOException, ParseException {
		
		/**check if the current mentioned column is NULL**/
		if(curColumnTypeCode==0) {
			if(curtargetValue=="null") {
				return true;
			}
			else {return false;}
		}
		
		/***
		 * For DateTime, Date, Text, 
		 * Only support operator =, !=
		 * **/
		if(curColumnTypeCode>=10) {
			switch(curOperator) {
			
			case "=":
				return str_ofColumnValue.equals(curtargetValue);
			case "!=":
				return !str_ofColumnValue.equals(curtargetValue);
			default:
				System.out.println("Wrong Boolean Operator! Only support =, != for data type of DateTime, Date and Text");
				mainCXYsqlPrompt.promptLoop();// back to a new prompt
				break;
			}
		}
		
		/***
		 * Check if it's Float type
		 * **/
		if(curColumnTypeCode==5) {
			float columnValue = Float.valueOf(str_ofColumnValue);
			float targetValue = Float.valueOf(curtargetValue);
			
			switch(curOperator) {
			
			case ">":
				return columnValue > targetValue;
			case "<":
				return columnValue < targetValue;
			case ">=":
				return columnValue >= targetValue;
			case "<=":
				return columnValue <= targetValue;
			case "=":
				return columnValue == targetValue;
			case "!=":
				return columnValue != targetValue;
			default:
				System.out.println("Wrong Boolean Operator, please use >, <, =, >=, <=, !=");
				mainCXYsqlPrompt.promptLoop();// back to a new prompt
				break;
			}
		}
		
		/***
		 * Check if it's Double type
		 * **/
		if(curColumnTypeCode==6) {
			double columnValue = Double.valueOf(str_ofColumnValue);
			double targetValue = Double.valueOf(curtargetValue);
			
			switch(curOperator) {
			
			case ">":
				return columnValue > targetValue;
			case "<":
				return columnValue < targetValue;
			case ">=":
				return columnValue >= targetValue;
			case "<=":
				return columnValue <= targetValue;
			case "=":
				return columnValue == targetValue;
			case "!=":
				return columnValue != targetValue;
			default:
				System.out.println("Wrong Boolean Operator, please use >, <, =, >=, <=, !=");
				mainCXYsqlPrompt.promptLoop();// back to a new prompt
				break;
			}
		}
		
		/***
		 * Check if it's Integer type
		 * Including TinyInt (Byte), SmallInt (Short), Int, BigInt (Long) 
		 * All transfer to Long type, and compare
		 * **/
		else {
			long columnValue = Long.valueOf(str_ofColumnValue);
			long targetValue = Long.valueOf(curtargetValue);
			
			switch(curOperator) {
			
			case ">":
				return columnValue > targetValue;
			case "<":
				return columnValue < targetValue;
			case ">=":
				return columnValue >= targetValue;
			case "<=":
				return columnValue <= targetValue;
			case "=":
				return columnValue == targetValue;
			case "!=":
				return columnValue != targetValue;
			default:
				System.out.println("Wrong Boolean Operator, please use >, <, =, >=, <=, !=");
				mainCXYsqlPrompt.promptLoop();// back to a new prompt
				break;
			}
		}
		
		return false;
	}
	

}
