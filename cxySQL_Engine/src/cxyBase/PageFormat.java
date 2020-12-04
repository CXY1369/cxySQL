package cxyBase;

import java.io.File;
import java.io.FileNotFoundException;
//import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class PageFormat {
	
	public static void initializeHeader(RandomAccessFile TableFile) throws IOException {
		
		/* Set file pointer to the beginning of the file */
		TableFile.seek(0);
		
		/* Write 0x0D to the page header to indicate that it's a table B-tree leaf page.  
		 * The file pointer will automatically increment to the next byte. */
		TableFile.write(0x0D);// write in one byte
		
		/* to indicate there are no cells on this page */
		TableFile.seek(2);
		TableFile.writeShort(0x0000);// write in two bytes
		
		/* to indicate the beginning offset of next cell-record*/
		TableFile.writeShort(Settings.getPageSize());// write in two bytes
		
		/* to indicate there's no right sibling page*/
		TableFile.writeInt(0xffffffff);// write in 4 bytes
		
		/* to indicate there's no parent*/
		TableFile.writeInt(0xffffffff);// write in 4 bytes
		
	}
	
	/****************************************************************************************************/
	/****************************************************************************************************/
	
	// to get the start position of each cell
	public static ArrayList<Integer> getCellStartPos(RandomAccessFile TableFile) throws IOException{
		
		ArrayList<Integer> cellPos_list = new ArrayList<Integer>();
		
		TableFile.seek(2);
		Short cellNums = TableFile.readShort();
		
		TableFile.seek(16);
		int cellPos;
		for(int i=0;i<cellNums;i++) {
			cellPos = TableFile.readShort();
			cellPos_list.add(cellPos);
		}
		return cellPos_list;
	}

	/****************************************************************************************************/
	/****************************************************************************************************/

	public static void pageInsertUpdate(String targetTableName,ArrayList<String> columns,ArrayList<String> values) throws IOException, ParseException {
		
		//open the target table
		RandomAccessFile targetTable = new RandomAccessFile("data"+File.separator+"user_data"+File.separator+targetTableName+".tbl", "rw");
		
		// pointer to the cell counts position at Page Header
		targetTable.seek(2);
		int numOfCell = targetTable.readShort();//get the current cell counts
		int rowid = numOfCell;// the internal row id equals the current cell counts
		int cellBeginPos = targetTable.readShort();// get the current cell beginning offset
		int cell_LocArray_offset = 15 + numOfCell*2;//get the current cell location array offset
		
		// calculate the inserting payload size
		List<Object> payloadInfo = CellFormat.getPayloadInfo(targetTableName, columns, values);
		
		int payloadSize = (int) payloadInfo.get(0);
		/*check violations**/
		if(payloadSize==-1) {
			System.out.println("Invalid statement. Violate column constraints.");
			targetTable.close();
			return;
		}
		int insertingCellSize = 6+ payloadSize;// cell Header + Cell payload
		
		/*Check if there's enough free space for inserting 
		 * (a new cell) and (2 bytes of cell location array)
		 * */
		if(cellBeginPos-cell_LocArray_offset > 2+insertingCellSize) {
			
			/* to increase the cell counts by 1 and write back to page Header*/
			targetTable.seek(2);
			numOfCell +=1;
			targetTable.writeShort(numOfCell);//2-byte
			
			/*write in the new cell begin offset to page Header*/
			cellBeginPos -= insertingCellSize;
			targetTable.writeShort(cellBeginPos);//2-byte
			
			/*Add the new cell begin offset to the Cell-Location-Array*/
			targetTable.seek(cell_LocArray_offset+1);
			targetTable.writeShort(cellBeginPos);//2-byte
			
			/*write the cell Header bytes to the page*/
			targetTable.seek(cellBeginPos);
			targetTable.writeShort(payloadSize);//write in 2 bytes of payload size to cell Header
			targetTable.writeInt(rowid+1);//write in 4 bytes of "Hidden" rowid to cell Header
			
			/*write the Record Header bytes to the page*/
			ArrayList<Integer> columnTypeCodeInTargetTable = (ArrayList<Integer>) payloadInfo.get(1);
			
			//System.out.println("Debug Info: Data type of columnTypeCodeInTargetTable is "+ payloadInfo.get(1).getClass().toString());//debug
			
			targetTable.writeByte(columnTypeCodeInTargetTable.size());// write in 1 byte of column number to record Header
			for(int i=0;i<columnTypeCodeInTargetTable.size();i++) {
				targetTable.writeByte(columnTypeCodeInTargetTable.get(i));// write in 1 byte of each column data type to record Header
			}
			
			/*write the Record Body bytes to the page*/
			String[] reorderValues = (String[]) payloadInfo.get(2);
			int filePointer = cellBeginPos+6+1+columnTypeCodeInTargetTable.size();
			for(int i=0;i<reorderValues.length;i++) {
				int j=i;
				while(j<columnTypeCodeInTargetTable.size() && (int) columnTypeCodeInTargetTable.get(j)==0) {
					j++;
					continue;
				}
				int datatypeCode = 	columnTypeCodeInTargetTable.get(j);
				int dataSize = DataTypeFormat.datatypeCode_toSize(datatypeCode);
				String value = reorderValues[i];
				//Write in the string value as bytes-array to Record Body
				CellFormat.writeRecord(targetTable, filePointer, datatypeCode, value);
			
				//move the pointer to next available offset
				filePointer += dataSize;
			}
				
		}
		else {
			System.out.println("No enough Space for inserting!");
		}
		
		targetTable.close();
		return;
	}
	
	/****************************************************************************************************/

	/****************************************************************************************************/

	public static void pageDeleteUpdate(String targetTableName,ArrayList<String> conditionTokens) throws IOException, ParseException {
				
		/**parse the conditions
		 * and get the boolean results
		 * */
		ArrayList<Boolean> ConditionResults_InPage = conditionProcess.getConditionResultsList(targetTableName, conditionTokens);
				
		
		/**
		 * open the target file
		 * **/
		RandomAccessFile targetTableFile = new RandomAccessFile("data"+File.separator+"user_data"+File.separator+ targetTableName +".tbl", "rw");
		
		
		/**get the list of each cell's begin offset
		 * on this page
		 * **/
		ArrayList<Integer> cellPos_list = PageFormat.getCellStartPos(targetTableFile);// get all the cell beginning offset on Metadata page
		if(cellPos_list.size()==0) {
			System.out.println("Empty Table, do not support deletion !");
			return;
		}
		
		/*** If true,
		 *   just delete the required cell_offset from the cell_Location_array
		 *   by covering it with new offset
		 *   If false, keep the record
		 * ***/
		
		targetTableFile.seek(2);// pointer to the byte storing cell counts
		int numOfCell = targetTableFile.readShort();//2-byte, get the current remaining cell counts
		int cellBeginOffset = targetTableFile.readShort();//2-byte, get the top cell beginning offset
		Boolean recordResult;
		int curCellPos;
		
		ArrayList<Integer> newCellPos_list = new ArrayList<Integer>();//Store the remaining cell begin offsets
		
		for(int i=0;i<cellPos_list.size();i++) {
			
			recordResult = ConditionResults_InPage.get(i);
			//debug
			//System.out.println("Debug Info: The record "+(i+1)+" deletion result is "+recordResult);//debug
			//debug
			curCellPos = cellPos_list.get(i);
			
			//if false, meaning no not delete
			//so remain and update some info
			if( !recordResult ) {
				newCellPos_list.add(curCellPos);// remain this cell offset
				cellBeginOffset = curCellPos;// update the top cell offset with the remaining cell
			}
			else {
				numOfCell -= 1;// per deletion, decrease the cell count by 1
			}
			
		}
		
		/***
		 * update page header
		 * **/
		targetTableFile.seek(2);// pointer to the byte storing cell counts
		targetTableFile.writeShort(numOfCell);//2-byte, write in the new cell counts
		targetTableFile.writeShort(cellBeginOffset);//2-byte,  write in the updated top cell offset
		
		/**
		 * update the page content
		 * **/
		for(int i=0;i<newCellPos_list.size();i++) {
			//update the cell location array
			targetTableFile.seek( 16+ 2*i );
			int remainCellPos = newCellPos_list.get(i);
			targetTableFile.writeShort(remainCellPos);
			
			//update the rowid in each remaining cell
			int new_rowid = i+1;// rowid starts from 1
			targetTableFile.seek(remainCellPos+2);// pointer to the rowid offset of the remaining record
			targetTableFile.writeInt(new_rowid);//4-byte, write in the new rowid
			
		}
		
		/**Finish deletion
		 * */
		targetTableFile.close();
	}
}

