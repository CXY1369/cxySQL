package cxyBase;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class createColumnCatalog {
	
	public static void initializeColumnCatalog() {
		
		/** Create davisbase_columns systems catalog */
		try {
			File tablePath = new File("data"+File.separator+"catalog"+File.separator+"davisbase_columns.tbl");
			if(!tablePath.exists()) {
				RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile(tablePath, "rw");
				/* Initially, the file is one page in length */
				davisbaseColumnsCatalog.setLength(Settings.getPageSize());
				
				PageFormat.initializeHeader(davisbaseColumnsCatalog);// initialize page header
				
				/**Insert davisbase_tables info into davisbase_columns*/
				List<Object> davisTableInfo = new ArrayList<>();
				davisTableInfo.add(Arrays.asList("davisbase_tables","table_name","text","not null","unique","primary key"));
				
				insertColumnCatalog(davisTableInfo,davisbaseColumnsCatalog);
				
				/**Insert davisbase_columns info into davisbase_columns*/
				List<Object> davisColumnInfo = new ArrayList<>();
				davisColumnInfo.add(Arrays.asList("davisbase_columns","table_name","text","not null"));
				davisColumnInfo.add(Arrays.asList("davisbase_columns","column_name","text","not null"));
				davisColumnInfo.add(Arrays.asList("davisbase_columns","data_type","text","not null"));
				davisColumnInfo.add(Arrays.asList("davisbase_columns","ordinal_position","byte","not null"));
				davisColumnInfo.add(Arrays.asList("davisbase_columns","isNot_Null","byte","not null"));
				davisColumnInfo.add(Arrays.asList("davisbase_columns","is_Unique","byte","not null"));
				davisColumnInfo.add(Arrays.asList("davisbase_columns","is_Prim_Key","byte","not null"));
				
				insertColumnCatalog(davisColumnInfo,davisbaseColumnsCatalog);
				
				davisbaseColumnsCatalog.close();
			}
		}
		catch (Exception e) {
			System.out.println("Unable to create the database_columns file");
			System.out.println(e);
		}
	}
	
	/***********************************************************************/
	/***********************************************************************/
	
	public static void insertColumnCatalog(List<Object> Info,RandomAccessFile davisbaseColumnsCatalog) throws IOException {
		
		for(int i=0;i<Info.size();i++) {
			
			List<String> curRecord = (List<String>) Info.get(i);
						
			if(curRecord.size()<3) {
				System.out.println("Incomplete statement!");
				return;
			}
			
			String insertTableName = curRecord.get(0);
			String insertColumnName = curRecord.get(1);
			String insertColumnType = curRecord.get(2); 
			int ordinalPos = i+1;// ordinal position starts from 1
			
			int isNotNull=0;
			if(curRecord.contains("not null")) {isNotNull=1;}
			
			int isUnique=0;
			if(curRecord.contains("unique")) {isUnique=1;}
			
			int isPrimKey=0;
			if(curRecord.contains("primary key")) {isPrimKey=1;}
			
			int numOfColumn =7;
			int recordHeaderSize = 1 + numOfColumn;
			int recordBodySize = insertTableName.length()+insertColumnName.length()+insertColumnType.length()+4;
			int payloadSize = recordHeaderSize + recordBodySize;
			int cellHeaderSize =6;
			int cellSize = cellHeaderSize + payloadSize;
			
			// pointer to the cell counts position at Page Header
			davisbaseColumnsCatalog.seek(2);
			int numOfCell = davisbaseColumnsCatalog.readShort();//2-byte, get the current cell counts
			int rowid = numOfCell;// the internal rowid equals the current cell counts
			int cellOffset = davisbaseColumnsCatalog.readShort();//2-byte, get the current cell beginning offset
			int cell_LocArray_offset = 15 + numOfCell*2;//get the current cell location array offset
			
			//debug
//			System.out.println("Debug Info: cellOffset is at "+cellOffset);//debug
//			System.out.println("Debug Info: cell_LocArray_offset is at "+cell_LocArray_offset);//debug
//			System.out.println("Debug Info: cellSize is at "+cellSize);//debug
			//debug
			
			if(cellOffset-cell_LocArray_offset <= 2+cellSize) {
				System.out.println("No enough space in davisbase_columns catalog file");
				return;
			}
			
			/**
			 * inserting and update page
			 * */
			
			/* to increase the cell counts by 1 and write back to page Header*/
			davisbaseColumnsCatalog.seek(2);
			numOfCell +=1;
			davisbaseColumnsCatalog.writeShort(numOfCell);//2-byte
			
			/*write in the new cell begin offset to page Header*/
			cellOffset -= cellSize;
			davisbaseColumnsCatalog.writeShort(cellOffset);//2-byte
			
			/*Add the new cell begin offset to the Cell-Location-Array*/
			davisbaseColumnsCatalog.seek(cell_LocArray_offset+1);
			davisbaseColumnsCatalog.writeShort(cellOffset);//2-byte
			
			/*write the cell Header bytes to the page*/
			davisbaseColumnsCatalog.seek(cellOffset);
			davisbaseColumnsCatalog.writeShort(payloadSize);//write in 2 bytes of payload size to cell Header
			davisbaseColumnsCatalog.writeInt(rowid+1);//write in 4 bytes of "Hidden" rowid to cell Header
			
			/**
			 * write the Record Header bytes to the page
			 * */
			davisbaseColumnsCatalog.writeByte(numOfColumn);// write in 1-byte, the column count in davisbase_columns, to record Header
			
			int insertTableNameTypeCode = 12+ insertTableName.length();// the data type code of inserting table name, which is string text
			davisbaseColumnsCatalog.writeByte(insertTableNameTypeCode);//1 byte
			
			int insertColumnNameTypeCode = 12+ insertColumnName.length();// the data type code of insert Column Name, which is string text
			davisbaseColumnsCatalog.writeByte(insertColumnNameTypeCode);//1 byte
			
			int insertColumnTypeTypeCode = 12+ insertColumnType.length();// the data type code of insert Column Name, which is string text
			davisbaseColumnsCatalog.writeByte(insertColumnTypeTypeCode);//1 byte
			
			int tinyByteTypeCode = 1;// ordinalPos, isNotNull, isUnique, isPrimKey are all tiny byte
			davisbaseColumnsCatalog.writeByte(tinyByteTypeCode);
			davisbaseColumnsCatalog.writeByte(tinyByteTypeCode);
			davisbaseColumnsCatalog.writeByte(tinyByteTypeCode);
			davisbaseColumnsCatalog.writeByte(tinyByteTypeCode);
			
			/**
			 * write the Record Body bytes to the page
			 * */
			davisbaseColumnsCatalog.writeBytes(insertTableName);
			davisbaseColumnsCatalog.writeBytes(insertColumnName);
			davisbaseColumnsCatalog.writeBytes(insertColumnType);
			davisbaseColumnsCatalog.writeByte(ordinalPos);
			davisbaseColumnsCatalog.writeByte(isNotNull);
			davisbaseColumnsCatalog.writeByte(isUnique);
			davisbaseColumnsCatalog.writeByte(isPrimKey);
			
		}
		
	}


	/***********************************************************************/

	/**
	 * @throws IOException *********************************************************************/
	
	public static void deleteFromColumnCatalog(String targetTableName) throws IOException {
		
		RandomAccessFile columnsCatalog = new RandomAccessFile("data"+File.separator+"catalog"+File.separator+"davisbase_columns.tbl","rw");
		
		columnsCatalog.seek(2);
		int numOfCell = columnsCatalog.readShort();// current number of cells in metadata
		int cellBeginOffset = columnsCatalog.readShort();// Cell start offset
		
		ArrayList<Integer> curCellPos_list = new ArrayList<Integer>();//current array list of cell offsets
		int cellPos;
		
		columnsCatalog.seek(16);// pointer to the beginning byte of the cell_location_array
		for(int i=0;i<numOfCell;i++ ) {
			cellPos = columnsCatalog.readShort();
			curCellPos_list.add(cellPos);
		}
		
		/********
		 * check which cell is target table name
		 * ***/
		ArrayList<Boolean> boolResults = new ArrayList<Boolean>();
		
		int cellHeaderSize = 6;
		int numOfColumn =7;
		int recordHeaderSize = 1 + numOfColumn;
		int typeCode;
		int tableName_stringSize;
		String curTableName;
		
		for(int i=0;i<numOfCell;i++ ) {
			
			cellPos = curCellPos_list.get(i);
			columnsCatalog.seek(cellPos+ cellHeaderSize +1);//pointer to the byte of table_name type code
			typeCode = columnsCatalog.read();// 1 byte, pointer is then to the beginning of content, table_name strings
			tableName_stringSize = typeCode - 12;// it's Text type
			/****
			 * get each table name, it's the first column in metadata
			 * */
			columnsCatalog.seek(cellPos+ cellHeaderSize + recordHeaderSize);//pointer to the table_name
			byte[] b = new byte[tableName_stringSize];
			columnsCatalog.read(b);
			curTableName = new String(b);
			
			boolResults.add( curTableName.equals(targetTableName) );
		}
		
		/**** 
		 * get new array list of cell offsets
		 * ***/
		ArrayList<Integer> newCellPos_list = new ArrayList<Integer>();//Store the remaining cell begin offsets
		Boolean recordResult;
		
		for(int i=0;i<curCellPos_list.size();i++) {
			
			recordResult = boolResults.get(i);
		
			cellPos = curCellPos_list.get(i);
			
			//if false, meaning no not delete
			//so remain and update some info
			if( !recordResult ) {
				newCellPos_list.add(cellPos);// remain this cell offset
				cellBeginOffset = cellPos;// update the top cell offset with the remaining cell
			}
			else {
				numOfCell -= 1;// per deletion, decrease the cell count by 1
			}
			
		}
		
		/***
		 * update page header
		 * **/
		columnsCatalog.seek(2);// pointer to the byte storing cell counts
		columnsCatalog.writeShort(numOfCell);//2-byte, write in the new cell counts
		columnsCatalog.writeShort(cellBeginOffset);//2-byte,  write in the updated top cell offset
		
		/**
		 * update the page content
		 * **/
		for(int i=0;i<newCellPos_list.size();i++) {
			//update the cell location array
			columnsCatalog.seek( 16 + 2*i);
			int remainCellPos = newCellPos_list.get(i);
			columnsCatalog.writeShort(remainCellPos);
			
			//update the rowid in each remaining cell
			int new_rowid = i+1;// rowid starts from 1
			columnsCatalog.seek(remainCellPos+2);// pointer to the rowid offset of the remaining record
			columnsCatalog.writeInt(new_rowid);//4-byte, write in the new rowid
			
		}
		
		/**Finish deletion
		 * */
		
		columnsCatalog.close();
		return;
	}
	
	/***********************************************************************/
}
