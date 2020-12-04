package cxyBase;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

public class createTableCatalog {
	
	public static void initializeTablePage() {
		
		/** Create davisbase_tables system catalog */
		try {
			File tablePath = new File("data"+File.separator+"catalog"+File.separator+"davisbase_tables.tbl");
			if(!tablePath.exists()) {
				RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(tablePath, "rw");
				
				/* Initially, the file is one page in length */
				davisbaseTablesCatalog.setLength(Settings.getPageSize());
				
				PageFormat.initializeHeader(davisbaseTablesCatalog);
				
				/*Only 2 system catalog table files at the beginning*/
				insertTableCatlog("davisbase_tables",davisbaseTablesCatalog);
				insertTableCatlog("davisbase_columns",davisbaseTablesCatalog);
				
				davisbaseTablesCatalog.close();
			}
		}
		catch (Exception e) {
			System.out.println("Unable to create the database_tables file");
			System.out.println(e);
		}
		
	}
	
	/************************************************************************/
	/*Insert the table name to the davisbase Tables Catalog*/
	
	public static void insertTableCatlog(String tableName,RandomAccessFile davisbaseTablesCatalog) throws IOException {
		
		davisbaseTablesCatalog.seek(2);
		int numOfTable = davisbaseTablesCatalog.readShort();// current number of tables
		int rowid = numOfTable;
		int cellOffset = davisbaseTablesCatalog.readShort();// Cell start offset
		
		int cell_LocArray_offset = 15 + numOfTable*2;//get the current cell location array offset
		
		/**cell header size is 6
		 * record header size is 2, since only one column in davisbase_tables
		 * record body size is the length of the table name*/
		int payloadSize = 2+tableName.length();
		int cellSize = 6+payloadSize;
		
		
		/*If no enough space for inserting*/
		if(cellOffset-cell_LocArray_offset <= 2+cellSize) {
			System.out.println("No enough space in davisbase_tables file");
			return;
		}
		
		/**
		 * inserting and update page
		 * */
		
		/* to increase the cell counts by 1 and write back to page Header*/
		davisbaseTablesCatalog.seek(2);
		numOfTable +=1;
		davisbaseTablesCatalog.writeShort(numOfTable);//2-byte
		
		/*write in the new cell begin offset to page Header*/
		cellOffset -= cellSize;
		davisbaseTablesCatalog.writeShort(cellOffset);//2-byte
		
		/*Add the new cell begin offset to the Cell-Location-Array*/
		davisbaseTablesCatalog.seek(cell_LocArray_offset+1);
		davisbaseTablesCatalog.writeShort(cellOffset);//2-byte
		
		/*write the cell Header bytes to the page*/
		davisbaseTablesCatalog.seek(cellOffset);
		davisbaseTablesCatalog.writeShort(payloadSize);//write in 2 bytes of payload size to cell Header
		davisbaseTablesCatalog.writeInt(rowid+1);//write in 4 bytes of "Hidden" rowid to cell Header
		
		/*write the cell payload bytes to the page*/
		davisbaseTablesCatalog.writeByte(1);// write in 1-byte, the column count which is 1, to record Header
		davisbaseTablesCatalog.writeByte(12+tableName.length());// 1-byte, the data type of inserting table name, to record Header
		davisbaseTablesCatalog.writeBytes(tableName);//Write the table name to the record body on this page
		
	}


	/***********************************************************************/

	/**
	 * @throws IOException *********************************************************************/
	
	public static void deleteFromTableCatalog(String targetTableName) throws IOException {
		
		RandomAccessFile tablesCatalog = new RandomAccessFile("data"+File.separator+"catalog"+File.separator+"davisbase_tables.tbl","rw");
		
		tablesCatalog.seek(2);
		int numOfTable = tablesCatalog.readShort();// current number of tables
		int cellBeginOffset = tablesCatalog.readShort();// Cell start offset
		
		ArrayList<Integer> curCellPos_list = new ArrayList<Integer>();//current array list of cell offsets
		int cellPos;
		
		tablesCatalog.seek(16);// pointer to the beginning byte of the cell_location_array
		for(int i=0;i<numOfTable;i++ ) {
			cellPos = tablesCatalog.readShort();
			curCellPos_list.add(cellPos);
		}
		
		/********
		 * check which cell is target table name
		 * ***/
		ArrayList<Boolean> boolResults = new ArrayList<Boolean>();
		
		int cellHeaderSize = 6;
		int typeCode;
		int tableName_stringSize;
		String curTableName;
		
		for(int i=0;i<numOfTable;i++ ) {
			
			cellPos = curCellPos_list.get(i);
			tablesCatalog.seek(cellPos+ cellHeaderSize +1);//pointer to the byte of type code
			typeCode = tablesCatalog.read();// 1 byte, pointer is then to the beginning of content, table_name strings
			tableName_stringSize = typeCode - 12;// it's Text type
			/****
			 * get each table name
			 * */
			byte[] b = new byte[tableName_stringSize];
			tablesCatalog.read(b);
			curTableName = new String(b);
			//debug
			//System.out.println("Debug Info: Now find table-name: "+ curTableName );//debug
			//System.out.println("Debug Info: Compare result is: "+ curTableName.equals(targetTableName) );//debug
			//debug
			
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
				numOfTable -= 1;// per deletion if true, decrease the cell count by 1
			}
			
		}
		
		/***
		 * update page header
		 * **/
		tablesCatalog.seek(2);// pointer to the byte storing cell counts
		tablesCatalog.writeShort(numOfTable);//2-byte, write in the new cell counts
		tablesCatalog.writeShort(cellBeginOffset);//2-byte,  write in the updated top cell offset
		
		/**
		 * update the page content
		 * **/
		for(int i=0;i<newCellPos_list.size();i++) {
			//update the cell location array
			//by rewriting in the new cell offsets
			tablesCatalog.seek(16+2*i);
			int remainCellPos = newCellPos_list.get(i);
			tablesCatalog.writeShort(remainCellPos);// 2-byte
			
			//update the rowid in each remaining cell
			int new_rowid = i+1;// rowid starts from 1
			tablesCatalog.seek(remainCellPos+2);// pointer to the rowid offset of the remaining record
			tablesCatalog.writeInt(new_rowid);//4-byte, write in the new rowid
			
		}
		
		/**Finish deletion
		 * */
		
		tablesCatalog.close();
		return;
	}
	
	/***********************************************************************/
}
