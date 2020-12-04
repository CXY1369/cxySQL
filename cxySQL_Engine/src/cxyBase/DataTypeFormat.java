package cxyBase;

import java.io.IOException;
import java.io.RandomAccessFile;

public class DataTypeFormat {
	
	public static int getDatatypeCode(String datatypeName) {
		
		datatypeName = datatypeName.toLowerCase();
		
		switch(datatypeName) {
		
		case "null":
			return 0;
		case "byte":
			return 1;
		case "short":
			return 2;
		case "integer":
			return 3;
		case "long":
			return 4;
		case "float":
			return 5;
		case "double":
			return 6;
		case "year":
			return 8;
		case "time":
			return 9;
		case "datetime":
			return 10;
		case "date":
			return 11;
		case "text":
			return 12;
		default:
			System.out.println("Invalid data type, please double check!");
			break;
			
		}
		
		return -1;
	}
	
	public static String getDatatype(int code) {
		
		if(code>=12) {
			return "Text";
		}
		
		switch(code) {
		
		case 0:
			return "Null";
		case 1:
			return "Byte";
		case 2:
			return "Short";
		case 3:
			return "Integer";
		case 4:
			return "Long";
		case 5:
			return "Float";
		case 6:
			return "Double";
		case 8:
			return "Year";
		case 9:
			return "Time";
		case 10:
			return "Datetime";
		case 11:
			return "Date";
		
		default:
			System.out.println("Invalid data type, please double check!");
			break;
		}
		return "Invalid Type";
		
	}
	
	public static int datatypeCode_toSize(int code) {
		
		if(code>=12) {
			return code-12;
		}
		
		switch(code) {
		
		case 0:
			return 0;
		case 1:
			return 1;
		case 2:
			return 2;
		case 3:
			return 4;
		case 4:
			return 8;
		case 5:
			return 4;
		case 6:
			return 8;
		case 8:
			return 1;
		case 9:
			return 4;
		case 10:
			return 8;
		case 11:
			return 8;
		default:
			System.out.println("Invalid data type occured, please double check!");
			break;
		}
		return -1;
	}

	public static String showData(int code, RandomAccessFile TableFile, int offset ) throws IOException {
		
		int size = datatypeCode_toSize(code);
		TableFile.seek(offset);
		
		if(code>=12) {
			byte[] b = new byte[size];
			TableFile.read(b);
			return new String(b);
		}
		
		switch(code) {
		
		case 0:
			return "";
		case 1:
			return ""+TableFile.readByte();
		case 2:
			return ""+TableFile.readShort();
		case 3:
			return ""+TableFile.readInt();
		case 4:
			return ""+TableFile.readLong();
		case 5:
			return ""+TableFile.readFloat();
		case 6:
			return ""+TableFile.readDouble();
		case 8:
			return ""+( TableFile.readByte()+2000 );
		case 9:
			int time = TableFile.readInt();
			if(time<0 || time>=86400000) {
				return "Invalid TIme";
			}
			return ""+time;
		case 10:
			
			byte[] buffer = new byte[size];
			TableFile.read(buffer);
			long  epoch = BytesTo_unsignedLong( buffer);
			//Long epoch = TableFile.readLong();// This is signed value
			String epochDate = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date (epoch*1000));
			return epochDate;
		case 11:
			byte[] buf = new byte[size];
			TableFile.read(buf);
			long  Time = BytesTo_unsignedLong(buf);   
		    
			Long Hour = Time/(3600*1000);
			Time = Time%(3600*1000);
			
			Long Minute = Time/(60*1000);
			Time = Time%(60*1000);
					
			return ""+Hour+":"+Minute+":"+Time/1000;
		
		default:
			break;
		}
		return "Invalid Type";
		
	}
	
	
	/*Transfer the byte[8] to unsigned Long Integer*/
	public static long BytesTo_unsignedLong(byte[] buffer) {   
	    long  values = 0;   
	    for (int i = 0; i < 8; i++) {    
	        values <<= 8; values|= (buffer[i] & 0xff);   
	    }   
	    return values;  
	 } 
	
	/*Transfer 2-byte short integer to byte[] */
	public static byte[] short2byte(short s){
        byte[] b = new byte[2]; 
        for(int i = 0; i < 2; i++){
            int offset = 16 - (i+1)*8; //因为byte占4个字节，所以要计算偏移量
            b[i] = (byte)((s >> offset)&0xff); //把16位分为2个8位进行分别存储
        }
        return b;
   }
	
	/*Transfer 4-byte integer to byte[] */
	public static byte[] int2byte(int s){
        byte[] b = new byte[4]; 
        for(int i = 0; i < 4; i++){
            int offset = 16 - (i+1)*8; //因为byte占4个字节，所以要计算偏移量
            b[i] = (byte)((s >> offset)&0xff); //把32位分为4个8位进行分别存储
        }
        return b;
   }
	
}
