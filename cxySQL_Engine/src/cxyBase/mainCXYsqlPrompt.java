package cxyBase;

import java.io.IOException;
//import java.io.RandomAccessFile;
//import java.nio.ByteBuffer;
import java.text.ParseException;
//import java.io.File;
//import java.io.FileReader;
import java.util.Scanner;

/**
*  @author Xingyu Chen
*  @version 1.0
*  <b>
*  <p>This is an example of how to create an interactive prompt</p>
*  <p>There is also some guidance to get started with read/write of
*     binary data files using the RandomAccessFile class from the
*     Java Standard Library.</p>
*  </b>
*
*/

public class mainCXYsqlPrompt {
	
	/* 
	 *  The Scanner class is used to collect user commands from the prompt
	 *  There are many ways to do this. This is just one.
	 *
	 *  Each time the semicolon (;) delimiter is entered, the userCommand 
	 *  String is re-populated.
	 */
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	public static void main(String[] args) throws IOException, ParseException{
		
		/* Initialize the data storage */
		Settings.initializeDataStore();
		
		/* Display the welcome screen */
		Utils.splashScreen();

		/*Loop the prompt to receive the command from user*/
		promptLoop();
		
		System.out.println("Exiting...");
		
		return;

	}
	
	public static void promptLoop() throws IOException, ParseException {
		
		/* Variable to hold user input from the prompt */
		String userCommand = ""; 

		while(!Settings.isExit()) {
			
			System.out.println(Utils.printSeparator("-",80));//print the separator 
			
			System.out.print(Settings.getPrompt());// update prompt
			
			/** Strip newlines and carriage returns 
			 *  get new command
			 * */
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim();			
			
			Commands.parseUserCommand(userCommand);
		}
		
	}

}