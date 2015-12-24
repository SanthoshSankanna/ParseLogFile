package com.mphasis;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

import org.json.simple.JSONObject;

public class ParsingLogFile {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		
		FileInputStream fstream = null;
		BufferedReader br = null;
		
		//Prepare JSON Object
		JSONObject mainObject = new JSONObject();
		
		if(0 < args.length) {
			try {
				//fstream = new FileInputStream("C:\\Temp\\lognz.txt");
				fstream = new FileInputStream(args[0]);
				br = new BufferedReader(new InputStreamReader(fstream));
				String strLine = null;
				
				
				/* read log line by line */
				while ((strLine = br.readLine()) != null) {
					
					//Now Parse the Single line
					Scanner scanner = new Scanner(strLine);
					
					//Using delimiter for one or more white spaces like single_space or multiple_space or tab_space 
				    scanner.useDelimiter("\\s+");
				    
				    
				    if (scanner.hasNext()) {
					    //Read Date Value
					    String dateValue = scanner.next();
					    
					    //Read Timestamp Value
					    String timestampValue = scanner.next();
					    
					    //Split time stamp value into two parts one is time value and another is milliseconds
					    String[] twoParts = timestampValue.split("\\.");
					    String timeVal = twoParts[0];
					    String secondsVal = twoParts[1];
					    
					    //Read TimeZone Value
					    String timeZoneValue = scanner.next();
					    
					    //Read Process ID Value
					    String processID = scanner.next();
					    
					    //Check whether the process is already added into JSON or not?
					    if(mainObject.get(processID) == null) {
					    	
					    	//New Process so new entry into JSON file
							JSONObject object = new JSONObject();
							object.put("connectionDate", dateValue);
							//object.put("connectionTimestamp", timestampValue);
							object.put("connectionTime", timeVal);
							object.put("connectionmilliseconds", secondsVal);
							object.put("connectionTimezone", timeZoneValue);
							object.put("error", "");
							
							//Add the Process as Key and Object as Value to JSON
							mainObject.put(processID, object);
					    }
					    
					    //Read Process definition Value
					    String processDef = scanner.next();
					    
					    if(processDef.equals("DEBUG:")) {
					    	
					    	//Read Inner Process Token Values
						    String innerToken = scanner.next();
						    
						    //Connection Started, Fetch Connection Details and put those details in JSON
					    	if(innerToken.equals("connection:")) {
					    		
						    	//Read HOST Value
							    String hostData = scanner.next();
							    String[] parts = hostData.split("=");
							    String hostVal = parts[1];
							    
							    //Read USER Value
							    String userData = scanner.next();
							    parts = userData.split("=");
							    String userVal = parts[1];
							    
							    //Read DATABASE Value
							    String databaseData = scanner.next();
							    parts = databaseData.split("=");
							    String databaseVal = parts[1];
							    
							    //if the Process ID key is already present in the JSON then 
							    JSONObject jsonObject = (JSONObject) mainObject.get(processID);
							    jsonObject.put("Host", hostVal);
							    jsonObject.put("user", userVal);
							    jsonObject.put("database", databaseVal);
							    
						    	continue;//Done with this line so Go to next line
						    }
						    else if (innerToken.equals("Session")) {
						    	
						    	//Fetch Session Id value
							    scanner.next();//Skip "Id" word
							    scanner.next();//Skip "is" word
							    String sessionIdVal = scanner.next();//Grab the number
							    JSONObject jsonObject = (JSONObject) mainObject.get(processID);
							    jsonObject.put("sessionid", sessionIdVal);
							    
						    	continue;//Done with this line so Go to next line
						    }
						    else if (innerToken.equals("QUERY:")) {
						    	
						    	//Ignoring the line which starts with query
						    	continue;//Done with this line so Go to next line
						    }
						    else if (innerToken.equals("disconnect:")) {
						    	
						    	JSONObject jsonObject = (JSONObject) mainObject.get(processID);
						    	jsonObject.put("disconnectionDate", dateValue);
						    	//jsonObject.put("disconnectionTimestamp", timestampValue);
						    	jsonObject.put("disconnectionTime", timeVal);
						    	jsonObject.put("disconnectionMilliseconds", secondsVal);
						    	jsonObject.put("disconnectionTimezone", timeZoneValue);
						    	continue;
						    }
					    }
					    else if(processDef.equals("ERROR:")) {
					    	
					    	//Read Inner Process Token Values
						    String innerToken = scanner.next();
						    
					    	if (innerToken.equals("QUERY:")) {
					    		//Add error values
					    		JSONObject jsonObject = (JSONObject) mainObject.get(processID);
					    		String errorString = "";
					    		//Fetch all the values till the end of the line
					    		while(scanner.hasNext()) {
					    			errorString = errorString + scanner.next() + " ";
					    		}
					    		jsonObject.put("error", errorString);
						    	continue;
						    }
					    }
				    }
				    //Close the Scanner Object
				    scanner.close();
				}
				System.out.println("Parsing is finished Successfully");
				System.out.println("Input File = " + args[0]);
			    System.out.println("Output File is placed in the same directory of the input file");
			    
			} catch (IOException x) {
			    System.err.format("IOException: %s%n", x);
			} catch (Exception e) {
				System.out.println("Error: " + e);
			} finally {
	            try {
	            	br.close();
	            	fstream.close();
	            } catch (IOException ex) {
	                //Logger.getLogger(BufferedReaderExample.class.getName()).log(Level.SEVERE, null, ex);
	            }
	        }
			try {
				FileWriter file = new FileWriter("test.json");
				file.write(mainObject.toJSONString());
				file.flush();
				file.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else {
			System.out.println("Please pass the absolute path of the file as an argument to the program");
			System.out.println("Example Path = C:\\Temp\\lognz.txt");
		}
    }
}