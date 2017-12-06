package test;

import java.io.BufferedReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.ArrayList;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;

public class JsonTest {
	static String path = "/Users/ccl/OneDrive/Documents/UAB/04.2017_Fall/"
			+ "cs616-BigDataProgramming/projects/project2-LoL/";
	static String folder = "input/";
	static String file = "1.out";
	static String fileName = path+folder+file;

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		

		// This will reference one line at a time
		String line = null;
		String content = "";
		try {
			// FileReader reads text files in the default encoding.
			FileReader fileReader = new FileReader(fileName);

			// Always wrap FileReader in BufferedReader.
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			while ((line = bufferedReader.readLine()) != null) {
				// System.out.println(line);
				content += line;
			}

			// Always close files.
			bufferedReader.close();
		} catch (FileNotFoundException ex) {
			System.out.println("Unable to open file '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("Error reading file '" + fileName + "'");
			// Or we could just do this:
			// ex.printStackTrace();
		}



	}



}