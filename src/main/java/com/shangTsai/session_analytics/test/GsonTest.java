package com.shangTsai.session_analytics.test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class GsonTest {

	private static JsonParser jsonParser = new JsonParser();
	
	public static void main(String[] args) {
		String json = "[{'name':'david', 'age':38, 'gender':'male'}, {'name':'mary', 'age':18, 'gender':'female'}, {'name':'jack', 'age':28, 'gender':'male'}]";
	
		JsonArray jsonArray = jsonParser.parse(json).getAsJsonArray();
		System.out.println(jsonArray);
		
		JsonElement david = jsonParser.parse(json).getAsJsonArray().get(0);
		System.out.println(david);
		
		String david_name = jsonParser.parse(json).getAsJsonArray()
				.get(0).getAsJsonObject()
				.get("name").getAsString();
		System.out.println(david_name);
		
		String name = jsonArray.get(0).getAsJsonObject().get("name").getAsString();
		
		
		System.out.println(name);
	}
}
