package com.alibaba.datax.plugin.reader.mongodbreader.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.util.CollectionSplitUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.util.MongoUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.util.RecursionDocument;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

public class RecursionDocument {

	// 数组中带对象
	public static void getListJson(String key, Object nestedObject, List<JSONObject> resultList, JSONObject preJson,
			JSONObject originJson) {
		if (nestedObject instanceof Double) {
			preJson.put(key, (Double) nestedObject);
		} else if (nestedObject instanceof Boolean) {
			preJson.put(key, (Boolean) nestedObject);
		} else if (nestedObject instanceof Date) {
			preJson.put(key, (Date) nestedObject);
		} else if (nestedObject instanceof Integer) {
			preJson.put(key, (Integer) nestedObject);
		} else if (nestedObject instanceof Long) {
			preJson.put(key, (Long) nestedObject);
		} else if (nestedObject instanceof String) {
			preJson.put(key, (String) nestedObject);
		} else if (nestedObject instanceof Map) {
			Document nestedDocument = (Document) nestedObject;
			// 延迟处理该字段表示字段中有array 先处理基本类型
			List<Entry<String, Object>> delay_list = new ArrayList<Entry<String, Object>>();
			for (Entry<String, Object> eachDocument : nestedDocument.entrySet()) {
				if (!(eachDocument.getValue() instanceof List)) {
					String thisNewKey = key + "^" + eachDocument.getKey();
					getListJson(thisNewKey, eachDocument.getValue(), resultList, preJson, originJson);
				} else {
					delay_list.add(eachDocument);
				}
			}
			for (Entry<String, Object> delay_element : delay_list) {
				originJson = (JSONObject) preJson.clone();
				String thisNewKey = key + "^" + delay_element.getKey();
				getListJson(thisNewKey, delay_element.getValue(), resultList, preJson, originJson);
				preJson = (JSONObject) originJson.clone();
			}
		} else if (nestedObject instanceof List) {
			List<Document> listObject = (List<Document>) nestedObject;
			if (listObject.size() == 0) {
				JSONObject saveObject = (JSONObject) preJson.clone();
				resultList.add(saveObject);
			}
			for (Document document : listObject) {
				preJson = (JSONObject) originJson.clone();
				getListJson(key, document, resultList, preJson, originJson);
				JSONObject saveObject = (JSONObject) preJson.clone();
				resultList.add(saveObject);
				preJson = (JSONObject) originJson.clone();
			}
		}
	}

	// 数组中带对象 对象中又嵌套数组
	public static void getDoubleListJson(String key, Object nestedObject, List<JSONObject> resultList,
			JSONObject preJson, JSONObject originJson, boolean is_inner) {
		if (nestedObject instanceof Double) {
			preJson.put(key, (Double) nestedObject);
		} else if (nestedObject instanceof Boolean) {
			preJson.put(key, (Boolean) nestedObject);
		} else if (nestedObject instanceof Date) {
			preJson.put(key, (Date) nestedObject);
		} else if (nestedObject instanceof Integer) {
			preJson.put(key, (Integer) nestedObject);
		} else if (nestedObject instanceof Long) {
			preJson.put(key, (Long) nestedObject);
		} else if (nestedObject instanceof String) {
			preJson.put(key, (String) nestedObject);
		} else if (nestedObject instanceof Map) {
			Document nestedDocument = (Document) nestedObject;
			// 延迟处理该字段表示字段中有array 先处理基本类型
			List<Entry<String, Object>> delay_list = new ArrayList<Entry<String, Object>>();
			for (Entry<String, Object> eachDocument : nestedDocument.entrySet()) {
				if (!(eachDocument.getValue() instanceof List)) {
					String thisNewKey = key + "^" + eachDocument.getKey();
					getDoubleListJson(thisNewKey, eachDocument.getValue(), resultList, preJson, originJson, false);
				} else {
					delay_list.add(eachDocument);
				}
			}
			for (Entry<String, Object> delay_element : delay_list) {
				originJson = (JSONObject) preJson.clone();
				String thisNewKey = key + "^" + delay_element.getKey();
				getDoubleListJson(thisNewKey, delay_element.getValue(), resultList, preJson, originJson, true);
				preJson = (JSONObject) originJson.clone();
			}
		} else if (nestedObject instanceof List) {
			List<Document> listObject = (List<Document>) nestedObject;
			if (listObject.size() == 0) {
				JSONObject saveObject = (JSONObject) preJson.clone();
				resultList.add(saveObject);
			}
			for (Document document : listObject) {
				if (!is_inner) {
					preJson = (JSONObject) originJson.clone();
				}
				getDoubleListJson(key, document, resultList, preJson, originJson, false);
				if (is_inner) {
					JSONObject saveObject = (JSONObject) preJson.clone();
					resultList.add(saveObject);
					preJson = (JSONObject) originJson.clone();
				}
			}
		}
	}

//	// 对象嵌套
//	public static void getObjectJson(String key, Object nestedObject, List<JSONObject> resultList, JSONObject preJson,
//			JSONObject originJson) {
//		if (nestedObject instanceof Map) {
//			Document nestedDocument = (Document) nestedObject;
//			for (Entry<String, Object> eachDocument : nestedDocument.entrySet()) {
//				String thisNewKey = key + "^" + eachDocument.getKey();
//				getObjectJson(thisNewKey, eachDocument.getValue(), resultList, preJson, originJson);
//			}
//		} else if (nestedObject instanceof Double) {
//			preJson.put(key, (Double) nestedObject);
//		} else if (nestedObject instanceof Boolean) {
//			preJson.put(key, (Boolean) nestedObject);
//		} else if (nestedObject instanceof Date) {
//			preJson.put(key, (Date) nestedObject);
//		} else if (nestedObject instanceof Integer) {
//			preJson.put(key, (Integer) nestedObject);
//		} else if (nestedObject instanceof Long) {
//			preJson.put(key, (Long) nestedObject);
//		} else if (nestedObject instanceof String) {
//			preJson.put(key, (String) nestedObject);
//		}
//	}

}
