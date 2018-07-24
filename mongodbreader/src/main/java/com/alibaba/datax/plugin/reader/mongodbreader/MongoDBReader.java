package com.alibaba.datax.plugin.reader.mongodbreader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import org.bson.Document;
import org.bson.types.ObjectId;

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

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by mingyan.zc on 2016/6/13.
 * Modified by mingyan.zc on 2017/7/5.
 */
public class MongoDBReader extends Reader {

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            return CollectionSplitUtil.doSplit(originalConfig, adviceNumber, mongoClient);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.userName = originalConfig.getString(KeyConstant.MONGO_USER_NAME, originalConfig.getString(KeyConstant.MONGO_USERNAME));
            this.password = originalConfig.getString(KeyConstant.MONGO_USER_PASSWORD, originalConfig.getString(KeyConstant.MONGO_PASSWORD));
            String database = originalConfig.getString(KeyConstant.MONGO_DB_NAME, originalConfig.getString(KeyConstant.MONGO_DATABASE));
            String authDb = originalConfig.getString(KeyConstant.MONGO_AUTHDB, database);
            if (!Strings.isNullOrEmpty(this.userName) && !Strings.isNullOrEmpty(this.password)) {
                this.mongoClient = MongoUtil.initCredentialMongoClient(originalConfig, userName, password, authDb);
            } else {
                this.mongoClient = MongoUtil.initMongoClient(originalConfig);
            }
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        private String authDb = null;
        private String database = null;
        private String collection = null;

        private String query = null;

        private JSONArray mongodbColumnMeta = null;
        private Object lowerBound = null;
        private Object upperBound = null;
        private boolean isObjectId = true;

        @Override
        public void startRead(RecordSender recordSender) {

            if (lowerBound == null || upperBound == null ||
                    mongoClient == null || database == null ||
                    collection == null || mongodbColumnMeta == null) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                        MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
            }
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection col = db.getCollection(this.collection);

            MongoCursor<Document> dbCursor = null;
            Document filter = new Document();
            if (lowerBound.equals("min")) {
                if (!upperBound.equals("max")) {
                    filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$lt", isObjectId ? new ObjectId(upperBound.toString()) : upperBound));
                }
            } else if (upperBound.equals("max")) {
                filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$gte", isObjectId ? new ObjectId(lowerBound.toString()) : lowerBound));
            } else {
                filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$gte", isObjectId ? new ObjectId(lowerBound.toString()) : lowerBound).append("$lt", isObjectId ? new ObjectId(upperBound.toString()) : upperBound));
            }
            if (!Strings.isNullOrEmpty(query)) {
                Document queryFilter = Document.parse(query);
                filter = new Document("$and", Arrays.asList(filter, queryFilter));
            }
            FindIterable<Document> documents = col.find(filter);

            // result发送条数阈值 默认10000
            int threshold = 10000;
            String send_threshold = readerSliceConfig.getString("send_threshold");
            if (!Strings.isNullOrEmpty(send_threshold)) {
                threshold = Integer.valueOf(send_threshold);
            }

            // 拼凑查询条件
            String query_params = readerSliceConfig.getString("query_params");
            if (!Strings.isNullOrEmpty(query_params)) {
                JSONObject jsonObject = JSONObject.parseObject(query_params);
                String skip = jsonObject.getString("skip");
                String limit = jsonObject.getString("limit");
                BasicDBObject sort = (BasicDBObject) com.mongodb.util.JSON.parse(jsonObject.getString("sort"));
                BasicDBObject projection = (BasicDBObject) com.mongodb.util.JSON.parse(jsonObject.getString("projection"));
                if (null != sort) {
                    documents.sort(sort);
                }
                if (!Strings.isNullOrEmpty(skip)) {
                    documents.skip(Integer.valueOf(skip));
                }
                if (!Strings.isNullOrEmpty(limit)) {
                    documents.limit(Integer.valueOf(limit));
                }
                if (null != projection) {
                    documents.projection(projection);
                }
            }
            dbCursor = documents.iterator();

            //先进行拆解
            List<JSONObject> midList = new ArrayList<JSONObject>();
            String syncType = "normal";
            String sync_type = readerSliceConfig.getString("syncType");
            if (!Strings.isNullOrEmpty(sync_type)) {
                syncType = sync_type;
            }
            JSONArray newMongodbColumnMeta = new JSONArray();
            JSONArray midMongodbColumnMeta = new JSONArray();
            //扁平化模式
            if (syncType.equals("flatten")) {
                for (int i = 0; i < mongodbColumnMeta.size(); i++) {
                    JSONObject colObject = (JSONObject) mongodbColumnMeta.get(i);
                    String type = colObject.getString("type");
                    if (type.startsWith("flatten^")) {
                        JSONObject newMidColObject = new JSONObject();
                        newMidColObject.put("name", colObject.getString("name"));
                        newMidColObject.put("type", colObject.getString("type").split("flatten\\^")[1]);
                        midMongodbColumnMeta.add(newMidColObject);
                    } else {
                        JSONObject newColObject = new JSONObject();
                        newColObject.put("name", colObject.getString("name"));
                        newColObject.put("type", colObject.getString("type"));
                        newMongodbColumnMeta.add(newColObject);
                    }
                }
                int i = 0;
                while (dbCursor.hasNext()) {
                    Document item = dbCursor.next();
                    List<JSONObject> subMidList = new ArrayList<JSONObject>();
                    JSONObject preJson = new JSONObject();
                    for (Object column : midMongodbColumnMeta) {
                        JSONObject colObject = (JSONObject) column;
                        String type = colObject.getString("type");
                        String name = colObject.getString("name");
                        Object nestedObject = item.get(name);

                        //
						if (nestedObject == null) {
							if (type.equals("document")) {
								String[] names = name.split("\\.");
								if (names.length > 1) {
									Object obj;
									Document nestedDocument = item;
									for (String str : names) {
										obj = nestedDocument.get(str);
										if (obj instanceof Document) {
											nestedDocument = (Document) obj;
										}
									}
									if (null != nestedDocument) {
										Document doc = nestedDocument;
										nestedObject = doc.get(names[names.length - 1]);
									}
								}
							}
						}
						if (nestedObject == null) {
							// continue; 这个不能直接continue会导致record到目的端错位
							preJson.put(name, null);
						}
						//

						else if (nestedObject instanceof Double) {
                            preJson.put(name, (Double) nestedObject);
                        } else if (nestedObject instanceof Boolean) {
                            preJson.put(name, (Boolean) nestedObject);
                        } else if (nestedObject instanceof Date) {
                            preJson.put(name, (Date) nestedObject);
                        } else if (nestedObject instanceof Integer) {
                            preJson.put(name, (Integer) nestedObject);
                        } else if (nestedObject instanceof Long) {
                            preJson.put(name, (Long) nestedObject);
                        } else if (nestedObject instanceof String) {
                            preJson.put(name, (String) nestedObject);
                        } else {
                        	preJson.put(name, nestedObject.toString());
                        }
						if (type.equals("ArrayObjectLevelOne")) {
                            JSONObject originJson = (JSONObject) preJson.clone();
                            RecursionDocument.getListJson(name, nestedObject, subMidList, preJson, originJson);
                        } else if (type.equals("ArrayObjectLevelTwo")) {
                            JSONObject originJson = (JSONObject) preJson.clone();
                            RecursionDocument.getDoubleListJson(name, nestedObject, subMidList, preJson, originJson, false);
                        }
                        midList.addAll(subMidList);
                    }
                    i++;
                    if (i % threshold == 0) {
                        for (JSONObject _item : midList) {
                            Document doc = Document.parse(_item.toJSONString());
                            resultSendToWriter(recordSender, newMongodbColumnMeta, doc);
                        }
                        midList.clear();
                    }
                }
                if (midList.size() != 0) {
                    for (JSONObject _item : midList) {
                        Document doc = Document.parse(_item.toJSONString());
                        resultSendToWriter(recordSender, newMongodbColumnMeta, doc);
                    }
                }

               /* //重复代码
                for (JSONObject _item : midList) {
                    JSONObject item = dbCursor.next();
                    Document item = Document.parse(_item.toJSONString());
                }*/
            }

            //一般化模式
            if (syncType.equals("normal")) {
                for (int i = 0; i < mongodbColumnMeta.size(); i++) {
                    Object colObject = mongodbColumnMeta.get(i);
                    newMongodbColumnMeta.add(colObject);
                }
                //重复代码
                while (dbCursor.hasNext()) {
                    Document item = dbCursor.next();
                    resultSendToWriter(recordSender, newMongodbColumnMeta, item);
                }
            }
        }

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.userName = readerSliceConfig.getString(KeyConstant.MONGO_USER_NAME, readerSliceConfig.getString(KeyConstant.MONGO_USERNAME));
            this.password = readerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD, readerSliceConfig.getString(KeyConstant.MONGO_PASSWORD));
            this.database = readerSliceConfig.getString(KeyConstant.MONGO_DB_NAME, readerSliceConfig.getString(KeyConstant.MONGO_DATABASE));
            this.authDb = readerSliceConfig.getString(KeyConstant.MONGO_AUTHDB, this.database);
            if (!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
                mongoClient = MongoUtil.initCredentialMongoClient(readerSliceConfig, userName, password, authDb);
            } else {
                mongoClient = MongoUtil.initMongoClient(readerSliceConfig);
            }

            this.collection = readerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.query = readerSliceConfig.getString(KeyConstant.MONGO_QUERY);
            this.mongodbColumnMeta = JSON.parseArray(readerSliceConfig.getString(KeyConstant.MONGO_COLUMN));
            this.lowerBound = readerSliceConfig.get(KeyConstant.LOWER_BOUND);
            this.upperBound = readerSliceConfig.get(KeyConstant.UPPER_BOUND);
            this.isObjectId = readerSliceConfig.getBool(KeyConstant.IS_OBJECTID);
        }

        @Override
        public void destroy() {

        }

        // 将结果发送
        private void resultSendToWriter(RecordSender recordSender, JSONArray newMongodbColumnMeta, Document item) {
            Record record = recordSender.createRecord();
            Iterator columnItera = newMongodbColumnMeta.iterator();
            while (columnItera.hasNext()) {
                JSONObject column = (JSONObject) columnItera.next();
                Object tempCol = item.get(column.getString(KeyConstant.COLUMN_NAME));
                if (tempCol == null) {
                    if (KeyConstant.isDocumentType(column.getString(KeyConstant.COLUMN_TYPE))) {
                        String[] name = column.getString(KeyConstant.COLUMN_NAME).split("\\.");
                        if (name.length > 1) {
                            Object obj;
                            Document nestedDocument = item;
                            for (String str : name) {
                                obj = nestedDocument.get(str);
                                if (obj instanceof Document) {
                                    nestedDocument = (Document) obj;
                                }
                            }
                            if (null != nestedDocument) {
                                Document doc = nestedDocument;
                                tempCol = doc.get(name[name.length - 1]);
                            }
                        }
                    }
                }
                if (tempCol == null) {
                    //continue; 这个不能直接continue会导致record到目的端错位
                    record.addColumn(new StringColumn(null));
                } else if (tempCol instanceof Double) {
                    //TODO deal with Double.isNaN()
                    record.addColumn(new DoubleColumn((Double) tempCol));
                } else if (tempCol instanceof Boolean) {
                    record.addColumn(new BoolColumn((Boolean) tempCol));
                } else if (tempCol instanceof Date) {
                    record.addColumn(new DateColumn((Date) tempCol));
                } else if (tempCol instanceof Integer) {
                    record.addColumn(new LongColumn((Integer) tempCol));
                } else if (tempCol instanceof Long) {
                    record.addColumn(new LongColumn((Long) tempCol));
                } else {
                    if (KeyConstant.isArrayType(column.getString(KeyConstant.COLUMN_TYPE))) {
                        String splitter = column.getString(KeyConstant.COLUMN_SPLITTER);
                        if (Strings.isNullOrEmpty(splitter)) {
                            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                                    MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
                        } else {
                            ArrayList array = (ArrayList) tempCol;
                            String tempArrayStr = Joiner.on(splitter).join(array);
                            record.addColumn(new StringColumn(tempArrayStr));
                        }
                    } else {
                        record.addColumn(new StringColumn(tempCol.toString()));
                    }
                }
            }
            recordSender.sendToWriter(record);
        }
    }
}
