### Datax MongoDBReader
#### 1 快速介绍

MongoDBReader 插件利用 MongoDB 的java客户端MongoClient进行MongoDB的读操作。最新版本的Mongo已经将DB锁的粒度从DB级别降低到document级别，配合上MongoDB强大的索引功能，基本可以达到高性能的读取MongoDB的需求。

#### 2 实现原理

MongoDBReader通过Datax框架从MongoDB并行的读取数据，通过主控的JOB程序按照指定的规则对MongoDB中的数据进行分片，并行读取，然后将MongoDB支持的类型通过逐一判断转换成Datax支持的类型。

#### 3 功能说明
* 该示例从ODPS读一份数据到MongoDB。

	    {
	    "job": {
	        "setting": {
	            "speed": {
	                "channel": 2
	            }
	        },
	        "content": [
	            {
	                "reader": {
	                    "name": "mongodbreader",
	                    "parameter": {
	                        "address": ["127.0.0.1:27017"],
	                        "userName": "",
	                        "userPassword": "",
	                        "dbName": "tag_per_data",
	                        "collectionName": "tag_data12",
	                        "column": [
	                            {
	                                "name": "unique_id",
	                                "type": "string"
	                            },
	                            {
	                                "name": "sid",
	                                "type": "string"
	                            },
	                            {
	                                "name": "user_id",
	                                "type": "string"
	                            },
	                            {
	                                "name": "auction_id",
	                                "type": "string"
	                            },
	                            {
	                                "name": "content_type",
	                                "type": "string"
	                            },
	                            {
	                                "name": "pool_type",
	                                "type": "string"
	                            },
	                            {
	                                "name": "frontcat_id",
	                                "type": "Array",
	                                "spliter": ""
	                            },
	                            {
	                                "name": "categoryid",
	                                "type": "Array",
	                                "spliter": ""
	                            },
	                            {
	                                "name": "gmt_create",
	                                "type": "string"
	                            },
	                            {
	                                "name": "taglist",
	                                "type": "Array",
	                                "spliter": " "
	                            },
	                            {
	                                "name": "property",
	                                "type": "string"
	                            },
	                            {
	                                "name": "scorea",
	                                "type": "int"
	                            },
	                            {
	                                "name": "scoreb",
	                                "type": "int"
	                            },
	                            {
	                                "name": "scorec",
	                                "type": "int"
	                            }
	                        ]
	                    }
	                },
	                "writer": {
	                    "name": "odpswriter",
	                    "parameter": {
	                        "project": "tb_ai_recommendation",
	                        "table": "jianying_tag_datax_read_test01",
	                        "column": [
	                            "unique_id",
	                            "sid",
	                            "user_id",
	                            "auction_id",
	                            "content_type",
	                            "pool_type",
	                            "frontcat_id",
	                            "categoryid",
	                            "gmt_create",
	                            "taglist",
	                            "property",
	                            "scorea",
	                            "scoreb"
	                        ],
	                        "accessId": "**************",
	                        "accessKey": "********************",
	                        "truncate": true,
	                        "odpsServer": "xxx/api",
	                        "tunnelServer": "xxx",
	                        "accountType": "aliyun"
	                    }
	                }
	            }
	        ]
	    }
        }
#### 4 参数说明

* address： MongoDB的数据地址信息，因为MonogDB可能是个集群，则ip端口信息需要以Json数组的形式给出。【必填】
* userName：MongoDB的用户名。【选填】
* userPassword： MongoDB的密码。【选填】
* collectionName： MonogoDB的集合名。【必填】
* column：MongoDB的文档列名。【必填】
* name：Column的名字。【必填】
* type：Column的类型。【选填】
* splitter：因为MongoDB支持数组类型，但是Datax框架本身不支持数组类型，所以mongoDB读出来的数组类型要通过这个分隔符合并成字符串。【选填】

#### 5 类型转换

| DataX 内部类型| MongoDB 数据类型    |
| -------- | -----  |
| Long     | int, Long |
| Double   | double |
| String   | string, array |
| Date     | date  |
| Boolean  | boolean |
| Bytes    | bytes |


#### 6 性能报告
#### 7 测试报告
#### 8 功能补充
	为了能扁平化结构加入了扁平化的方法：将数组对象中的字段拼到最外层相当于多条记录。一个嵌套字段加上最外层的字段放到一个新的二维表（注意：目前只支持嵌套两层）
	eg: {
    		"_id" : 20738084,
				"subject_pref" : [
			 {
					 "score" : 3.98426193375504,
					 "click_score" : 0.726463484550927,
					 "subject" : "wl"
			 },
			 {
					 "score" : 2.92215943564494,
					 "click_score" : 0.541341132946451,
					 "subject" : "sw"
			 },
			 {
					 "score" : 1.69236771687384,
					 "click_score" : 0.135335283236613,
					 "subject" : "yy"
			 },
			 {
					 "score" : 1.21464470851585,
					 "click_score" : 0.320457634841089,
					 "subject" : "sx"
			 },
			 {
					 "score" : 0.568085989731626,
					 "click_score" : 0.320457634841089,
					 "subject" : "hx"
			 },
			 {
					 "score" : 0.552844632021563,
					 "click_score" : 0.135335283236613,
					 "subject" : "yw"
			 }
	 ]
 	}
	变为：
	wl,0.7264634845509275,20738084,3.984261933755039
	sw,0.5413411329464508,20738084,2.922159435644941
	yy,0.1353352832366127,20738084,1.6923677168738418
	sx,0.32045763484108936,20738084,1.214644708515853
	hx,0.32045763484108936,20738084,0.5680859897316257
	yw,0.1353352832366127,20738084,0.5528446320215629

	配置方式如下
			主要修改的是 column 这一项
		1、该数组第一个元素必须为
			{
				"syncType" : "flatten"  // flatten / normal 两种模式flatten为这次开发的扁平化模式  normal为之前的一般模式
			}

		2、先配置扁平化前的字段
		{
			"name": "_id",
			"type": "flatten^Integer"
		},{
			"name": "weak_knowledges",
			"type": "flatten^ArrayObjectLevelTwo"
		}
		type必须以 "flatten^" 开头 后面为 ArrayObjectLevelTwo、ArrayObjectLevelOne 的字段将扁平化其余字段不变
		扁平化后字段名字有所改变所以，column的后面追加的配置为
		{
			"name": "weak_knowledges^bad_knowledges^lscore",
			"type": "Double"
		},{
			"name": "weak_knowledges^bad_knowledges^id",
			"type": "Double"
		},{
			"name": "_id",
			"type": "String"
		},{
			"name": "weak_knowledges^bad_knowledges^ques_num",
			"type": "Double"
		},{
			"name": "weak_knowledges^subject",
			"type": "String"
		},{
			"name": "weak_knowledges^bad_knowledges^level",
			"type": "Double"
		}

		所以完整的 column 配置为
		"column": [
				{
					"syncType" : "flatten"
				},
				{
					"name": "_id",
					"type": "flatten^Integer"
				},{
					"name": "weak_knowledges",
					"type": "flatten^ArrayObjectLevelTwo"
				},{
					"name": "weak_knowledges^bad_knowledges^lscore",
					"type": "Double"
				},{
					"name": "weak_knowledges^bad_knowledges^id",
					"type": "Double"
				},{
					"name": "_id",
					"type": "String"
				},{
					"name": "weak_knowledges^bad_knowledges^ques_num",
					"type": "Double"
				},{
					"name": "weak_knowledges^subject",
					"type": "String"
				},{
					"name": "weak_knowledges^bad_knowledges^level",
					"type": "Double"
				}
		]
