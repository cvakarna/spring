package com.biz;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;
import org.springframework.stereotype.Component;

import com.biz.model.Fields;
import com.biz.model.Table;
import com.biz.model.TableDetails;
import com.biz.model.TableFieldsInfo;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@Component
public class TableUtil {

	protected String creatTable(String table) {

		String dbName = "springbootdb";
		Gson gson = new Gson();
		Table t = gson.fromJson(table, Table.class);
		JsonParser jsonParser = new JsonParser();
		JsonObject tableAsJsonObj = (JsonObject) jsonParser.parse(table);
		JsonElement ele = tableAsJsonObj.get("TableDetails");
		TableDetails tableDetails = gson.fromJson(ele, TableDetails.class);

		JsonElement info = tableAsJsonObj.get("TableFieldsInfo");
		TableFieldsInfo tableFieldsInfo = gson.fromJson(info, TableFieldsInfo.class);
		List<Fields> fieldsList = tableFieldsInfo.getFields();

		String tableName = dbName + "." + tableDetails.getTableName().trim();
		String tableDescription = tableDetails.getTableDescription();

		Map<String, String> mapResult = createColumns(fieldsList);

		String partialQuery = mapResult.get("columnQuery");
		String indexColumnJson = mapResult.get("indexes");

		String query = "CREATE TABLE IF NOT EXISTS " + tableName + "(" + partialQuery + ") COMMENT '" + tableDescription
				+ "'";

		return query;

	}
    protected String DisplayTableInfo(SqlRowSet rowset,SqlRowSet tableRowset,String tableName)
    {
    	String tableDescription = "";
    	TableDetails tableDetails = new TableDetails();
    	TableFieldsInfo info = new TableFieldsInfo();
    	List<Fields> fieldsList = new ArrayList<>();
    	Gson gson =  new GsonBuilder().serializeNulls().create();
    	while(rowset.next())
    	{
    		
    		String fieldName = rowset.getString("COLUMN_NAME");
    		String type = rowset.getString("DATA_TYPE");
    		String columnType = rowset.getString("COLUMN_TYPE").split("\\(")[1];
    		String length = columnType.substring(0,columnType.length()-1);
    		boolean isNull = false;
    		String checkNull = rowset.getString("IS_NULLABLE");
    		if(!checkNull.equals("NO")){
    			isNull = true;
    		}
    			
    		
    		String collationName = rowset.getString("COLLATION_NAME");
    		
    		String columnKey = rowset.getString("COLUMN_KEY");
    		String columnComment = rowset.getString("COLUMN_COMMENT");
    		Fields field = new Fields(fieldName,type,length,collationName,isNull,columnKey,columnComment);
    		fieldsList.add(field);
    		
    	}
    	if(tableRowset.next())
    	{
    		tableDescription = tableRowset.getString("TABLE_COMMENT");
    		
    		
    	}
    	info.setFields(fieldsList);
    	JsonObject jsonObj = new JsonObject();
    	tableDetails.setTableDescription(tableDescription);
    	tableDetails.setTableName(tableName);
    	String tableDetailsAsJson = gson.toJson(tableDetails);
    	JsonParser parser = new JsonParser();
		JsonObject tableJSON = (JsonObject) parser.parse(tableDetailsAsJson);
		
		
    	
    	String fieldsInfo = gson.toJson(info);
    	JsonObject fieldsInfoJson  = (JsonObject)parser.parse(fieldsInfo);
    	jsonObj.add("TableDetails",tableJSON);
    	jsonObj.add("TableFieldsInfo", fieldsInfoJson);
    	
    	String finalJson = jsonObj.toString();
    	
    	
    	return finalJson;
    }
	protected String getTableDefinition(SqlRowSet rowSet) {
		List<TableData> tableList = new LinkedList<>();
		Gson mapper = new GsonBuilder().serializeNulls().create();
		while (rowSet.next()) {

			String createdBy = "";
			String changedby = "";
			String tableName = rowSet.getString("TABLE_NAME");
			String tableDescription = rowSet.getString("TABLE_COMMENT");
			String tableChangedOn = rowSet.getString("UPDATE_TIME");
			String createdOn = rowSet.getString("CREATE_TIME");
			TableData data = new TableData(tableName, tableDescription, createdOn, createdBy, tableChangedOn,
					changedby);
			tableList.add(data);
		}
		String resultAsJson = mapper.toJson(tableList);
		// List<Map<String, Object>> objects =
		// this.getEntitiesFromResultSet(rowSet);

		return resultAsJson;
	}

	protected List<Map<String, Object>> getEntitiesFromResultSet(SqlRowSet rowSet) {
		ArrayList<Map<String, Object>> entities = new ArrayList<>();
		while (rowSet.next()) {
			entities.add(getEntityFromResultSet(rowSet));
		}
		return entities;
	}

	protected Map<String, Object> getEntityFromResultSet(SqlRowSet rowSet) {
		SqlRowSetMetaData metaData = rowSet.getMetaData();
		int columnCount = metaData.getColumnCount();
		Map<String, Object> resultsMap = new HashMap<>();
		for (int i = 1; i <= columnCount; ++i) {
			String columnName = metaData.getColumnName(i).toLowerCase();
			Object object = rowSet.getObject(i);
			resultsMap.put(columnName, object);
		}
		return resultsMap;
	}

	private Map<String, String> createColumns(List<Fields> fields) {
		StringBuffer buffer = new StringBuffer();
		List<String> indexes = new ArrayList<>();
		AtomicReference<String> primarykey = new AtomicReference<>();
		Map<String, String> map = new HashMap<>();

		fields.forEach(field -> {

			String columnName = field.getFieldName();
			String columnDescription = field.getFieldDescription();
			String columnType = field.getType();
			String columnLength = field.getLength();
			String collation = field.getCollation();
			boolean isNull = field.getNull();
			String nullField = isNull ? "NULL" : "NOT NULL";
			String index = field.getIndex().toUpperCase().trim();

			if (!index.equals("") && !index.equals(null) && !index.equals("INDEX")) {
				if (collation.equals("") || collation.equals(null)) {
					if (index.equals("UNIQUE")) {

						String partilaQuery = columnName + " " + columnType + "(" + columnLength + ")  " + nullField
								+ " " + index + " COMMENT " + "'" + columnDescription + "'";
						buffer.append(partilaQuery + " ,");

					} else if (index.equals("PRIMARY KEY")) {
						String partilaQuery = columnName + " " + columnType + "(" + columnLength + ")  " + nullField
								+ " COMMENT " + "'" + columnDescription + "' ";
						buffer.append(partilaQuery + " ,");
						primarykey.set(columnName);
					}
				} else {
					if (index.equals("UNIQUE")) {
						String partilaQuery = columnName + " " + columnType + "(" + columnLength + ") COLLATE "
								+ collation + " " + nullField + " " + index + " COMMENT " + "'" + columnDescription
								+ "'";
						buffer.append(partilaQuery + ",");
					} else if (index.equals("PRIMARY KEY")) {
						String partilaQuery = columnName + " " + columnType + "(" + columnLength + ") COLLATE "
								+ collation + " " + nullField + " COMMENT " + "'" + columnDescription + "'";
						buffer.append(partilaQuery + ",");
					}
				}

			} else {
				if (collation.equals("") || collation.equals(null)) {

					String partilaQuery = columnName + " " + columnType + "(" + columnLength + ")  " + nullField
							+ " COMMENT " + "'" + columnDescription + "'";
					buffer.append(partilaQuery + ",");
				} else {
					String partilaQuery = columnName + " " + columnType + "(" + columnLength + ")  COLLATE '"
							+ collation + "'  " + nullField + " COMMENT " + "'" + columnDescription + "'";
					buffer.append(partilaQuery + ",");
				}
				indexes.add(columnName);
			}

		});
		Gson gson = new Gson();
		if (primarykey!=null) {
			buffer.append("PRIMARY KEY (" + primarykey.get().toString() + ")" + ",");
		}
		String indexJson = gson.toJson(indexes);
		buffer.setLength(buffer.length() - 1);
		map.put("columnQuery", buffer.toString());
		map.put("indexes", indexJson);
		return map;

	}

}
