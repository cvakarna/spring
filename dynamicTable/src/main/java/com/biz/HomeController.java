package com.biz;



import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;


@RestController
public class HomeController {
	// DB Connection
	@Autowired
	JdbcTemplate sql;

	@Autowired
	TableUtil util;

	@RequestMapping(value = "/list", method = RequestMethod.GET)
	public String getTableList() {
		String dbName = "springbootdb";
		SqlRowSet rowSet = sql
				.queryForRowSet("select * from information_schema.tables where TABLE_SCHEMA='springbootdb'");
		String resultAsJsonString = this.util.getTableDefinition(rowSet);
		return resultAsJsonString;
	}

	@RequestMapping(value = "/create", method = RequestMethod.POST,consumes="application/json")
	public String createTable(@RequestBody String tableInfo ) {
        String query = this.util.creatTable(tableInfo);
        
        sql.execute(query);
  
        
		return null;

	}

	@RequestMapping(value = "/read", method = RequestMethod.GET)
	public String readTableInfo(@RequestParam(value = "tableName") String tableName) {
		String dbName = "springbootdb";
		String columnInfoQuery = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME ='" + tableName + "' and TABLE_SCHEMA = '"+dbName+"'";
		SqlRowSet rowSet = sql.queryForRowSet(columnInfoQuery);
		SqlRowSet tableRowset = sql.queryForRowSet("SELECT TABLE_COMMENT FROM INFORMATION_SCHEMA.tables WHERE TABLE_SCHEMA='"+dbName+"' AND TABLE_NAME= '"+tableName+"'");
		String result = this.util.DisplayTableInfo(rowSet,tableRowset,tableName);
		
	 
		
		/*Gson gson = new Gson();
		return gson.toJson(this.util.getEntitiesFromResultSet(rowSet));*/
		return result;

	}

	@RequestMapping(value = "/update", method = RequestMethod.POST)
	public String updateTable(@RequestParam(value = "tableName") String tableName) {

		return null;

	}

	@RequestMapping(value = "/delete", method = RequestMethod.DELETE)
	public String deleteTable(@RequestParam(value = "tableName") String tableName) {
		
		String dbName = "springbootdb";
		String query = "drop table if exists "+tableName;
		sql.execute(query);
		
		return null;
	}

}
