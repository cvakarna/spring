package com.biz.model;

public class TableDetails {
 
	private String tableName;
	public String getTableName() {
		return tableName;
	}



	public void setTableName(String tableName) {
		this.tableName = tableName;
	}



	public String getTableDescription() {
		return tableDescription;
	}



	public void setTableDescription(String tableDescription) {
		this.tableDescription = tableDescription;
	}



	private String tableDescription;
	public TableDetails(String tableName, String tableDescription) {
		super();
		this.tableName = tableName;
		this.tableDescription = tableDescription;
	}

	
	
	public TableDetails() {
		// TODO Auto-generated constructor stub
	}
	
}
