package com.biz.model;

public class Table {
	
	private TableDetails tableDetails;
	public TableDetails getTableDetails() {
		return tableDetails;
	}
	public void setTableDetails(TableDetails tableDetails) {
		this.tableDetails = tableDetails;
	}
	public TableFieldsInfo getTableFieldsInfo() {
		return tableFieldsInfo;
	}
	public void setTableFieldsInfo(TableFieldsInfo tableFieldsInfo) {
		this.tableFieldsInfo = tableFieldsInfo;
	}
	private TableFieldsInfo tableFieldsInfo;

	public Table(TableDetails tableDetails, TableFieldsInfo tableFieldsInfo) {
		super();
		this.tableDetails = tableDetails;
		this.tableFieldsInfo = tableFieldsInfo;
	}
	public Table() {
		// TODO Auto-generated constructor stub
	}
	
}
