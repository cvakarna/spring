package com.biz;

public class TableData {
	private String TableName;
	public TableData(String tableName, String tableDescription, String createdOn, String createdBy, String changedOn,
			String changedBy) {
		super();
		TableName = tableName;
		TableDescription = tableDescription;
		CreatedOn = createdOn;
		CreatedBy = createdBy;
		ChangedOn = changedOn;
		ChangedBy = changedBy;
	}
	private String TableDescription;
	private String CreatedOn;
	private String CreatedBy;
	private String ChangedOn;
	private String ChangedBy;
	
	
	private String getTableName() {
		return TableName;
	}
	public void setTableName(String tableName) {
		TableName = tableName;
	}
	public String getTableDescription() {
		return TableDescription;
	}
	public void setTableDescription(String tableDescription) {
		TableDescription = tableDescription;
	}
	public String getCreatedOn() {
		return CreatedOn;
	}
	public void setCreatedOn(String createdOn) {
		CreatedOn = createdOn;
	}
	public String getCreatedBy() {
		return CreatedBy;
	}
	public void setCreatedBy(String createdBy) {
		CreatedBy = createdBy;
	}
	public String getChangedOn() {
		return ChangedOn;
	}
	public void setChangedOn(String changedOn) {
		ChangedOn = changedOn;
	}
	public String getChangedBy() {
		return ChangedBy;
	}
	public void setChangedBy(String changedBy) {
		ChangedBy = changedBy;
	}
	
}
