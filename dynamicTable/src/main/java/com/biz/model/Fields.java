package com.biz.model;

public class Fields {
	
	public Fields(String fieldName, String type, String length, String collation, boolean null1, String index,
			String fieldDescription) {
		super();
		FieldName = fieldName;
		Type = type;
		Length = length;
		Collation = collation;
		Null = null1;
		Index = index;
		FieldDescription = fieldDescription;
	}

	private String FieldName;
	public String getFieldName() {
		return FieldName;
	}

	public void setFieldName(String fieldName) {
		FieldName = fieldName;
	}

	public String getType() {
		return Type;
	}

	public void setType(String type) {
		Type = type;
	}

	public String getLength() {
		return Length;
	}

	public void setLength(String length) {
		Length = length;
	}

	public String getCollation() {
		return Collation;
	}

	public void setCollation(String collation) {
		Collation = collation;
	}

	public boolean getNull() {
		return Null;
	}

	public void setNull(boolean null1) {
		Null = null1;
	}

	public String getIndex() {
		return Index;
	}

	public void setIndex(String index) {
		Index = index;
	}

	public String getFieldDescription() {
		return FieldDescription;
	}

	public void setFieldDescription(String fieldDescription) {
		FieldDescription = fieldDescription;
	}

	private String Type;
	private String Length;
	private String Collation;
	private boolean Null;
	private String Index;
	private String FieldDescription;
	
	public Fields() {
		// TODO Auto-generated constructor stub
	}
	
	
	

}
