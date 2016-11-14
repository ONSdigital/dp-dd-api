package models;

import java.math.BigDecimal;

public class DataDTO
{
	private Long geographicAreaId;
	private String extCode;
	private String areaName;
	private String geographicLevelType;
	private Long timeId;
	private String timeName;
	private Long variableId;
	private String valueDomain;
	private String unitType;
	private String variableName;
	private BigDecimal value;



	public DataDTO(Long geographicAreaId, String extCode, String areaName, 
			String geographicLevelType, Long timeId, 
			String timeName,  Long variableId, String valueDomain,
			String unitType, String variableName, BigDecimal value)
	{
		super();
		this.geographicAreaId = geographicAreaId;
		this.extCode = extCode;
		this.areaName = areaName;
		this.geographicLevelType = geographicLevelType;
		this.timeId = timeId;
		this.timeName = timeName;
		this.variableId = variableId;
		this.valueDomain = valueDomain;
		this.unitType = unitType;
		this.variableName = variableName;
		this.value = value;
	}

	public String getValueDomain()
	{
		return valueDomain;
	}

	public void setValueDomain(String valueDomain)
	{
		this.valueDomain = valueDomain;
	}

	public String getUnitType()
	{
		return unitType;
	}

	public void setUnitType(String unitType)
	{
		this.unitType = unitType;
	}

	public Long getGeographicAreaId()
	{
		return geographicAreaId;
	}

	public void setGeographicAreaId(Long geographicAreaId)
	{
		this.geographicAreaId = geographicAreaId;
	}

	public String getExtCode()
	{
		return extCode;
	}

	public void setExtCode(String extCode)
	{
		this.extCode = extCode;
	}

	public String getAreaName() {
		return areaName;
	}

	public void setAreaName(String areaName) {
		this.areaName = areaName;
	}
	
	public String getGeographicLevelType()
	{
		return geographicLevelType;
	}

	public void setGeographicLevelType(String geographicLevelType)
	{
		this.geographicLevelType = geographicLevelType;
	}

	public Long getTimeId() {
		return timeId;
	}

	public void setTimeId(Long timeId) {
		this.timeId = timeId;
	}

	public String getTimeName() {
		return timeName;
	}

	public void setTimeName(String timeName) {
		this.timeName = timeName;
	}

	public Long getVariableId()
	{
		return variableId;
	}

	public void setVariableId(Long variableId)
	{
		this.variableId = variableId;
	}

	public String getVariableName()
	{
		return variableName;
	}

	public void setVariableName(String variableName)
	{
		this.variableName = variableName;
	}

	public BigDecimal getValue()
	{
		return value;
	}

	public void setValue(BigDecimal value)
	{
		this.value = value;
	}
}