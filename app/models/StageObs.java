package models;

import java.math.BigDecimal;

public class StageObs {
	private BigDecimal value;
	private String status;
	private String obsType;
	private String obsTypeValue;
	private String geogCode;
	private String timeCode;
	
	public String getGeogCode() {
		return geogCode;
	}
	public String getTimeCode() {
		return timeCode;
	}

	public void setTimeCode(String timeCode) {
		this.timeCode = timeCode;
	}

	public String getObsTypeValue() {
		return obsTypeValue;
	}
	
	public void setObsTypeValue(String obsTypeValue) {
		this.obsTypeValue = obsTypeValue;
	}
	
	public void setGeogCode(String geogCode) {
		this.geogCode = geogCode;
	}
	public BigDecimal getValue() {
		return value;
	}
	public void setValue(BigDecimal value) {
		this.value = value;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getObsType() {
		return obsType;
	}
	public void setObsType(String obsType) {
		this.obsType = obsType;
	}

  
}
