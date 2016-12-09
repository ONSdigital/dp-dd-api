package models;

import java.io.Serializable;
import java.util.UUID;
import javax.persistence.*;

/**
 * The primary key class for the dimensional_data_point database table.
 * 
 */
@Embeddable
public class DimensionalDataPointPK implements Serializable {
	//default serial version id, required for serializable classes.
	private static final long serialVersionUID = 1L;

	@Column(name="dimensional_data_set_id", insertable=false, updatable=false)
	private UUID dimensionalDataSetId;

	@Column(name="geographic_area_id", insertable=false, updatable=false)
	private Long geographicAreaId;

	@Column(name="time_period_id", insertable=false, updatable=false)
	private Long timePeriodId;

	@Column(name="variable_id", insertable=false, updatable=false)
	private Long variableId;

	public DimensionalDataPointPK() {
	}
	public UUID getDimensionalDataSetId() {
		return this.dimensionalDataSetId;
	}
	public void setDimensionalDataSetId(UUID dimensionalDataSetId) {
		this.dimensionalDataSetId = dimensionalDataSetId;
	}
	public Long getGeographicAreaId() {
		return this.geographicAreaId;
	}
	public void setGeographicAreaId(Long geographicAreaId) {
		this.geographicAreaId = geographicAreaId;
	}
	public Long getTimePeriodId() {
		return this.timePeriodId;
	}
	public void setTimePeriodId(Long timePeriodId) {
		this.timePeriodId = timePeriodId;
	}
	public Long getVariableId() {
		return this.variableId;
	}
	public void setVariableId(Long variableId) {
		this.variableId = variableId;
	}

	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof DimensionalDataPointPK)) {
			return false;
		}
		DimensionalDataPointPK castOther = (DimensionalDataPointPK)other;
		return 
			this.dimensionalDataSetId.equals(castOther.dimensionalDataSetId)
			&& this.geographicAreaId.equals(castOther.geographicAreaId)
			&& this.timePeriodId.equals(castOther.timePeriodId)
			&& this.variableId.equals(castOther.variableId);
	}

	public int hashCode() {
		final int prime = 31;
		int hash = 17;
		hash = hash * prime + this.dimensionalDataSetId.hashCode();
		hash = hash * prime + this.geographicAreaId.hashCode();
		hash = hash * prime + this.timePeriodId.hashCode();
		hash = hash * prime + this.variableId.hashCode();
		
		return hash;
	}
}