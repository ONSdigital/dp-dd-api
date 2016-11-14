package models;

import java.io.Serializable;
import javax.persistence.*;

/**
 * The primary key class for the population database table.
 * 
 */
@Embeddable
public class PopulationPK implements Serializable {
	//default serial version id, required for serializable classes.
	private static final long serialVersionUID = 1L;

	@Column(name="geographic_area_id", insertable=false, updatable=false)
	private Long geographicAreaId;

	@Column(name="time_period_id", insertable=false, updatable=false)
	private Long timePeriodId;

	public PopulationPK() {
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

	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof PopulationPK)) {
			return false;
		}
		PopulationPK castOther = (PopulationPK)other;
		return 
			this.geographicAreaId.equals(castOther.geographicAreaId)
			&& this.timePeriodId.equals(castOther.timePeriodId);
	}

	public int hashCode() {
		final int prime = 31;
		int hash = 17;
		hash = hash * prime + this.geographicAreaId.hashCode();
		hash = hash * prime + this.timePeriodId.hashCode();
		
		return hash;
	}
}