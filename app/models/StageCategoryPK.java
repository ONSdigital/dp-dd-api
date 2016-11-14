package models;

import java.io.Serializable;
import javax.persistence.*;

/**
 * The primary key class for the stage_category database table.
 * 
 */
@Embeddable
public class StageCategoryPK implements Serializable {
	//default serial version id, required for serializable classes.
	private static final long serialVersionUID = 1L;
	
 //   @Lob
//	@Column(name="observation_seq_id", insertable=false, updatable=false)
	@Column(name="observation_seq_id")
	private Long observationSeqId;

	@Column(name="dimension_number")
	private Integer dimensionNumber;

	public StageCategoryPK() {
	}
	public Long getObservationSeqId() {
		return this.observationSeqId;
	}
	public void setObservationSeqId(Long observationSeqId) {
		this.observationSeqId = observationSeqId;
	}
	public Integer getDimensionNumber() {
		return this.dimensionNumber;
	}
	public void setDimensionNumber(Integer dimensionNumber) {
		this.dimensionNumber = dimensionNumber;
	}

	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof StageCategoryPK)) {
			return false;
		}
		StageCategoryPK castOther = (StageCategoryPK)other;
		return 
			this.observationSeqId.equals(castOther.observationSeqId)
			&& this.dimensionNumber.equals(castOther.dimensionNumber);
	}

	public int hashCode() {
		final int prime = 31;
		int hash = 17;
		hash = hash * prime + this.observationSeqId.hashCode();
		hash = hash * prime + this.dimensionNumber.hashCode();
		
		return hash;
	}
}