package models;

import java.io.Serializable;
import javax.persistence.*;
import java.util.List;


/**
 * The persistent class for the observation_type database table.
 * 
 */
@Entity
@Table(name="observation_type")
@NamedQuery(name="ObservationType.findAll", query="SELECT o FROM ObservationType o")
public class ObservationType implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="observation_type")
	private String observationType;

	private String name;

	//bi-directional many-to-one association to DimensionalDataPoint
	@OneToMany(mappedBy="observationTypeBean")
	private List<DimensionalDataPoint> dimensionalDataPoints;

	public ObservationType() {
	}

	public String getObservationType() {
		return this.observationType;
	}

	public void setObservationType(String observationType) {
		this.observationType = observationType;
	}

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<DimensionalDataPoint> getDimensionalDataPoints() {
		return this.dimensionalDataPoints;
	}

	public void setDimensionalDataPoints(List<DimensionalDataPoint> dimensionalDataPoints) {
		this.dimensionalDataPoints = dimensionalDataPoints;
	}

	public DimensionalDataPoint addDimensionalDataPoint(DimensionalDataPoint dimensionalDataPoint) {
		getDimensionalDataPoints().add(dimensionalDataPoint);
		dimensionalDataPoint.setObservationTypeBean(this);

		return dimensionalDataPoint;
	}

	public DimensionalDataPoint removeDimensionalDataPoint(DimensionalDataPoint dimensionalDataPoint) {
		getDimensionalDataPoints().remove(dimensionalDataPoint);
		dimensionalDataPoint.setObservationTypeBean(null);

		return dimensionalDataPoint;
	}

}