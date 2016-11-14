package models;

import java.io.Serializable;
import javax.persistence.*;
import java.util.List;


/**
 * The persistent class for the population database table.
 * 
 */
@Entity
@NamedQuery(name="Population.findAll", query="SELECT p FROM Population p")
public class Population implements Serializable {
	private static final long serialVersionUID = 1L;

	@EmbeddedId
	private PopulationPK id;

	@Column(name="geographic_area_ext_code")
	private String geographicAreaExtCode;

	//bi-directional many-to-one association to DimensionalDataPoint
	@OneToMany(mappedBy="population")
	private List<DimensionalDataPoint> dimensionalDataPoints;

	//bi-directional many-to-one association to GeographicArea
	@ManyToOne
	@JoinColumn(name="geographic_area_id")
	private GeographicArea geographicArea;

	//bi-directional many-to-one association to TimePeriod
	@ManyToOne
	@JoinColumn(name="time_period_id")
	private TimePeriod timePeriod;

	public Population() {
	}

	public PopulationPK getId() {
		return this.id;
	}

	public void setId(PopulationPK id) {
		this.id = id;
	}

	public String getGeographicAreaExtCode() {
		return this.geographicAreaExtCode;
	}

	public void setGeographicAreaExtCode(String geographicAreaExtCode) {
		this.geographicAreaExtCode = geographicAreaExtCode;
	}

	public List<DimensionalDataPoint> getDimensionalDataPoints() {
		return this.dimensionalDataPoints;
	}

	public void setDimensionalDataPoints(List<DimensionalDataPoint> dimensionalDataPoints) {
		this.dimensionalDataPoints = dimensionalDataPoints;
	}

	public DimensionalDataPoint addDimensionalDataPoint(DimensionalDataPoint dimensionalDataPoint) {
		getDimensionalDataPoints().add(dimensionalDataPoint);
		dimensionalDataPoint.setPopulation(this);

		return dimensionalDataPoint;
	}

	public DimensionalDataPoint removeDimensionalDataPoint(DimensionalDataPoint dimensionalDataPoint) {
		getDimensionalDataPoints().remove(dimensionalDataPoint);
		dimensionalDataPoint.setPopulation(null);

		return dimensionalDataPoint;
	}

	public GeographicArea getGeographicArea() {
		return this.geographicArea;
	}

	public void setGeographicArea(GeographicArea geographicArea) {
		this.geographicArea = geographicArea;
	}

	public TimePeriod getTimePeriod() {
		return this.timePeriod;
	}

	public void setTimePeriod(TimePeriod timePeriod) {
		this.timePeriod = timePeriod;
	}

}