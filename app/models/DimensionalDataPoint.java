package models;

import java.io.Serializable;
import javax.persistence.*;
import java.math.BigDecimal;


/**
 * The persistent class for the dimensional_data_point database table.
 * 
 */
@Entity
@Table(name="dimensional_data_point")
@NamedQuery(name="DimensionalDataPoint.findAll", query="SELECT d FROM DimensionalDataPoint d")
public class DimensionalDataPoint implements Serializable {
	private static final long serialVersionUID = 1L;

	@EmbeddedId
	private DimensionalDataPointPK id;

	@Column(name="data_marking")
	private String dataMarking;

	@Column(name="observation_type_value")
	private String observationTypeValue;

	private BigDecimal value;

	//bi-directional many-to-one association to DimensionalDataSet
	@ManyToOne
	@JoinColumn(name="dimensional_data_set_id")
	private DimensionalDataSet dimensionalDataSet;

	//bi-directional many-to-one association to ObservationType
	@ManyToOne
	@JoinColumn(name="observation_type")
	private ObservationType observationTypeBean;

	//bi-directional many-to-one association to Population
	@ManyToOne
	@JoinColumns({
		@JoinColumn(name="geographic_area_id", referencedColumnName="geographic_area_id"),
		@JoinColumn(name="time_period_id", referencedColumnName="time_period_id")
		})
	private Population population;

	//bi-directional many-to-one association to Variable
	@ManyToOne
	@JoinColumn(name="variable_id")
	private Variable variable;

	public DimensionalDataPoint() {
	}

	public DimensionalDataPointPK getId() {
		return this.id;
	}

	public void setId(DimensionalDataPointPK id) {
		this.id = id;
	}

	public String getDataMarking() {
		return this.dataMarking;
	}

	public void setDataMarking(String dataMarking) {
		this.dataMarking = dataMarking;
	}

	public String getObservationTypeValue() {
		return this.observationTypeValue;
	}

	public void setObservationTypeValue(String observationTypeValue) {
		this.observationTypeValue = observationTypeValue;
	}

	public BigDecimal getValue() {
		return this.value;
	}

	public void setValue(BigDecimal value) {
		this.value = value;
	}

	public DimensionalDataSet getDimensionalDataSet() {
		return this.dimensionalDataSet;
	}

	public void setDimensionalDataSet(DimensionalDataSet dimensionalDataSet) {
		this.dimensionalDataSet = dimensionalDataSet;
	}

	public ObservationType getObservationTypeBean() {
		return this.observationTypeBean;
	}

	public void setObservationTypeBean(ObservationType observationTypeBean) {
		this.observationTypeBean = observationTypeBean;
	}

	public Population getPopulation() {
		return this.population;
	}

	public void setPopulation(Population population) {
		this.population = population;
	}

	public Variable getVariable() {
		return this.variable;
	}

	public void setVariable(Variable variable) {
		this.variable = variable;
	}

}