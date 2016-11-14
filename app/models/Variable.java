package models;

import java.io.Serializable;
import javax.persistence.*;
import java.util.List;


/**
 * The persistent class for the variable database table.
 * 
 */
@Entity
@NamedQuery(name="Variable.findAll", query="SELECT v FROM Variable v")
public class Variable implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
    @SequenceGenerator( name = "varseq", sequenceName = "varseq", allocationSize = 1, initialValue = 1 )
    @GeneratedValue( strategy = GenerationType.SEQUENCE, generator = "varseq" )
	@Column(name="variable_id")
	private Long variableId;

	@Column(name="ext_code")
	private String extCode;

	private String metadata;

	private String name;

	//bi-directional many-to-one association to DimensionalDataPoint
	@OneToMany(mappedBy="variable")
	private List<DimensionalDataPoint> dimensionalDataPoints;

	//bi-directional many-to-one association to UnitType
	@ManyToOne
	@JoinColumn(name="unit_type")
	private UnitType unitTypeBean;

	//bi-directional many-to-one association to ValueDomain
	@ManyToOne
	@JoinColumn(name="value_domain")
	private ValueDomain valueDomainBean;

	//bi-directional many-to-many association to Category
	@ManyToMany
	@JoinTable(
		name="variable_category"
		, joinColumns={
			@JoinColumn(name="variable_id")
			}
		, inverseJoinColumns={
			@JoinColumn(name="category_id")
			}
		)
	private List<Category> categories;

	public Variable() {
	}

	public Long getVariableId() {
		return this.variableId;
	}

	public void setVariableId(Long variableId) {
		this.variableId = variableId;
	}

	public String getExtCode() {
		return this.extCode;
	}

	public void setExtCode(String extCode) {
		this.extCode = extCode;
	}

	public String getMetadata() {
		return this.metadata;
	}

	public void setMetadata(String metadata) {
		this.metadata = metadata;
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
		dimensionalDataPoint.setVariable(this);

		return dimensionalDataPoint;
	}

	public DimensionalDataPoint removeDimensionalDataPoint(DimensionalDataPoint dimensionalDataPoint) {
		getDimensionalDataPoints().remove(dimensionalDataPoint);
		dimensionalDataPoint.setVariable(null);

		return dimensionalDataPoint;
	}

	public UnitType getUnitTypeBean() {
		return this.unitTypeBean;
	}

	public void setUnitTypeBean(UnitType unitTypeBean) {
		this.unitTypeBean = unitTypeBean;
	}

	public ValueDomain getValueDomainBean() {
		return this.valueDomainBean;
	}

	public void setValueDomainBean(ValueDomain valueDomainBean) {
		this.valueDomainBean = valueDomainBean;
	}

	public List<Category> getCategories() {
		return this.categories;
	}

	public void setCategories(List<Category> categories) {
		this.categories = categories;
	}

}