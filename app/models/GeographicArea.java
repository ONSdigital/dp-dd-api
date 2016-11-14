package models;

import java.io.Serializable;
import java.math.BigDecimal;

import javax.persistence.*;
import java.util.List;


/**
 * The persistent class for the geographic_area database table.
 * 
 */
@Entity
@Table(name="geographic_area")
@NamedQuery(name="GeographicArea.findAll", query="SELECT g FROM GeographicArea g")
@SqlResultSetMapping(name = "DataTableResult", classes = {
		@ConstructorResult(targetClass = DataDTO.class,	columns = {
				@ColumnResult(name = "geographic_area_id", type=Long.class),
				@ColumnResult(name = "ext_code"), 
				@ColumnResult(name = "area_name"),
				@ColumnResult(name = "geographic_level_type"),
				@ColumnResult(name = "time_period_id", type=Long.class),
				@ColumnResult(name = "time_name"),			
				@ColumnResult(name = "variable_id", type=Long.class),
				@ColumnResult(name = "value_domain"),
				@ColumnResult(name = "unit_type"),
				@ColumnResult(name = "variable_name"),
				@ColumnResult(name = "value", type=BigDecimal.class) }) })
public class GeographicArea implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
    @SequenceGenerator( name = "areaseq", sequenceName = "areaseq", allocationSize = 1, initialValue = 1 )
    @GeneratedValue( strategy = GenerationType.SEQUENCE, generator = "areaseq" )
	@Column(name="geographic_area_id")
	private Long geographicAreaId;

	@Column(name="ext_code")
	private String extCode;

	private String metadata;

	private String name;

	//bi-directional many-to-one association to GeographicArea
	@ManyToOne
	@JoinColumn(name="rel_geographic_area_id")
	private GeographicArea geographicArea;

	//bi-directional many-to-one association to GeographicArea
	@OneToMany(mappedBy="geographicArea")
	private List<GeographicArea> geographicAreas;

	//bi-directional many-to-one association to GeographicAreaHierarchy
	@ManyToOne
	@JoinColumn(name="geographic_area_hierarchy")
	private GeographicAreaHierarchy geographicAreaHierarchyBean;

	//bi-directional many-to-one association to GeographicAreaType
	@ManyToOne
	@JoinColumn(name="geographic_area_type")
	private GeographicAreaType geographicAreaTypeBean;

	//bi-directional many-to-one association to GeographicLevelType
	@ManyToOne
	@JoinColumn(name="geographic_level_type")
	private GeographicLevelType geographicLevelTypeBean;

	//bi-directional many-to-one association to Population
	@OneToMany(mappedBy="geographicArea")
	private List<Population> populations;

	public GeographicArea() {
	}

	public Long getGeographicAreaId() {
		return this.geographicAreaId;
	}

	public void setGeographicAreaId(Long geographicAreaId) {
		this.geographicAreaId = geographicAreaId;
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

	public GeographicArea getGeographicArea() {
		return this.geographicArea;
	}

	public void setGeographicArea(GeographicArea geographicArea) {
		this.geographicArea = geographicArea;
	}

	public List<GeographicArea> getGeographicAreas() {
		return this.geographicAreas;
	}

	public void setGeographicAreas(List<GeographicArea> geographicAreas) {
		this.geographicAreas = geographicAreas;
	}

	public GeographicArea addGeographicArea(GeographicArea geographicArea) {
		getGeographicAreas().add(geographicArea);
		geographicArea.setGeographicArea(this);

		return geographicArea;
	}

	public GeographicArea removeGeographicArea(GeographicArea geographicArea) {
		getGeographicAreas().remove(geographicArea);
		geographicArea.setGeographicArea(null);

		return geographicArea;
	}

	public GeographicAreaHierarchy getGeographicAreaHierarchyBean() {
		return this.geographicAreaHierarchyBean;
	}

	public void setGeographicAreaHierarchyBean(GeographicAreaHierarchy geographicAreaHierarchyBean) {
		this.geographicAreaHierarchyBean = geographicAreaHierarchyBean;
	}

	public GeographicAreaType getGeographicAreaTypeBean() {
		return this.geographicAreaTypeBean;
	}

	public void setGeographicAreaTypeBean(GeographicAreaType geographicAreaTypeBean) {
		this.geographicAreaTypeBean = geographicAreaTypeBean;
	}

	public GeographicLevelType getGeographicLevelTypeBean() {
		return this.geographicLevelTypeBean;
	}

	public void setGeographicLevelTypeBean(GeographicLevelType geographicLevelTypeBean) {
		this.geographicLevelTypeBean = geographicLevelTypeBean;
	}

	public List<Population> getPopulations() {
		return this.populations;
	}

	public void setPopulations(List<Population> populations) {
		this.populations = populations;
	}

	public Population addPopulation(Population population) {
		getPopulations().add(population);
		population.setGeographicArea(this);

		return population;
	}

	public Population removePopulation(Population population) {
		getPopulations().remove(population);
		population.setGeographicArea(null);

		return population;
	}

}