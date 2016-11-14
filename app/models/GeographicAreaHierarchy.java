package models;

import java.io.Serializable;
import javax.persistence.*;
import java.util.List;


/**
 * The persistent class for the geographic_area_hierarchy database table.
 * 
 */
@Entity
@Table(name="geographic_area_hierarchy")
@NamedQuery(name="GeographicAreaHierarchy.findAll", query="SELECT g FROM GeographicAreaHierarchy g")
public class GeographicAreaHierarchy implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="geographic_area_hierarchy")
	private String geographicAreaHierarchy;

	//bi-directional many-to-one association to GeographicArea
	@OneToMany(mappedBy="geographicAreaHierarchyBean")
	private List<GeographicArea> geographicAreas;

	public GeographicAreaHierarchy() {
	}

	public String getGeographicAreaHierarchy() {
		return this.geographicAreaHierarchy;
	}

	public void setGeographicAreaHierarchy(String geographicAreaHierarchy) {
		this.geographicAreaHierarchy = geographicAreaHierarchy;
	}

	public List<GeographicArea> getGeographicAreas() {
		return this.geographicAreas;
	}

	public void setGeographicAreas(List<GeographicArea> geographicAreas) {
		this.geographicAreas = geographicAreas;
	}

	public GeographicArea addGeographicArea(GeographicArea geographicArea) {
		getGeographicAreas().add(geographicArea);
		geographicArea.setGeographicAreaHierarchyBean(this);

		return geographicArea;
	}

	public GeographicArea removeGeographicArea(GeographicArea geographicArea) {
		getGeographicAreas().remove(geographicArea);
		geographicArea.setGeographicAreaHierarchyBean(null);

		return geographicArea;
	}

}