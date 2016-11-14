package models;

import java.io.Serializable;
import javax.persistence.*;
import java.util.List;


/**
 * The persistent class for the taxonomy database table.
 * 
 */
@Entity
@NamedQuery(name="Taxonomy.findAll", query="SELECT t FROM Taxonomy t")
public class Taxonomy implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	private String taxonomy;

	private String metadata;

	//bi-directional many-to-many association to DataResource
	@ManyToMany(mappedBy="taxonomies")
	private List<DataResource> dataResources;

	//bi-directional many-to-one association to Taxonomy
	@ManyToOne
	@JoinColumn(name="rel_taxonomy")
	private Taxonomy taxonomyBean;

	//bi-directional many-to-one association to Taxonomy
	@OneToMany(mappedBy="taxonomyBean")
	private List<Taxonomy> taxonomies;

	public Taxonomy() {
	}

	public String getTaxonomy() {
		return this.taxonomy;
	}

	public void setTaxonomy(String taxonomy) {
		this.taxonomy = taxonomy;
	}

	public String getMetadata() {
		return this.metadata;
	}

	public void setMetadata(String metadata) {
		this.metadata = metadata;
	}

	public List<DataResource> getDataResources() {
		return this.dataResources;
	}

	public void setDataResources(List<DataResource> dataResources) {
		this.dataResources = dataResources;
	}

	public Taxonomy getTaxonomyBean() {
		return this.taxonomyBean;
	}

	public void setTaxonomyBean(Taxonomy taxonomyBean) {
		this.taxonomyBean = taxonomyBean;
	}

	public List<Taxonomy> getTaxonomies() {
		return this.taxonomies;
	}

	public void setTaxonomies(List<Taxonomy> taxonomies) {
		this.taxonomies = taxonomies;
	}

	public Taxonomy addTaxonomy(Taxonomy taxonomy) {
		getTaxonomies().add(taxonomy);
		taxonomy.setTaxonomyBean(this);

		return taxonomy;
	}

	public Taxonomy removeTaxonomy(Taxonomy taxonomy) {
		getTaxonomies().remove(taxonomy);
		taxonomy.setTaxonomyBean(null);

		return taxonomy;
	}

}