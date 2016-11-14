package models;

import java.io.Serializable;
import javax.persistence.*;
import java.util.List;


/**
 * The persistent class for the concept_system database table.
 * 
 */
@Entity
@Table(name="concept_system")
@NamedQuery(name="ConceptSystem.findAll", query="SELECT c FROM ConceptSystem c")
public class ConceptSystem implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="concept_system")
	private String conceptSystem;

	//bi-directional many-to-one association to Category
	@OneToMany(mappedBy="conceptSystemBean")
	private List<Category> categories;

	//bi-directional many-to-many association to SubjectField
	@ManyToMany(mappedBy="conceptSystems")
	private List<SubjectField> subjectFields;

	public ConceptSystem() {
	}

	public String getConceptSystem() {
		return this.conceptSystem;
	}

	public void setConceptSystem(String conceptSystem) {
		this.conceptSystem = conceptSystem;
	}

	public List<Category> getCategories() {
		return this.categories;
	}

	public void setCategories(List<Category> categories) {
		this.categories = categories;
	}

	public Category addCategory(Category category) {
		getCategories().add(category);
		category.setConceptSystemBean(this);

		return category;
	}

	public Category removeCategory(Category category) {
		getCategories().remove(category);
		category.setConceptSystemBean(null);

		return category;
	}

	public List<SubjectField> getSubjectFields() {
		return this.subjectFields;
	}

	public void setSubjectFields(List<SubjectField> subjectFields) {
		this.subjectFields = subjectFields;
	}

}