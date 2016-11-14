package models;

import java.io.Serializable;
import javax.persistence.*;
import java.util.List;


/**
 * The persistent class for the subject_field database table.
 * 
 */
@Entity
@Table(name="subject_field")
@NamedQuery(name="SubjectField.findAll", query="SELECT s FROM SubjectField s")
public class SubjectField implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="subject_field")
	private String subjectField;

	private String metadata;

	//bi-directional many-to-one association to SubjectField
	@ManyToOne
	@JoinColumn(name="rel_subject_field")
	private SubjectField subjectFieldBean;

	//bi-directional many-to-one association to SubjectField
	@OneToMany(mappedBy="subjectFieldBean")
	private List<SubjectField> subjectFields;

	//bi-directional many-to-many association to ConceptSystem
	@ManyToMany
	@JoinTable(
		name="subject_field_concept_system"
		, joinColumns={
			@JoinColumn(name="subject_field")
			}
		, inverseJoinColumns={
			@JoinColumn(name="concept_system")
			}
		)
	private List<ConceptSystem> conceptSystems;

	public SubjectField() {
	}

	public String getSubjectField() {
		return this.subjectField;
	}

	public void setSubjectField(String subjectField) {
		this.subjectField = subjectField;
	}

	public String getMetadata() {
		return this.metadata;
	}

	public void setMetadata(String metadata) {
		this.metadata = metadata;
	}

	public SubjectField getSubjectFieldBean() {
		return this.subjectFieldBean;
	}

	public void setSubjectFieldBean(SubjectField subjectFieldBean) {
		this.subjectFieldBean = subjectFieldBean;
	}

	public List<SubjectField> getSubjectFields() {
		return this.subjectFields;
	}

	public void setSubjectFields(List<SubjectField> subjectFields) {
		this.subjectFields = subjectFields;
	}

	public SubjectField addSubjectField(SubjectField subjectField) {
		getSubjectFields().add(subjectField);
		subjectField.setSubjectFieldBean(this);

		return subjectField;
	}

	public SubjectField removeSubjectField(SubjectField subjectField) {
		getSubjectFields().remove(subjectField);
		subjectField.setSubjectFieldBean(null);

		return subjectField;
	}

	public List<ConceptSystem> getConceptSystems() {
		return this.conceptSystems;
	}

	public void setConceptSystems(List<ConceptSystem> conceptSystems) {
		this.conceptSystems = conceptSystems;
	}

}