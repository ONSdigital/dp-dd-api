package models;

import java.io.Serializable;
import javax.persistence.*;
import java.util.List;


/**
 * The persistent class for the presentation_type database table.
 * 
 */
@Entity
@Table(name="presentation_type")
@NamedQuery(name="PresentationType.findAll", query="SELECT p FROM PresentationType p")
public class PresentationType implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="presentation_type")
	private String presentationType;

	//bi-directional many-to-one association to Presentation
	@OneToMany(mappedBy="presentationTypeBean")
	private List<Presentation> presentations;

	public PresentationType() {
	}

	public PresentationType(String presentationType) {
		super();
		this.setPresentationType(presentationType);
	}

	public String getPresentationType() {
		return this.presentationType;
	}

	public void setPresentationType(String presentationType) {
		this.presentationType = presentationType;
	}

	public List<Presentation> getPresentations() {
		return this.presentations;
	}

	public void setPresentations(List<Presentation> presentations) {
		this.presentations = presentations;
	}

	public Presentation addPresentation(Presentation presentation) {
		getPresentations().add(presentation);
		presentation.setPresentationTypeBean(this);

		return presentation;
	}

	public Presentation removePresentation(Presentation presentation) {
		getPresentations().remove(presentation);
		presentation.setPresentationTypeBean(null);

		return presentation;
	}

}