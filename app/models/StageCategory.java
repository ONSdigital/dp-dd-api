package models;

import java.io.Serializable;
import javax.persistence.*;


/**
 * The persistent class for the stage_category database table.
 * 
 */
@Entity
@Table(name="stage_category")
@NamedQuery(name="StageCategory.findAll", query="SELECT s FROM StageCategory s")
public class StageCategory implements Serializable {
	private static final long serialVersionUID = 1L;

	@EmbeddedId
	private StageCategoryPK id;

//	@Column(name="observation_seq_id", insertable=false, updatable=false)
//	private Long observationSeqId;

//	@Column(name="dimension_number", insertable=false, updatable=false)
//	private Integer dimensionNumber;
	
	@Column(name="category_id")
	private String categoryId;

	@Column(name="category_name_cym")
	private String categoryNameCym;

	@Column(name="category_name_eng")
	private String categoryNameEng;

	@Column(name="concept_system_id")
	private String conceptSystemId;

	@Column(name="concept_system_label_cym")
	private String conceptSystemLabelCym;

	@Column(name="concept_system_label_eng")
	private String conceptSystemLabelEng;

	@Column(name="is_sub_total")
	private String isSubTotal;

	@Column(name="is_total")
	private String isTotal;

	//bi-directional many-to-one association to StageDimensionalDataPoint
	@ManyToOne
	@JoinColumn(name="observation_seq_id", insertable=false, updatable=false)
	//@JoinColumn(name="observation_seq_id")
	private StageDimensionalDataPoint stageDimensionalDataPoint;

	public StageCategory() {
	}

	public StageCategoryPK getId() {
		return this.id;
	}

	public void setId(StageCategoryPK id) {
		this.id = id;
	}

	public String getCategoryId() {
		return this.categoryId;
	}

	public void setCategoryId(String categoryId) {
		this.categoryId = categoryId;
	}

	public String getCategoryNameCym() {
		return this.categoryNameCym;
	}

	public void setCategoryNameCym(String categoryNameCym) {
		this.categoryNameCym = categoryNameCym;
	}

	public String getCategoryNameEng() {
		return this.categoryNameEng;
	}

	public void setCategoryNameEng(String categoryNameEng) {
		this.categoryNameEng = categoryNameEng;
	}

	public String getConceptSystemId() {
		return this.conceptSystemId;
	}

	public void setConceptSystemId(String conceptSystemId) {
		this.conceptSystemId = conceptSystemId;
	}

	public String getConceptSystemLabelCym() {
		return this.conceptSystemLabelCym;
	}

	public void setConceptSystemLabelCym(String conceptSystemLabelCym) {
		this.conceptSystemLabelCym = conceptSystemLabelCym;
	}

	public String getConceptSystemLabelEng() {
		return this.conceptSystemLabelEng;
	}

	public void setConceptSystemLabelEng(String conceptSystemLabelEng) {
		this.conceptSystemLabelEng = conceptSystemLabelEng;
	}

	public String getIsSubTotal() {
		return this.isSubTotal;
	}

	public void setIsSubTotal(String isSubTotal) {
		this.isSubTotal = isSubTotal;
	}

	public String getIsTotal() {
		return this.isTotal;
	}

	public void setIsTotal(String isTotal) {
		this.isTotal = isTotal;
	}

	public StageDimensionalDataPoint getStageDimensionalDataPoint() {
		return this.stageDimensionalDataPoint;
	}

	public void setStageDimensionalDataPoint(StageDimensionalDataPoint stageDimensionalDataPoint) {
		this.stageDimensionalDataPoint = stageDimensionalDataPoint;
	}

}