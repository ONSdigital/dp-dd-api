package models;

import java.io.Serializable;
import javax.persistence.*;
import java.math.BigDecimal;
import java.util.List;


/**
 * The persistent class for the stage_dimensional_data_point database table.
 * 
 */
@Entity
@Table(name="stage_dimensional_data_point")
@NamedQuery(name="StageDimensionalDataPoint.findAll", query="SELECT s FROM StageDimensionalDataPoint s")
public class StageDimensionalDataPoint implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
//	@GeneratedValue(strategy=GenerationType.IDENTITY)
    @SequenceGenerator( name = "obsseq", sequenceName = "obsseq", allocationSize = 1, initialValue = 1 )
    @GeneratedValue( strategy = GenerationType.SEQUENCE, generator = "obsseq" )
 	@Column(name="observation_seq_id")
	private Long observationSeqId;

	private String cdid;

	private String cdiddescrip;

	private String confidentiality;

	@Column(name="data_marking")
	private String dataMarking;

	@Column(name="dimensional_data_set_id")
	private Long dimensionalDataSetId;

	@Column(name="geographic_area")
	private String geographicArea;

	@Column(name="observation_type")
	private String observationType;

	@Column(name="observation_type_value")
	private String observationTypeValue;

	@Column(name="statistical_population_id")
	private Long statisticalPopulationId;

	@Column(name="statistical_population_label_cym")
	private String statisticalPopulationLabelCym;

	@Column(name="statistical_population_label_eng")
	private String statisticalPopulationLabelEng;

	@Column(name="time_period_id")
	private Long timePeriodId;

	@Column(name="time_period_name_cym")
	private String timePeriodNameCym;

	@Column(name="time_period_name_eng")
	private String timePeriodNameEng;

	@Column(name="time_type")
	private String timeType;

	@Column(name="unit_multiplier")
	private String unitMultiplier;

	@Column(name="unit_type_cym")
	private String unitTypeCym;

	@Column(name="unit_type_eng")
	private String unitTypeEng;

	private BigDecimal value;

	@Column(name="value_domain_cym")
	private String valueDomainCym;

	@Column(name="value_domain_eng")
	private String valueDomainEng;

	@Column(name="time_period_code")
	private String timePeriodCode;
	
	//bi-directional many-to-one association to StageCategory
	@OneToMany(mappedBy="stageDimensionalDataPoint")
	private List<StageCategory> stageCategories;

	public StageDimensionalDataPoint() {
	}

	public Long getObservationSeqId() {
		return this.observationSeqId;
	}

	public void setObservationSeqId(Long observationSeqId) {
		this.observationSeqId = observationSeqId;
	}

	public String getCdid() {
		return this.cdid;
	}

	public void setCdid(String cdid) {
		this.cdid = cdid;
	}

	public String getCdiddescrip() {
		return this.cdiddescrip;
	}

	public void setCdiddescrip(String cdiddescrip) {
		this.cdiddescrip = cdiddescrip;
	}

	public String getConfidentiality() {
		return this.confidentiality;
	}

	public void setConfidentiality(String confidentiality) {
		this.confidentiality = confidentiality;
	}

	public String getDataMarking() {
		return this.dataMarking;
	}

	public void setDataMarking(String dataMarking) {
		this.dataMarking = dataMarking;
	}

	public Long getDimensionalDataSetId() {
		return this.dimensionalDataSetId;
	}

	public void setDimensionalDataSetId(Long dimensionalDataSetId) {
		this.dimensionalDataSetId = dimensionalDataSetId;
	}

	public String getGeographicArea() {
		return this.geographicArea;
	}

	public void setGeographicArea(String geographicArea) {
		this.geographicArea = geographicArea;
	}

	public String getObservationType() {
		return this.observationType;
	}

	public void setObservationType(String observationType) {
		this.observationType = observationType;
	}

	public String getObservationTypeValue() {
		return this.observationTypeValue;
	}

	public void setObservationTypeValue(String observationTypeValue) {
		this.observationTypeValue = observationTypeValue;
	}

	public Long getStatisticalPopulationId() {
		return this.statisticalPopulationId;
	}

	public void setStatisticalPopulationId(Long statisticalPopulationId) {
		this.statisticalPopulationId = statisticalPopulationId;
	}

	public String getStatisticalPopulationLabelCym() {
		return this.statisticalPopulationLabelCym;
	}

	public void setStatisticalPopulationLabelCym(String statisticalPopulationLabelCym) {
		this.statisticalPopulationLabelCym = statisticalPopulationLabelCym;
	}

	public String getStatisticalPopulationLabelEng() {
		return this.statisticalPopulationLabelEng;
	}

	public void setStatisticalPopulationLabelEng(String statisticalPopulationLabelEng) {
		this.statisticalPopulationLabelEng = statisticalPopulationLabelEng;
	}

	public Long getTimePeriodId() {
		return this.timePeriodId;
	}

	public void setTimePeriodId(Long timePeriodId) {
		this.timePeriodId = timePeriodId;
	}

	public String getTimePeriodNameCym() {
		return this.timePeriodNameCym;
	}

	public void setTimePeriodNameCym(String timePeriodNameCym) {
		this.timePeriodNameCym = timePeriodNameCym;
	}

	public String getTimePeriodNameEng() {
		return this.timePeriodNameEng;
	}

	public void setTimePeriodNameEng(String timePeriodNameEng) {
		this.timePeriodNameEng = timePeriodNameEng;
	}

	public String getTimeType() {
		return this.timeType;
	}

	public void setTimeType(String timeType) {
		this.timeType = timeType;
	}

	public String getUnitMultiplier() {
		return this.unitMultiplier;
	}

	public void setUnitMultiplier(String unitMultiplier) {
		this.unitMultiplier = unitMultiplier;
	}

	public String getUnitTypeCym() {
		return this.unitTypeCym;
	}

	public void setUnitTypeCym(String unitTypeCym) {
		this.unitTypeCym = unitTypeCym;
	}

	public String getUnitTypeEng() {
		return this.unitTypeEng;
	}

	public void setUnitTypeEng(String unitTypeEng) {
		this.unitTypeEng = unitTypeEng;
	}

	public BigDecimal getValue() {
		return this.value;
	}

	public void setValue(BigDecimal value) {
		this.value = value;
	}

	public String getValueDomainCym() {
		return this.valueDomainCym;
	}

	public void setValueDomainCym(String valueDomainCym) {
		this.valueDomainCym = valueDomainCym;
	}

	public String getValueDomainEng() {
		return this.valueDomainEng;
	}

	public void setValueDomainEng(String valueDomainEng) {
		this.valueDomainEng = valueDomainEng;
	}
	
	public String getTimePeriodCode() {
		return timePeriodCode;
	}

	public void setTimePeriodCode(String timePeriodCode) {
		this.timePeriodCode = timePeriodCode;
	}


	public List<StageCategory> getStageCategories() {
		return this.stageCategories;
	}

	public void setStageCategories(List<StageCategory> stageCategories) {
		this.stageCategories = stageCategories;
	}

	public StageCategory addStageCategory(StageCategory stageCategory) {
		getStageCategories().add(stageCategory);
		stageCategory.setStageDimensionalDataPoint(this);

		return stageCategory;
	}

	public StageCategory removeStageCategory(StageCategory stageCategory) {
		getStageCategories().remove(stageCategory);
		stageCategory.setStageDimensionalDataPoint(null);

		return stageCategory;
	}

}