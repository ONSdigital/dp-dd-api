package models;

import java.io.Serializable;
import javax.persistence.*;


/**
 * The persistent class for the presentation database table.
 * 
 */
@Entity
@NamedQuery(name="Presentation.findAll", query="SELECT p FROM Presentation p")
public class Presentation implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
    @SequenceGenerator( name = "presseq", sequenceName = "presseq", allocationSize = 1, initialValue = 1 )
    @GeneratedValue( strategy = GenerationType.SEQUENCE, generator = "presseq" )
	@Column(name="presentation_id")
	private Long presentationId;

	private String accessurl;

	private String conformsto;

	private String description;

	private String downloadurl;

	@Column(name="file_data")
	private byte[] fileData;

	@Column(name="file_name")
	private String fileName;

	@Column(name="file_size")
	private Long fileSize;

	private String format;

	@Column(name="json_metadata")
	private String jsonMetadata;

	private String spatial;

	private String temporal;

	private String title;

	//bi-directional many-to-one association to DimensionalDataSet
	@ManyToOne
	@JoinColumn(name="dimensional_data_set_id")
	private DimensionalDataSet dimensionalDataSet;

	//bi-directional many-to-one association to PresentationType
	@ManyToOne
	@JoinColumn(name="presentation_type")
	private PresentationType presentationTypeBean;

	public Presentation() {
	}

	public Long getPresentationId() {
		return this.presentationId;
	}

	public void setPresentationId(Long presentationId) {
		this.presentationId = presentationId;
	}

	public String getAccessurl() {
		return this.accessurl;
	}

	public void setAccessurl(String accessurl) {
		this.accessurl = accessurl;
	}

	public String getConformsto() {
		return this.conformsto;
	}

	public void setConformsto(String conformsto) {
		this.conformsto = conformsto;
	}

	public String getDescription() {
		return this.description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getDownloadurl() {
		return this.downloadurl;
	}

	public void setDownloadurl(String downloadurl) {
		this.downloadurl = downloadurl;
	}

	public byte[] getFileData() {
		return this.fileData;
	}

	public void setFileData(byte[] fileData) {
		this.fileData = fileData;
	}

	public String getFileName() {
		return this.fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public Long getFileSize() {
		return this.fileSize;
	}

	public void setFileSize(Long fileSize) {
		this.fileSize = fileSize;
	}

	public String getFormat() {
		return this.format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public String getJsonMetadata() {
		return this.jsonMetadata;
	}

	public void setJsonMetadata(String jsonMetadata) {
		this.jsonMetadata = jsonMetadata;
	}

	public String getSpatial() {
		return this.spatial;
	}

	public void setSpatial(String spatial) {
		this.spatial = spatial;
	}

	public String getTemporal() {
		return this.temporal;
	}

	public void setTemporal(String temporal) {
		this.temporal = temporal;
	}

	public String getTitle() {
		return this.title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public DimensionalDataSet getDimensionalDataSet() {
		return this.dimensionalDataSet;
	}

	public void setDimensionalDataSet(DimensionalDataSet dimensionalDataSet) {
		this.dimensionalDataSet = dimensionalDataSet;
	}

	public PresentationType getPresentationTypeBean() {
		return this.presentationTypeBean;
	}

	public void setPresentationTypeBean(PresentationType presentationTypeBean) {
		this.presentationTypeBean = presentationTypeBean;
	}

}