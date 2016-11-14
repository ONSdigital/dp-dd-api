package models;

import java.util.List;

public class Attribs {
	private String dataResource;
	private String column_concept;
	private String row_concept;
	private List<String> conceptList;
	private String status;
	private String taxonomy;
	
	public String getColumn_concept() {
		return column_concept;
	}
	public String getDataResource() {
		return dataResource;
	}
	public void setDataResource(String dataResource) {
		this.dataResource = dataResource;
	}
	public void setColumn_concept(String column_concept) {
		this.column_concept = column_concept;
	}
	public String getRow_concept() {
		return row_concept;
	}
	public void setRow_concept(String row_concept) {
		this.row_concept = row_concept;
	}
	public List<String> getConceptList() {
		return conceptList;
	}
	public void setConceptList(List<String> conceptList) {
		this.conceptList = conceptList;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getTaxonomy() {
		return taxonomy;
	}
	public void setTaxonomy(String taxonomy) {
		this.taxonomy = taxonomy;
	}
	
}
 