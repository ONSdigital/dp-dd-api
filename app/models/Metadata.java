package models;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import play.data.validation.ValidationError;

public class Metadata {
	private String resourceId;
	private Long dimdsid;
	private String json;
	private String status;
	  
	public Long getDimdsid() {
		return dimdsid;
	}
	public void setDimdsid(Long dimdsid) {
		this.dimdsid = dimdsid;
	}
	public String getJson() {
		return json;
	}
	public void setJson(String json) {
		this.json = json;
	}
	  public String getResourceId() {
		return resourceId;
	}
	public void setResourceId(String resourceId) {
		this.resourceId = resourceId;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}

   public List<ValidationError> validate() {

      List<ValidationError> errors = new ArrayList<ValidationError>();
      
      if (resourceId == null) {
        errors.add(new ValidationError("datasetid", "No id was given."));
      }
      
      if (json == null || json.length() == 0) {
         errors.add(new ValidationError("json", "No JSON was given."));
       }
      
      if(errors.size() > 0){
          return errors;
      }
      else{
        return null;
      }  
    }
	
}
