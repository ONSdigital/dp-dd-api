package models;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import play.data.validation.ValidationError;

public class EditAttribs {
	  private String dsid;
     private String status;
  	private List<String> dsidList;
	  
	public String getDsid() {
		return dsid;
	}
	public void setDsid(String dsid) {
		this.dsid = dsid;
	}

   public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	
   public List<ValidationError> validate() {

      List<ValidationError> errors = new ArrayList<ValidationError>();
      
      if (dsid == null || dsid.length() == 0) {
        errors.add(new ValidationError("dsid", "No dataset id was given."));
      }
      
      if(errors.size() > 0){
          return errors;
      }
      else{
        return null;
      }  
    }
	
}