package controllers;

import play.Logger;
import play.data.Form;
import play.mvc.*;
import models.*;
import play.data.FormFactory;
import play.data.validation.ValidationError;

import java.io.File;
import java.util.ArrayList;
import java.util.TimeZone;

import javax.inject.*;
import javax.persistence.EntityManager;

import services.InputCSVParser;
import services.LoadToTarget;
import play.db.jpa.JPAApi;
import play.db.jpa.Transactional;
import play.twirl.api.Html;


/**
 * This controller contains an action to process the input file
 */
public class CreateController extends Controller {
	private static final Logger.ALogger logger = Logger.of(CreateController.class);
	
	@Inject 
	FormFactory formFactory;
	
	@Inject
	JPAApi jpaApi;
	
    private String id;
    private String title;   

    private String filename;
    private String status;
    
    @Transactional
    public Result processform() {
   	 
    	TimeZone tz = TimeZone.getTimeZone("Europe/London");
    	TimeZone.setDefault(tz);
    	Form<Dataset> dsForm = formFactory.form(Dataset.class).bindFromRequest();
    	if(dsForm.hasErrors()) {
    	   return badRequest(views.html.create.render(dsForm));
    	} else {
    		
   	    play.mvc.Http.MultipartFormData<File> body = request().body().asMultipartFormData();
    	    play.mvc.Http.MultipartFormData.FilePart<File> filename = body.getFile("filename");	
    		
    	Dataset form = dsForm.get();
    	  id = form.getId();    		 
          title = form.getTitle();
       //   filename = filename.getFilename();
          status   = form.getStatus();
          String fileName = filename.getFilename();
          java.io.File file = filename.getFile();
       	  InputCSVParser inputCSV = new InputCSVParser(form,file);
   		  EntityManager em = jpaApi.em(); 
			
   		  DataResource testdrs = em.find(DataResource.class, id);
			if (testdrs != null){
					Logger.error("dataset " + id + " already exists");
			    	 ValidationError e = new ValidationError("id", "dataset " + id + " already exists",new ArrayList());
			    	 ArrayList<ValidationError> errors = new ArrayList<ValidationError>();
			    	 errors.add(e);
			    	 dsForm.errors().put("id",errors);
			    	 
			    	 ValidationError g = new ValidationError("", "ERROR",new ArrayList());
			    	 ArrayList<ValidationError> gerrors = new ArrayList<ValidationError>();
			    	 gerrors.add(g);
			    	 dsForm.errors().put("",gerrors);

		    	   return badRequest(views.html.create.render(dsForm));
			}
   		  DataResource drs = new DataResource();
   		  drs.setDataResource(id);
   		  drs.setTitle(title);   		
   		  DimensionalDataSet dds = new DimensionalDataSet();
   		  dds.setDataResourceBean(drs);
   		  dds.setTitle(title);
   		  em.persist(drs);
   		  em.persist(dds);
       	  em.flush();
       	  em.clear();
       	  inputCSV.runJPA(em, dds);
       	  em.flush();
       	  em.clear();
         
         return ok(views.html.message.render((id + " " + form.getStatus()), Html.apply("<p>Dataset id: " + id + "<br/>Dataset title: " + title + "<br/>Filename: " + fileName + "<br/>Status: " + form.getStatus() + "</p>")));
      }
   }
}

