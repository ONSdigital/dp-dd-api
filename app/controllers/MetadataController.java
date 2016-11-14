package controllers;

import play.Logger;
import play.data.Form;
import play.mvc.*;
import models.*;
import play.data.FormFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.*;
import javax.persistence.EntityManager;

import services.LoadToTarget;
import services.MetadataParser;
import play.db.jpa.JPAApi;
import play.db.jpa.Transactional;

import play.twirl.api.Html;

/**
 * This controller contains an action to process the input file
 */
public class MetadataController extends Controller {
	private static final Logger.ALogger logger = Logger.of(MetadataController.class);
	@Inject 
	FormFactory formFactory;
	
	private String json;
    private String dsname;
    private Long dimdsid;
	
	@Inject
	JPAApi jpaApi;
	
	
   @Transactional
   public Result processform() {
   	
   Form<Metadata> metaForm = formFactory.form(Metadata.class).bindFromRequest();
   	if(metaForm.hasErrors()) {
		EntityManager em = jpaApi.em();
    	List <DimensionalDataSet> dis = em.createQuery("SELECT d FROM DimensionalDataSet d WHERE d.status IN ('2-Target-OK','3-Attributes-OK','4-Metadata-OK','4-Metadata-Failed','5-Generate-OK')" ,DimensionalDataSet.class).getResultList();
    	ArrayList<String> datasetList = new ArrayList<String>();
    	for (int i=0; i<dis.size(); i++)
    	{
    		datasetList.add(dis.get(i).getDataResourceBean().getDataResource());
    	}
    	Collections.sort(datasetList);
    	em.flush();
    	em.clear();
   	   return badRequest(views.html.metadata.render(metaForm,datasetList));
   	} else {
  		
   		Metadata met1 = metaForm.get();
     	json = met1.getJson();
    	dsname = met1.getResourceId();
		met1.setStatus(" metdata loaded OK");
    	MetadataParser mp = new MetadataParser(met1);
		EntityManager em = jpaApi.em();
        mp.runJPA(em);		
  
    return ok(views.html.message.render(("Dataset " + dsname + met1.getStatus()), Html.apply("<p>Dataset id: " + dsname + "</p>")));
 }
    
    }
    
}
