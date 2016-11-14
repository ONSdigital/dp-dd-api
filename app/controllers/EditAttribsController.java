package controllers;

import play.data.Form;
import play.mvc.*;
import models.*;
import play.data.FormFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.*;
import javax.persistence.EntityManager;

import play.db.jpa.JPAApi;
import play.db.jpa.Transactional;
/**
 * This controller contains an action to process the input file
 */
public class EditAttribsController extends Controller {
	@Inject 
	FormFactory formFactory;
	
    private String dsid;
	
	@Inject
	JPAApi jpaApi;
    
    @Transactional
    public Result processform() {
    	Form<EditAttribs> editAttribsForm = formFactory.form(EditAttribs.class).bindFromRequest();
    	
   	if(editAttribsForm.hasErrors()) {
		EntityManager em = jpaApi.em();
    	List <DimensionalDataSet> dis = em.createQuery("SELECT d FROM DimensionalDataSet d WHERE d.status IN ('2-Target-OK','3-Attributes-OK','4-Metadata-OK','3-Attributes-Failed','5-Generate-OK')" ,DimensionalDataSet.class).getResultList();
    	ArrayList<String> datasetList = new ArrayList<String>();
    	for (int i=0; i<dis.size(); i++)
    	{
    		datasetList.add(dis.get(i).getDataResourceBean().getDataResource());
    	}
    	Collections.sort(datasetList);
    	em.flush();
    	em.clear();
   		return badRequest(views.html.editattribs.render(editAttribsForm,datasetList));
   	} else {
    	
    	EditAttribs edat = editAttribsForm.get();
    
    	dsid = edat.getDsid();
   
       /* return ok(views.html.attribs.render(prepareAtts(dsid))); */
        return redirect(routes.AttributesController.prepareAtts(dsid));
   	 }		

    }
    
}
