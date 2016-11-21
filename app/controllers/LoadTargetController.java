package controllers;

import play.*;
import play.data.Form;
import play.mvc.*;
import play.twirl.api.Html;
import models.*;
import play.data.FormFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import javax.inject.*;
import javax.persistence.EntityManager;

import services.LoadToTarget;
import play.db.jpa.JPAApi;
import play.db.jpa.Transactional;
/**
 * This controller contains an action to process the input file
 */
public class LoadTargetController extends Controller {
	private static final Logger.ALogger logger = Logger.of(LoadTargetController.class);
	
	@Inject 
	FormFactory formFactory;
	
	private String task = "TA";
    private String dsname;
    private Long dimdsid;
	
	@Inject
	JPAApi jpaApi;
    
    @Transactional
    public Result processform() {
    	Form<Editor> editForm = formFactory.form(Editor.class).bindFromRequest();
    	TimeZone tz = TimeZone.getTimeZone("Europe/London");
    	TimeZone.setDefault(tz);
    	
    	if(editForm.hasErrors()) {
    		EntityManager em = jpaApi.em();
    		List <DimensionalDataSet> dis = em.createQuery("SELECT d FROM DimensionalDataSet d WHERE d.status = '1-Staging-OK'" ,DimensionalDataSet.class).getResultList();
    		ArrayList<String> datasetList = new ArrayList<String>();
    		for (int i=0; i<dis.size(); i++)
    		{
    			datasetList.add(dis.get(i).getDataResourceBean().getDataResource());
    		}
    		Collections.sort(datasetList);
    		em.flush();
    		em.clear();
    		return badRequest(views.html.loadtarget.render(editForm,datasetList));
    	} else {
    		Editor ed1 = editForm.get();
    		task = ed1.getTask();
    		dsname = ed1.getDsname();
    		EntityManager em = jpaApi.em();
    		List <DataResource> dis = em.createQuery("SELECT d FROM DataResource d WHERE d.dataResource = :dsid",DataResource.class).setParameter("dsid", dsname).getResultList();
    		// Logger.info("size = " + dis.size());
    		DataResource drs = dis.get(0);
    		List <DimensionalDataSet> dimds = em.createQuery("SELECT d FROM DimensionalDataSet d WHERE d.dataResourceBean = :dsid",DimensionalDataSet.class).setParameter("dsid", drs).getResultList();

			dimdsid = dimds.get(0).getDimensionalDataSetId();
    		new LoadToTarget().run(em, dimdsid);

    		return ok(views.html.message.render(("Dataset " + dsname + ed1.getStatus()), Html.apply("<p>Dataset id: " + dsname + "</p>")));
   	
   		}		

    }
    
}
