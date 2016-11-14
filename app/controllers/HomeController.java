package controllers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;
import javax.persistence.EntityManager;

import models.DataResource;
import models.Dataset;
import models.DimensionalDataSet;
import models.Editor;
import models.Generate;
import models.Metadata;
import models.EditAttribs;

import play.data.Form;
import play.data.FormFactory;
import play.db.jpa.JPAApi;
import play.db.jpa.Transactional;
import play.mvc.Controller;
import play.mvc.Result;


/**
 * This controller contains an action to handle HTTP requests
 * to the application's home page.
 */
public class HomeController extends Controller {

    /**
     * An action that renders an HTML page with a welcome message.
     * The configuration in the <code>routes</code> file means that
     * this method will be called when the application receives a
     * <code>GET</code> request with a path of <code>/</code>.
     */ 
	
	@Inject 
	FormFactory formFactory;
    
	@Inject
	JPAApi jpaApi;
	
    public Result create() {
    	Form<Dataset> dsForm = formFactory.form(Dataset.class);
        return ok(views.html.create.render(dsForm));
     }
    
    public Result publish() {
        return ok(views.html.publish.render("na"));
    }   
    
    @Transactional
    public Result metadata() {
    	Form<Metadata> metaForm = formFactory.form(Metadata.class);
    	Metadata m1 = new Metadata();
    	m1.setDimdsid(11L);
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
        return ok(views.html.metadata.render(metaForm.fill(m1),datasetList));
    }
    
	@Transactional
    public Result loadTarget() {
		EntityManager em = jpaApi.em();
    	Form<Editor> editForm = formFactory.form(Editor.class);
    	Editor ed1 = new Editor();
    	ed1.setTask("T");
    	ed1.setDimdsid(11L); 
    	List <DimensionalDataSet> dis = em.createQuery("SELECT d FROM DimensionalDataSet d WHERE d.status = '1-Staging-OK'" ,DimensionalDataSet.class).getResultList();
    	ArrayList<String> datasetList = new ArrayList<String>();
    	for (int i=0; i<dis.size(); i++)
    	{
    		datasetList.add(dis.get(i).getDataResourceBean().getDataResource());
    	}
    	Collections.sort(datasetList);
    	em.flush();
    	em.clear();
        return ok(views.html.loadtarget.render(editForm.fill(ed1),datasetList));
    } 
    
	@Transactional
    public Result generate() {
    	Form<Generate> genForm = formFactory.form(Generate.class);
    	Generate gen = new Generate();
		EntityManager em = jpaApi.em();
    	List <DimensionalDataSet> dis = em.createQuery("SELECT d FROM DimensionalDataSet d WHERE d.status IN ('2-Target-OK','3-Attributes-OK','4-Metadata-OK','5-Generate-Failed','5-Generate-OK')" ,DimensionalDataSet.class).getResultList();
    	ArrayList<String> datasetList = new ArrayList<String>();
    	for (int i=0; i<dis.size(); i++)
    	{
    		datasetList.add(dis.get(i).getDataResourceBean().getDataResource());
    	}
    	Collections.sort(datasetList);
    	em.flush();
    	em.clear();
        return ok(views.html.generate.render(genForm.fill(gen),datasetList));
    }
	
	@Transactional
    public Result editattribs() {
     	Form<EditAttribs> editAttribsForm = formFactory.form(EditAttribs.class);
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
        return ok(views.html.editattribs.render(editAttribsForm,datasetList));
     }  
    
    public Result index() {
       return ok(views.html.index.render("Welcome"));
  }
}
