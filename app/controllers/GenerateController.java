package controllers;

import play.*;
import play.data.Form;
import play.mvc.*;
import play.twirl.api.Html;
import views.html.*;
import models.*;
import play.data.FormFactory;
import play.data.validation.ValidationError;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import javax.inject.*;
import javax.persistence.EntityManager;

import services.CSVGenerator;
import play.db.jpa.JPAApi;
import play.db.jpa.Transactional;
/**
 * This controller contains an action to process the input file
 */
public class GenerateController extends Controller {
	@Inject 
	FormFactory formFactory;
	
	private String task = "TA";
    private String dsname;
    private Long dimdsid;
	
	@Inject
	JPAApi jpaApi;
    
    @Transactional
    public Result processform() {
    	Form<Generate> genForm = formFactory.form(Generate.class).bindFromRequest();
    	TimeZone tz = TimeZone.getTimeZone("Europe/London");
    	TimeZone.setDefault(tz);
    	
   	if(genForm.hasErrors()) {
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
   	   return badRequest(views.html.generate.render(genForm,datasetList));
   	} else {
    	
    	Generate g1 = genForm.get();
    	dsname = g1.getDsname();
		EntityManager em = jpaApi.em();
    	List <DataResource> dis = em.createQuery("SELECT d FROM DataResource d WHERE d.dataResource = :dsid",DataResource.class).setParameter("dsid", dsname).getResultList();
    	// Logger.info("size = " + dis.size());
    	DataResource drs = dis.get(0);
    	List <DimensionalDataSet> dimds = em.createQuery("SELECT d FROM DimensionalDataSet d WHERE d.dataResourceBean = :dsid",DimensionalDataSet.class).setParameter("dsid", drs).getResultList();
    	// Logger.info("size2 = " + dimds.size());
    	DimensionalDataSet dds = dimds.get(0);
    	dimdsid = dds.getDimensionalDataSetId();
      	Logger.info("Genarating CSV for Dataset Resource ID = " + dsname);
    	g1.setDimdsid(dimdsid);
    	g1.setStatus("OK");
    	List <Presentation> pressies = em.createQuery("SELECT p FROM Presentation p WHERE p.dimensionalDataSet = :dsid",Presentation.class).setParameter("dsid", dds).getResultList();
    	if (pressies.isEmpty())
    	{
			PresentationType pt1 = em.find(PresentationType.class, "CSV");
			if (pt1 == null){
			   pt1 = new PresentationType();
			   pt1.setPresentationType("CSV");
			   em.persist(pt1);
			}
			PresentationType pt2 = em.find(PresentationType.class, "XLS");
			if (pt2 == null){
			   pt2 = new PresentationType();
			   pt2.setPresentationType("XLS");
			   em.persist(pt1);
			}
    		// create presentation object
			String bees = "Some bees";
			byte [] mrblobby = bees.getBytes();
    		String filename = drs.getDataResource() + " " + drs.getTitle();
    		String url = "http://ec2-52-40-142-234.us-west-2.compute.amazonaws.com/generate/" + drs.getDataResource();
    		Presentation pres1 = new Presentation();
    		pres1.setFileName(filename);
    		pres1.setDownloadurl(url);
    		pres1.setDimensionalDataSet(dds);
    		pres1.setFileSize(1000L);
    		pres1.setFileData(mrblobby);
    		pres1.setPresentationTypeBean(pt1);
    		em.persist(pres1);
    		Presentation pres2 = new Presentation();
    		pres2.setFileName(filename);
    		pres2.setDownloadurl(url);
    		pres2.setDimensionalDataSet(dds);
    		pres2.setFileSize(1000L);
    		pres2.setFileData(mrblobby);
    		pres2.setPresentationTypeBean(pt2);
    		em.persist(pres2);
    	}
    	
    	CSVGenerator gen = new CSVGenerator(dsname, g1);

        gen.run(em, dimds.get(0), null, "/logs/somefile.csv");
    	em.flush();
    	em.clear();
    	
        if (g1.getStatus().equals("OK")){
        	response().setContentType("application/x-download");  
        	response().setHeader("Content-disposition","attachment; filename=" + dsname + ".csv"); 
        	File file = Play.application().getFile("/logs/" + dsname + ".csv");
        	//   return ok(new File("/logs/" + dsname + ".csv"));
        	return ok(file);
        }
        else
        {
        	return ok(views.html.message.render(("Dataset " + dsname + g1.getStatus()), Html.apply("<p>Dataset id: " + dsname + "</p>")));
        }
   	 }		

    }
    
    @Transactional
    public Result download(String dsname){
    	Generate g1 = new Generate();
    	g1.setDsname(dsname);
		EntityManager em = jpaApi.em();
    	List <DataResource> dis = em.createQuery("SELECT d FROM DataResource d WHERE d.dataResource = :dsid",DataResource.class).setParameter("dsid", dsname).getResultList();
    	// Logger.info("size = " + dis.size());
    	DataResource drs = dis.get(0);
    	List <DimensionalDataSet> dimds = em.createQuery("SELECT d FROM DimensionalDataSet d WHERE d.dataResourceBean = :dsid",DimensionalDataSet.class).setParameter("dsid", drs).getResultList();
    	// Logger.info("size2 = " + dimds.size());
    	dimdsid = dimds.get(0).getDimensionalDataSetId();
      	Logger.info("Genarating CSV for Dataset Resource ID = " + dsname);
    	g1.setDimdsid(dimdsid);
    	g1.setStatus("OK");
    	CSVGenerator gen = new CSVGenerator(dsname, g1);

        gen.run(em, dimds.get(0), null, "/logs/someFile.csv");
	
        if (g1.getStatus().equals("OK")){
        	response().setContentType("application/x-download");  
        	response().setHeader("Content-disposition","attachment; filename=" + dsname + ".csv"); 
        	File file = Play.application().getFile("/logs/" + dsname + ".csv");
        	//   return ok(new File("/logs/" + dsname + ".csv"));
        	return ok(file);
        }
        else
        {
        	return ok(views.html.message.render(("Dataset " + dsname + g1.getStatus()), Html.apply("<p>Dataset id: " + dsname + "</p>")));
        }
    	
    }
    
}
