package controllers;

import play.*;
import play.data.Form;
import play.mvc.*;
import play.twirl.api.Html;
import utils.TimeHelper;
import models.*;
import play.data.FormFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import javax.inject.*;
import javax.persistence.EntityManager;

import play.db.jpa.JPAApi;
import play.db.jpa.Transactional;
/**
 * This controller contains an action to process the input file
 */
public class AttributesController extends Controller {
	private static final Logger.ALogger logger = Logger.of(AttributesController.class);
	@Inject 
	FormFactory formFactory;

	private String datasetid;
	private Long datasetdefid;
	
	@Inject
	JPAApi jpaApi;
	
	@Transactional
	public Result prepareAtts(String dsid) {
    	datasetid = dsid;
		EntityManager em = jpaApi.em();
		Form<Attribs> attForm = formFactory.form(Attribs.class);
    	Attribs a1 = new Attribs();
    	List <DataResource> dis = em.createQuery("SELECT d FROM DataResource d WHERE d.dataResource = :dsid",DataResource.class).setParameter("dsid", datasetid).getResultList();
    	Logger.info("datasetid = " + datasetid);
   // 	Logger.info("size = " + dis.size());
    	DataResource drs = dis.get(0);
    	List<Taxonomy> tList = drs.getTaxonomies();
    	if (tList !=null && !tList.isEmpty()){
    		a1.setTaxonomy(tList.get(0).getTaxonomy());
    	}
    	a1.setColumn_concept(drs.getColumnConcept());
    	a1.setRow_concept(drs.getRowConcept());
    	a1.setDataResource(datasetid);
    	List <DimensionalDataSet> dimds = em.createQuery("SELECT d FROM DimensionalDataSet d WHERE d.dataResourceBean = :dsid",DimensionalDataSet.class).setParameter("dsid", drs).getResultList();
   // 	Logger.info("size2 = " + dimds.size());
    	datasetdefid = dimds.get(0).getDimensionalDataSetId();
    	ArrayList<String> conList = new ArrayList<String>();
   
      	List <Category> cons = em.createQuery("SELECT c FROM Category c WHERE EXISTS "
    			+ "(SELECT dds.dimensionalDataSetId FROM DimensionalDataSet dds JOIN dds.dimensionalDataPoints ddp "
    			+ "JOIN ddp.variable v JOIN v.categories cat WHERE ddp.variable.variableId = v.variableId "
    			+ "AND cat.categoryId = c.categoryId "
    			+ "AND dds.dataResourceBean=:dsid)",Category.class).setParameter("dsid", drs).getResultList();

    	conList.add("Geographic Area");
    	conList.add("Time Period");
    	
    	Logger.info("concepts found = " + cons.size());
    	for (int i=0; i< cons.size(); i++){
    		String testString = cons.get(i).getConceptSystemBean().getConceptSystem();
    		if (!conList.contains(testString)){
    			conList.add(testString);
    		}
    		
    	}
    	a1.setConceptList(conList);
    	List <Taxonomy> taxes = em.createNamedQuery("Taxonomy.findAll",Taxonomy.class).getResultList();
    	ArrayList<String> taxList = new ArrayList<String>();
    	for (int j=0; j< taxes.size(); j++){
    		String testString = taxes.get(j).getTaxonomy();
    		if (!taxList.contains(testString)){
    			taxList.add(testString);
    		}
    		
    	}
    	
    	return ok(views.html.attribs.render(attForm.fill(a1),conList,taxList)); 
	}
	
	@Transactional
    public Result processform() {
    	TimeZone tz = TimeZone.getTimeZone("Europe/London");
    	TimeZone.setDefault(tz);
		EntityManager em = jpaApi.em();
		Form<Attribs> attForm = formFactory.form(Attribs.class);
    	Attribs a1 = attForm.bindFromRequest().get();
    	a1.setStatus("Attributes updated OK");
    	List <DimensionalDataSet> dimds = em.createQuery("SELECT d FROM DimensionalDataSet d WHERE d.dimensionalDataSetId = :dsid",DimensionalDataSet.class).setParameter("dsid", datasetdefid).getResultList();
    	DimensionalDataSet ds = dimds.get(0);
    	try {
			String timeStamp = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date());
			ds.setModified(timeStamp);
			ds.setValidationException("");
			ds.setLoadException("");
			ds.setValidationMessage("Success");
        	ds.setStatus("3-Attributes-OK");
        	ds.setSource("ONS");
        	ds.setContact("Darren Barnes");
        	ds.setReleaseDate("Christmas");
        	ds.setNextRelease("Easter");
    		List <DataResource> dis = em.createQuery("SELECT d FROM DataResource d WHERE d.dataResource = :dsid",DataResource.class).setParameter("dsid", datasetid).getResultList();
    		DataResource drs = dis.get(0);
    		drs.setRowConcept(a1.getRow_concept());
    		drs.setColumnConcept(a1.getColumn_concept());
    		List<Taxonomy> tList = new ArrayList<Taxonomy>();
    		Taxonomy taxi = em.find(Taxonomy.class, a1.getTaxonomy());
    		tList.add(taxi);
    		drs.setTaxonomies(tList);
    		em.merge(drs);
    	}
	    catch (Exception e) {
	    	e.printStackTrace();
	//    	logger.error(String.format("Edit Attributes Failed " + e.getMessage() ));
	    	ds.setStatus("3-Attributes-Failed");
	    	ds.setValidationException(String.format("Edit Attributes Failed " + e.getMessage()));
	        a1.setStatus(String.format("Edit Attributes Failed " + e.getMessage()));
	    }
	 finally {
		em.merge(ds);
		em.flush();
		em.clear();
	}
		return ok(views.html.message.render((a1.getStatus()), Html.apply("<p>Column Concept: " + a1.getColumn_concept() + "<br/>Row Concept: " + a1.getRow_concept() + "<br/>Taxonomy Node: " + a1.getTaxonomy() + "</p>")));

    }
    
}
