package controllers;

import play.mvc.*;
import models.*;
import play.data.FormFactory;
import java.util.List;
import java.util.TimeZone;

import javax.inject.*;
import javax.persistence.EntityManager;

import play.db.jpa.JPAApi;
import play.db.jpa.Transactional;
/**
 * This controller contains an action to process the input file
 */
public class StatusController extends Controller {
	@Inject 
	FormFactory formFactory;
	
	@Inject
	JPAApi jpaApi;
    
    @Transactional
    public Result getDatasets(){
    	TimeZone tz = TimeZone.getTimeZone("Europe/London");
    	TimeZone.setDefault(tz);
    	EntityManager em = jpaApi.em();
        List <DimensionalDataSet> gcet = em.createQuery("SELECT d FROM DimensionalDataSet d",DimensionalDataSet.class).getResultList();
        return ok(views.html.status.render(gcet));    
    }
    
}
