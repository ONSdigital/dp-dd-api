package services;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceException;

import play.*;
import utils.TimeHelper;

import org.eclipse.persistence.exceptions.DatabaseException;

import exceptions.CSVValidationException;
import exceptions.GLLoadException;
import models.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class LoadToTarget {
	private static final Logger.ALogger logger = Logger.of(LoadToTarget.class);

	public void run(EntityManager em, Long ddsid) {
		logger.info(String.format("Loading to Target started for dataset id " + ddsid));
    	TimeZone tz = TimeZone.getTimeZone("Europe/London");
    	TimeZone.setDefault(tz);
		DimensionalDataSet ds = em.find(DimensionalDataSet.class, ddsid);
		try {
			String timeStamp = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date());
			ds.setModified(timeStamp);
			ds.setValidationMessage("");
			ds.setValidationException("");
			ds.setLoadException("");
			em.merge(ds);

			Long recct = stageToTarget(em, ds);


			ds.setStatus("2-Target-OK");
			ds.setObscount(recct);
			logger.info(String.format("Load to Target successful"));
		} catch (CSVValidationException validationException) {
			ds.setStatus("2-Target-Failed");
			ds.setValidationMessage(validationException.getMessage());
			ds.setValidationException(validationException.getLocalizedMessage());
			logger.info(String.format("Loading to target not successful - " + validationException.getMessage() ));
		} catch (GLLoadException loadException) {
			ds.setStatus("2-Target-Failed");
			ds.setValidationException(loadException.getMessage());
			ds.setLoadException(loadException.getMessage());
			logger.info(String.format("Loading to target not successful - " +  loadException.getMessage() ));
		} finally {
			em.merge(ds);
			em.flush();
			em.clear();
		}

	}

	private void createDefaultUnitTypes(EntityManager em) {
        UnitType ut = em.find(UnitType.class, "Persons");
        if (ut == null){
            ut = new UnitType();
            ut.setUnitType("Persons");
            em.persist(ut);
        }
        ValueDomain vd = em.find(ValueDomain.class, "Count");
        if (vd == null){
            vd = new ValueDomain();
            vd.setValueDomain("Count");
            em.persist(vd);
        }
        TimeType ttq = em.find(TimeType.class, "QUARTER");
        if (ttq == null){
            ttq = new TimeType();
            ttq.setTimeType("QUARTER");
            em.persist(ttq);
        }
        TimeType ttm = em.find(TimeType.class, "MONTH");
        if (ttm == null){
            ttm = new TimeType();
            ttm.setTimeType("MONTH");
            em.persist(ttm);
        }
        TimeType tty = em.find(TimeType.class, "YEAR");
        if (tty == null){
            tty = new TimeType();
            tty.setTimeType("YEAR");
            em.persist(tty);
        }
    }

	private Long stageToTarget(EntityManager em, DimensionalDataSet ds){
		/*    	
		For each staged dimensional data point matching the current dimensional data set id...
		    	
		    	1. Create a skeleton dimensional data point record in memory
		    	2. Fetch the staged category records for current observation seq id
		    	3. For each staged category record
		    		3.1. Try to fetch the concept id, if not found create new concept
		    		3.2. Try to fetch the category id, if not found create new category
		    	4. If no new items created in 3.1 and 3.2, fetch the variable id for the combo, else create a variable and a set of variablecategory records for it
		    	5. Try to fetch the geographic area id by extcode, if not found create new geographic area and derive area and level types via lookup on first three digits of extcode
		    	6. Try to fetch a time id for the current time code. If not found create a new time_period entry.
		    	7. Try to find a population record for the current area / time combo, if not found create a new one for it
		    	8. we should now have all the required ids populated and can do a "persist" on the data.
		CHANGE - count the times and areas and fix if only one
		CHANGE - Assume geography preloaded - error if not
		*/
		Boolean singlearea = true;
		Boolean singletime = true;
        Long recct = 0L;
        String variableName = "";
		try {

			List areas =  em.createQuery("SELECT distinct s.geographicArea FROM StageDimensionalDataPoint s WHERE s.dimensionalDataSetId = " + ds.getDimensionalDataSetId(), StageDimensionalDataPoint.class).getResultList();
			List times =  em.createQuery("SELECT distinct s.timePeriodCode FROM StageDimensionalDataPoint s WHERE s.dimensionalDataSetId = " + ds.getDimensionalDataSetId(), StageDimensionalDataPoint.class).getResultList();
		 	logger.info("area count = " + areas.size());
		 	logger.info("time count =  " + times.size());
			if (areas.size() > 1){
				singlearea = false;
			}
			if (times.size() > 1){
				singletime = false;
			}

			// set default types
			createDefaultUnitTypes(em);

			List results =  em.createQuery("SELECT s FROM StageDimensionalDataPoint s WHERE s.dimensionalDataSetId = " + ds.getDimensionalDataSetId() +" order by s.geographicArea, s.timePeriodCode", StageDimensionalDataPoint.class).getResultList();
			logger.info("records found = " + results.size());

            setSameTypeAndDomainForAllRecords(em, (StageDimensionalDataPoint)results.get(0));

	    	String extCode = (String)areas.get(0);
			GeographicArea singlegeo = null;
			if (singlearea){
				List singleAreaList =  em.createQuery("SELECT a FROM GeographicArea a WHERE a.extCode = :ecode",GeographicArea.class).setParameter("ecode", extCode).getResultList();
				//		logger.info("arealist = " + areaList.size());
			
				if (singleAreaList.isEmpty()){
					logger.error(String.format("area " + extCode + " not found on database"));
					throw new CSVValidationException(String.format("area " + extCode + " not found on database"));
				}
				else
				{
					singlegeo = (GeographicArea)singleAreaList.get(0);
				}
			}
			TimeHelper thelp = new TimeHelper();
			TimePeriod singleTim = null;
			if (singletime){
				String timeCode = (String)times.get(0);
				List timeList =  em.createQuery("SELECT t FROM TimePeriod t WHERE t.name = :tcode",TimePeriod.class).setParameter("tcode", timeCode).getResultList();
			//	logger.info("timelist = " + timeList.size());
				if (timeList.isEmpty()){
					StageDimensionalDataPoint tsp = (StageDimensionalDataPoint)results.get(0);
					singleTim = new TimePeriod();
					singleTim.setName(timeCode);
        			singleTim.setStartDate(thelp.getStartDate(timeCode));
        			singleTim.setEndDate(thelp.getEndDate(timeCode));
					singleTim.setTimeTypeBean(em.find(TimeType.class, "YEAR"));
					if (tsp.getTimeType().equalsIgnoreCase("QUARTER"))
					{
						singleTim.setTimeTypeBean(em.find(TimeType.class, "QUARTER"));
					}
					if (tsp.getTimeType().equalsIgnoreCase("MONTH"))
					{
						singleTim.setTimeTypeBean(em.find(TimeType.class, "MONTH"));
					}
					em.persist(singleTim);
				}
				else
				{
					singleTim = (TimePeriod)timeList.get(0);
				}
			}

			//	no more than 5000000 records allowed!
			int recstoload = 5000000;
			int chunksize = 1000; 
			if (results.size() < recstoload){
				recstoload = results.size();
		    }
			
		
			for (int i = 0; i < recstoload; i++){
				StageDimensionalDataPoint sdp = (StageDimensionalDataPoint)results.get(i);
		    //	1. Create a skeleton dimensional data point record in memory
				DimensionalDataPoint dp = new DimensionalDataPoint();
				dp.setDimensionalDataSet(ds);
				dp.setValue(sdp.getValue());
				
		    //	2. Fetch the staged category records for current observation seq id
				List<StageCategory> clist = sdp.getStageCategories();
			
				// sort list by dimension number
				ArrayList<StageCategory> sclist = new ArrayList<StageCategory>();
				for (int n = 0; n < clist.size(); n++){
					for (int m = 0; m < clist.size(); m++){
						if (clist.get(m).getId().getDimensionNumber() == n){
							sclist.add(clist.get(m));
						}
					}
				}
		//		logger.info("clist = " + clist.size());	
		    //	3. For each staged category record
				ArrayList <Category> vcatList = new ArrayList<Category>();
		    	for (int j = 0; j < sclist.size(); j++){
	    	
		    		
		    //	3.1. Try to fetch the concept id, if not found create new concept
		       		StageCategory scat = sclist.get(j);
		    //		logger.info("catno = " + scat.getId().getDimensionNumber());
		    		String conceptName = scat.getConceptSystemLabelEng();
	    	//	3.2. Try to fetch the category id, if not found create new category
		    		String categoryName = scat.getCategoryNameEng();
					List catList =  em.createQuery("SELECT c FROM Category c WHERE c.name = :cname",Category.class).setParameter("cname", categoryName).getResultList();
					Category cat = null;
			//		logger.info("catlist = " + catList.size());
					if (catList.isEmpty()){
		    		 cat = new Category();
			    	 cat.setName(categoryName);
			    	 ConceptSystem consys = em.find(ConceptSystem.class, conceptName);
			    	 	if (consys == null){		
			    			consys = new ConceptSystem();
				    		consys.setConceptSystem(conceptName);
				    		em.persist(consys);
			    		}
			    	cat.setConceptSystemBean(consys);
			    	em.persist(cat);
		    		}
		    		else
		    		{
		    			cat = (Category)catList.get(0);
		    		}
					vcatList.add(cat);
			    //  	4. If no new items created in 3.1 and 3.2, fetch the variable id for the combo, else create a variable and a set of variablecategory records for it		    	
			    	
		    	}
		    		    	
		    	StringBuilder variableText = new StringBuilder("");
		    	for (int k = 0; k < vcatList.size(); k++){
		    	variableText.append(vcatList.get(k).getName());
	    		if (k < vcatList.size()-1){
		    			variableText.append(" | ");
		    		}
		    	}
		    	variableName = variableText.toString();

		//		logger.info("variableName = " + variableName);
				List varList =  em.createQuery("SELECT v FROM Variable v WHERE v.name = :vname",Variable.class).setParameter("vname", variableName).getResultList();
				Variable var = null;
			//	logger.info("varlist size = " + varList.size());
			//	int varct = varList.size();
				if (varList.isEmpty()){
	    		 var = new Variable();
		    	 var.setName(variableName);
		    	 var.setUnitTypeBean(em.find(UnitType.class, "Persons"));
		    	 var.setValueDomainBean(em.find(ValueDomain.class, "Count"));
		    	 var.setCategories(vcatList);
			     em.persist(var);
	    		}
	    		else
	    		{
	    			var = (Variable)varList.get(0);
	    		}

				
		   // 	5. Try to fetch the geographic area id by extcode, if not found create new geographic area and derive area and level types via lookup on first three digits of extcode
				GeographicArea geo = singlegeo;
				if (!singlearea){
				     extCode = sdp.getGeographicArea();
				     List areaList =  em.createQuery("SELECT a FROM GeographicArea a WHERE a.extCode = :ecode",GeographicArea.class).setParameter("ecode", extCode).getResultList();
					//		logger.info("arealist = " + areaList.size());
					if (areaList.isEmpty()){
						logger.error(String.format("area " + extCode + " not found on database"));
						throw new CSVValidationException(String.format("area " + extCode + " not found on database"));
					}
					else
					{
						geo = (GeographicArea)areaList.get(0);
					}
				}
		    	// 6. Try to fetch a time id for the current time code. If not found create a new time_period entry.
				TimePeriod tim = singleTim;
				if (!singletime){
					String timeCode = sdp.getTimePeriodCode();
					List timeList =  em.createQuery("SELECT t FROM TimePeriod t WHERE t.name = :tcode",TimePeriod.class).setParameter("tcode", timeCode).getResultList();

					//	logger.info("timelist size = " + timeList.size());
					if (timeList.isEmpty()){
						tim = new TimePeriod();
						tim.setName(timeCode);
	        			tim.setStartDate(thelp.getStartDate(timeCode));
	        			tim.setEndDate(thelp.getEndDate(timeCode));
						tim.setTimeTypeBean(em.find(TimeType.class, "YEAR"));
						if (sdp.getTimeType().equalsIgnoreCase("QUARTER"))
						{
							tim.setTimeTypeBean(em.find(TimeType.class, "QUARTER"));
						}
						if (sdp.getTimeType().equalsIgnoreCase("MONTH"))
						{
							tim.setTimeTypeBean(em.find(TimeType.class, "MONTH"));
						}
						em.persist(tim);
					}
					else
					{
						tim = (TimePeriod)timeList.get(0);
					}
				}

		    //	7. Try to find a population record for the current area / time combo, if not found create a new one for it
				PopulationPK ppk = new PopulationPK();
				ppk.setGeographicAreaId(geo.getGeographicAreaId());
				ppk.setTimePeriodId(tim.getTimePeriodId());
				Population pop = em.find(Population.class,ppk);
			//	logger.info("population found");
				if (pop == null){
					   pop = new Population();
					   pop.setGeographicArea(geo);
					   pop.setTimePeriod(tim);
					   pop.setGeographicAreaExtCode(extCode);
					   em.persist(pop);
					}
		    //	8. we should now have all the required ids populated and can do a "persist" on the data.
				
				dp.setPopulation(pop);
				dp.setVariable(var);
			//	logger.info("DSID = " + dp.getDimensionalDataSet().getDimensionalDataSetId());
			//	logger.info("AREA = " + dp.getPopulation().getGeographicArea().getGeographicAreaId());
			//	logger.info("TIME = " + dp.getPopulation().getTimePeriod().getTimePeriodId());
			//	logger.info("VARI = " + dp.getVariable().getVariableId());
				em.persist(dp);
				recct = recct + 1;
				
				if (recct % chunksize == 0){
					logger.info("saving chunk, record count = " +recct);
					em.flush();
					em.clear();
				}
				
	//			Logger.info("Saving record number " + recct);
			}
		} catch (PersistenceException e) {
			logger.info("variable name = " + variableName);
			logger.error("Database error: " + e.getMessage());
			throw new GLLoadException("Database error: " + e.getMessage());
		} catch (DatabaseException e) {
			logger.error("Database error: " + e.getMessage());
			logger.info("variable name = " + variableName);
			throw new GLLoadException("Database error: " + e.getMessage());
		} catch (CSVValidationException ve) {
			throw ve;
		} catch (Exception e) {
			logger.error(String.format("Load to Target failed " + e.getMessage() ));
			throw new GLLoadException(String.format("Load to Target failed " + e.getMessage()));

		}
        return recct;
	}

    private void setSameTypeAndDomainForAllRecords(EntityManager em, StageDimensionalDataPoint isp) {
        String utype = isp.getUnitTypeEng();
        if (utype!= null && utype.trim().length() > 0){
UnitType ut = em.find(UnitType.class, utype);
            if (ut == null){
                ut = new UnitType();
                ut.setUnitType(utype);
                em.persist(ut);
            }
        }
        String vdomain = isp.getValueDomainEng();
        if (vdomain!= null && vdomain.trim().length() > 0){
ValueDomain vd = em.find(ValueDomain.class, vdomain);
            if (vd == null){
                vd = new ValueDomain();
                vd.setValueDomain(vdomain);
                em.persist(vd);
            }
        }
    }

}
