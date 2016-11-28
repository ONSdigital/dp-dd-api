package services;

import exceptions.CSVValidationException;
import exceptions.GLLoadException;
import models.*;
import org.eclipse.persistence.exceptions.DatabaseException;
import play.Logger;
import utils.TimeHelper;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class LoadToTarget {
    private static final Logger.ALogger logger = Logger.of(LoadToTarget.class);

    TimeHelper timeHelper = new TimeHelper();

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
            logger.info(String.format("Loading to target not successful - " + validationException.getMessage()));
        } catch (GLLoadException loadException) {
            ds.setStatus("2-Target-Failed");
            ds.setValidationException(loadException.getMessage());
            ds.setLoadException(loadException.getMessage());
            logger.info(String.format("Loading to target not successful - " + loadException.getMessage()));
        } finally {
            em.merge(ds);
            em.flush();
            em.clear();
        }

    }

    private void createDefaultUnitTypes(EntityManager em) {
        UnitType ut = em.find(UnitType.class, "Persons");
        if (ut == null) {
            ut = new UnitType();
            ut.setUnitType("Persons");
            em.persist(ut);
        }
        ValueDomain vd = em.find(ValueDomain.class, "Count");
        if (vd == null) {
            vd = new ValueDomain();
            vd.setValueDomain("Count");
            em.persist(vd);
        }
        TimeType ttq = em.find(TimeType.class, "QUARTER");
        if (ttq == null) {
            ttq = new TimeType();
            ttq.setTimeType("QUARTER");
            em.persist(ttq);
        }
        TimeType ttm = em.find(TimeType.class, "MONTH");
        if (ttm == null) {
            ttm = new TimeType();
            ttm.setTimeType("MONTH");
            em.persist(ttm);
        }
        TimeType tty = em.find(TimeType.class, "YEAR");
        if (tty == null) {
            tty = new TimeType();
            tty.setTimeType("YEAR");
            em.persist(tty);
        }
    }

    private Long stageToTarget(EntityManager em, DimensionalDataSet ds) {
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
        Long recct = 0L;
        String variableName = "";

        try {
            createDefaultUnitTypes(em);

            List<StageDimensionalDataPoint> results = em.createQuery("SELECT s FROM StageDimensionalDataPoint s WHERE s.dimensionalDataSetId = " + ds.getDimensionalDataSetId() + " order by s.geographicArea, s.timePeriodCode", StageDimensionalDataPoint.class).getResultList();
            logger.info("records found = " + results.size());

            for (StageDimensionalDataPoint sdp : results) {
                setSameTypeAndDomainForAllRecords(em, sdp);

                //	1. Create a skeleton dimensional data point record in memory - NOT HERE - DO IT AT WHEN YOU NEED IT IN SECTION 8!

                //	2. Fetch the staged category records for current observation seq id
                List<StageCategory> stageCategoryList = sdp.getStageCategories();
                stageCategoryList.sort(Comparator.comparingInt(c -> c.getId().getDimensionNumber()));

                //	3. For each staged category record
                ArrayList<Category> variableCategoryList = new ArrayList<Category>();
                for (StageCategory stageCategory : stageCategoryList) {

                    //	3.1. Try to fetch the concept id, if not found create new concept
                    String conceptName = stageCategory.getConceptSystemLabelEng();

                    //	3.2. Try to fetch the category id, if not found create new category
                    List<Category> catList = em.createQuery("SELECT c FROM Category c WHERE c.name = :cname", Category.class).setParameter("cname", stageCategory.getCategoryNameEng()).getResultList();

                    if (catList.isEmpty()) {
                        variableCategoryList.add(createCategory(em, stageCategory, conceptName));
                    } else {
                        variableCategoryList.add(catList.get(0));
                    }
                    //  	4. If no new items created in 3.1 and 3.2, fetch the variable id for the combo, else create a variable and a set of variablecategory records for it
                }

                variableName = variableCategoryList.stream().map(i -> i.getName()).collect(Collectors.joining(" | "));

                List varList = em.createQuery("SELECT v FROM Variable v WHERE v.name = :vname", Variable.class).setParameter("vname", variableName).getResultList();
                Variable variable = null;
                if (varList.isEmpty()) {
                    variable = createVariable(em, variableName, variableCategoryList);
                } else {
                    variable = (Variable) varList.get(0);
                }

                // 	5. Try to fetch the geographic area id by extcode, if not found create new geographic area and derive area and level types via lookup on first three digits of extcode
                GeographicArea geographicArea = em.createQuery("SELECT a FROM GeographicArea a WHERE a.extCode = :ecode", GeographicArea.class).setParameter("ecode", sdp.getGeographicArea()).getSingleResult();

                // 6. Try to fetch a time id for the current time code. If not found create a new time_period entry.
                TimePeriod timePeriod;
                if(em.createQuery("SELECT t FROM TimePeriod t WHERE t.name = :tcode", TimePeriod.class).setParameter("tcode", sdp.getTimePeriodCode()).getMaxResults() == 1) {
                    timePeriod = em.createQuery("SELECT t FROM TimePeriod t WHERE t.name = :tcode", TimePeriod.class).setParameter("tcode", sdp.getTimePeriodCode()).getSingleResult();
                } else {
                    timePeriod = createTimePeriod(em, sdp);
                }

                //	7. Try to find a population record for the current area / time combo, if not found create a new one for it
                PopulationPK ppk = new PopulationPK();
                ppk.setGeographicAreaId(geographicArea.getGeographicAreaId());
                ppk.setTimePeriodId(timePeriod.getTimePeriodId());

                Population population = em.find(Population.class, ppk);
                if (population == null) {
                    population = createPopulation(em, geographicArea, timePeriod);
                }

                //	8. we should now have all the required ids populated and can do a "persist" on the data.
                em.persist(new DimensionalDataPoint(ds, sdp.getValue(), population, variable));

                if (++recct % 1000 == 0) {  // saving every 1000 lines
                    logger.info("saving chunk, record count = " + recct);
                    em.flush();
                    em.clear();
                }
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
            logger.error(String.format("Load to Target failed " + e.getMessage()));
            throw new GLLoadException(String.format("Load to Target failed " + e.getMessage()));

        }
        return recct;
    }

    private Population createPopulation(EntityManager em, GeographicArea geographicArea, TimePeriod timePeriod) {
        Population population;
        population = new Population();
        population.setGeographicArea(geographicArea);
        population.setTimePeriod(timePeriod);
        population.setGeographicAreaExtCode(geographicArea.getExtCode());
        em.persist(population);
        return population;
    }

    private Variable createVariable(EntityManager em, String variableName, ArrayList<Category> variableCategoryList) {
        Variable variable;
        variable = new Variable();
        variable.setName(variableName);
        variable.setUnitTypeBean(em.find(UnitType.class, "Persons"));
        variable.setValueDomainBean(em.find(ValueDomain.class, "Count"));
        variable.setCategories(variableCategoryList);
        em.persist(variable);
        return variable;
    }

    private Category createCategory(EntityManager em, StageCategory stageCategory, String conceptName) {
        Category cat = new Category();
        cat.setName(stageCategory.getCategoryNameEng());
        ConceptSystem consys = em.find(ConceptSystem.class, conceptName);
        if (consys == null) {
            consys = new ConceptSystem();
            consys.setConceptSystem(conceptName);
            em.persist(consys);
        }
        cat.setConceptSystemBean(consys);
        em.persist(cat);
        return cat;
    }

    private TimePeriod createTimePeriod(EntityManager em, StageDimensionalDataPoint sdp) {
        TimePeriod timePeriod;
        timePeriod = new TimePeriod();
        timePeriod.setName(sdp.getTimePeriodCode());
        timePeriod.setStartDate(timeHelper.getStartDate(sdp.getTimePeriodCode()));
        timePeriod.setEndDate(timeHelper.getEndDate(sdp.getTimePeriodCode()));
        if (sdp.getTimeType().equalsIgnoreCase("QUARTER")) {
            timePeriod.setTimeTypeBean(em.find(TimeType.class, "QUARTER"));
        } else if (sdp.getTimeType().equalsIgnoreCase("MONTH")) {
            timePeriod.setTimeTypeBean(em.find(TimeType.class, "MONTH"));
        } else {
            timePeriod.setTimeTypeBean(em.find(TimeType.class, "YEAR"));
        }
        em.persist(timePeriod);
        return timePeriod;
    }

    private void setSameTypeAndDomainForAllRecords(EntityManager em, StageDimensionalDataPoint isp) {
        String utype = isp.getUnitTypeEng();
        if (utype != null && utype.trim().length() > 0) {
            UnitType ut = em.find(UnitType.class, utype);
            if (ut == null) {
                ut = new UnitType();
                ut.setUnitType(utype);
                em.persist(ut);
            }
        }
        String vdomain = isp.getValueDomainEng();
        if (vdomain != null && vdomain.trim().length() > 0) {
            ValueDomain vd = em.find(ValueDomain.class, vdomain);
            if (vd == null) {
                vd = new ValueDomain();
                vd.setValueDomain(vdomain);
                em.persist(vd);
            }
        }
    }

}
