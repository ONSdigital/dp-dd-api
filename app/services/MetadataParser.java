package services;

import exceptions.CSVValidationException;
import exceptions.GLLoadException;
import models.Metadata;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import play.Logger;
import uk.co.onsdigital.discovery.model.DataResource;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;

import javax.persistence.EntityManager;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

// see http://guidance.data.gov.uk/dcat_fields.html
public class MetadataParser implements Runnable {
	  private static final Logger.ALogger logger = Logger.of(MetadataParser.class);
	EntityManager em;
	String dsName;
	UUID ddsid;
	String jsonString;
	Metadata met1;
	
	public MetadataParser(Metadata met) {
		this.met1 = met;
		this.dsName = met.getResourceId();
	    this.jsonString = met.getJson();
	}
	
	public void runJPA(EntityManager em) {
		this.em = em;
		run();
	}
	
	@Override
	public void run() {
		logger.info(String.format("Metadata Loading started for dataset id " + dsName));
    	TimeZone tz = TimeZone.getTimeZone("Europe/London");
    	TimeZone.setDefault(tz);
		List <DataResource> dis = em.createQuery("SELECT d FROM DataResource d WHERE d.dataResource = :dsid",DataResource.class).setParameter("dsid", dsName).getResultList();
    	// Logger.info("size = " + dis.size());
    	DataResource drs = dis.get(0);
    	List <DimensionalDataSet> dimds = em.createQuery("SELECT d FROM DimensionalDataSet d WHERE d.dataResourceBean = :dsid",DimensionalDataSet.class).setParameter("dsid", drs).getResultList();
  //  	Logger.info("size2 = " + dimds.size());
    	ddsid = dimds.get(0).getDimensionalDataSetId();
	
		DimensionalDataSet ds = em.find(DimensionalDataSet.class, ddsid);
		try {
			String timeStamp = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date());
			ds.setModified(timeStamp);
			ds.setValidationException("");
			ds.setLoadException("");
			ds.setValidationMessage("Success");
			parseJSON(ds);
			ds.setModified(timeStamp);
			ds.setStatus("4-Metadata-OK");
			logger.info(String.format("JSON successfully loaded"));
			met1.setStatus(" JSON metadata successfully loaded");
		} catch (CSVValidationException validationException) {
			ds.setStatus("4-Metadata-Failed");
			ds.setValidationMessage(validationException.getMessage());
			ds.setValidationException(validationException.getLocalizedMessage());
			logger.info(String.format("Metadata file failed validation - " + validationException.getMessage() ));
			met1.setStatus(String.format("Metadata file failed validation - " + validationException.getMessage() ));
		} catch (GLLoadException loadException) {
			ds.setStatus("4-Metadata-Failed");
			ds.setValidationException(loadException.getMessage());
			ds.setLoadException(loadException.getMessage());
			logger.info(String.format("Loading of metadata was not successful - ",  loadException ));
			met1.setStatus(String.format("Loading of metadata was not successful - ",  loadException ));
		} finally {
			em.merge(ds);
			em.flush();
			em.clear();
		}
	}
	
	/*
	{
	    "title": "Zimbabwe Regional Geochemical Survey.",
	    "description": "During the period 1982-86 a team of geologists from the British Geological Survey ...",
	    "identifier": "9df8df51-63db-37a8-e044-0003ba9b0d98",
	    "landingPage" : null,
	    "issued": "2012-05-10",
	    "modified": "2012-05-10T21:04",
	    "language": ["en", "es", "ca"],
	    "keyword" : ["exploration", "geochemical-exploration", "geochemical-maps", "geochemistry", "geology", "nercddc", "regional-geology"],
	    "publisher": {
	        "name": "Geological Society",
	        "mbox": "info@gs.org"
	    },
	    "distribution": [{"accessURL": "http://www.bgs.ac.uk/gbase/geochemcd/home.html",
	                       "byteSize": null,
	                       "description": "Resource locator",
	                       "format": "text/html",
	                       "title": ""}]
	}
	*/
	
	
	private void parseJSON(DimensionalDataSet ds){
		try {
			JSONObject json = new JSONObject(jsonString);
		//	DimensionalDataSet ds = em.find(DimensionalDataSet.class, ddsid);
			// json text
			ds.setMetadata(jsonString);
			// title
			String s3URL = json.getString("s3URL");
			logger.info(String.format("s3URL = " + s3URL ));
			ds.setS3URL(s3URL);
			// description
			String description = json.getString("description");
			logger.info(String.format("Description = " + description ));
			ds.setDescription(description);
			// identifier
			String identifier = json.getString("identifier");
			logger.info(String.format("Identifier = " + identifier ));
			ds.setIdentifier(identifier);
			// landingpage
			String landingpage = null;
			if(!json.isNull("landingPage")) {
				landingpage = json.getString("landingPage");
			}
			logger.info(String.format("LandingPage = " + landingpage ));
			ds.setLandingpage(landingpage);	
			// issued
			String issued = json.getString("issued");
			logger.info(String.format("Issued = " + issued ));
			ds.setIssued(issued);
			// modified
			String modified = json.getString("modified");
			logger.info(String.format("Modified = " + modified));
			ds.setModified(modified);	
			// language
			StringBuffer sb = new StringBuffer("languages={");
		    JSONArray langArray = (JSONArray) json.get("language");
		    for (int i = 0; i < langArray.length(); i++){
		    	String lang = (String)langArray.get(i);
		    	sb.append(lang);
		    	if (i == langArray.length()-1){
		    		sb.append("}");
		    	}
		    	else
		    	{
		    		sb.append(",");
		    	}
		    
		    }
			logger.info(String.format("Languages = " + sb.toString() ));
		    ds.setLanguage(sb.toString());
			// keyword
			StringBuffer sb2 = new StringBuffer("keywords={");
		    JSONArray keyArray = (JSONArray) json.get("keyword");
		    for (int i = 0; i < keyArray.length(); i++){
		    	String key = (String)keyArray.get(i);
		    	sb2.append(key);
		    	if (i == keyArray.length()-1){
		    		sb2.append("}");
		    	}
		    	else
		    	{
		    		sb2.append(",");
		    	}
		    
		    }
			logger.info(String.format("Keywords = " + sb2.toString() ));
		    ds.setKeyword(sb2.toString());
		    // publisher
		    JSONObject pub = json.getJSONObject("publisher");
		    String name = pub.getString("name");
		    String mbox = pub.getString("mbox");
			String publisher = "publisher={name=" + name + ",mbox=" + mbox + "}";
			logger.info(String.format("Publisher = " + publisher));
			ds.setPublisher(publisher);	
		    // distribution
			JSONArray distArray = (JSONArray) json.get("distribution");
		    JSONObject dist1 = (JSONObject) distArray.get(0);
		    String daccessURL = dist1.getString("accessURL");
			String dbyteSize = null;
			if(!json.isNull("byteSize")) {
				dbyteSize = dist1.getString("byteSize");
			}
            String ddescription = dist1.getString("description");
            String dformat = dist1.getString("format");
            String dtitle = dist1.getString("title");
			String distribution = "distribution={accessURL=" + daccessURL + ",byteSize=" + dbyteSize +
					",description=" + ddescription + ",format=" + dformat + ",title=" + dtitle + "}";
			logger.info(String.format("Distribution = " + distribution));
			ds.setDistribution(distribution);
			em.persist(ds);
		} catch (JSONException e) {
			logger.error(String.format("JSON parse failed " + e.getMessage() ));
			throw new CSVValidationException(String.format("JSON parse failed " + e.getMessage()));

		}
	}

}
