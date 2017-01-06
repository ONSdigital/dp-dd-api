package services;

import au.com.bytecode.opencsv.CSVParser;
import exceptions.CSVValidationException;
import exceptions.GLLoadException;
import models.Dataset;
import play.Logger;
import play.db.jpa.Transactional;
import uk.co.onsdigital.discovery.model.Category;
import uk.co.onsdigital.discovery.model.ConceptSystem;
import uk.co.onsdigital.discovery.model.DimensionalDataPoint;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;
import uk.co.onsdigital.discovery.model.GeographicArea;
import uk.co.onsdigital.discovery.model.Population;
import uk.co.onsdigital.discovery.model.PopulationPK;
import uk.co.onsdigital.discovery.model.TimePeriod;
import uk.co.onsdigital.discovery.model.TimeType;
import uk.co.onsdigital.discovery.model.UnitType;
import uk.co.onsdigital.discovery.model.ValueDomain;
import uk.co.onsdigital.discovery.model.Variable;
import utils.TimeHelper;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;


public class InputCSVParser {

    private final long jobId;
    private final long busArea;
    private final String busAreaName;
    private Dataset dataset;
    private String filename;
    private File inFile;
    /**
     * The _logger.
     */
    private static final Logger.ALogger logger = Logger.of(InputCSVParser.class);
    /**
     * The Constant END_OF_FILE.
     */
    static final String END_OF_FILE = "*********";
    private static final int TIME_DIMENSION_ITEM_INDEX = 17;
    private static final int GEOG_AREA_CODE_INDEX = 14;
    private static final int CSV_DIM_ITEM_COLUMN_COUNT = 8;
    private static final int CSV_MIN_COLUMN_COUNT = 43;
    private static final int ATTR_STAT_UNIT_ENGLISH = 2;
    private static final int ATTR_STAT_UNIT_WELSH = 3;
    private static final int ATTR_MEASURE_TYPE_ENGLISH = 4;
    private static final int ATTR_MEASURE_TYPE_WELSH = 5;
    private static final int ATTR_UNIT_MULTIPLIER = 9;
    private static final int ATTR_MEASURE_UNIT_ENGLISH = 10;
    private static final int ATTR_MEASURE_UNIT_WELSH = 11;
    private static final String ALLOWED_ATTR_CHARACTERS = "^[^,\"^]*";
    private static final String ALLOWED_ATTR_ERROR_MSG = "Attributes must not contain characters \" ^ , ";

    TimeHelper timeHelper = new TimeHelper();

    public InputCSVParser(Dataset ds, File file) {
        this.jobId = 0;
        this.busArea = 0;
        this.busAreaName = "Global";
        this.inFile = file;
        this.filename = "object";
        this.dataset = ds;
    }

    public InputCSVParser() {
        this.jobId = 0;
        this.busArea = 0;
        this.busAreaName = "Global";
    }

    public void run(EntityManager em, DimensionalDataSet dds, File inFile) {
        String resourceId = dds.getDataResourceBean().getDataResource();
        logger.info(String.format("File loading started for dataset Id: %s.", resourceId));
        TimeZone tz = TimeZone.getTimeZone("Europe/London");
        TimeZone.setDefault(tz);
        try {
            String timeStamp = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date());
            dds.setModified(timeStamp);
            dds.setValidationException("");
            dds.setLoadException("");
            dds.setValidationMessage("Success");

            parseCSV(em, dds, inFile);


//			this.dataset.setStatus("Loaded to staging");
            dds.setStatus("1-Staging-OK");
            logger.info(String.format("Observations successfully loaded for dataset %s.", resourceId));

        } catch (CSVValidationException validationException) {
//			this.dataset.setStatus("Input file failed validation");
            dds.setStatus("1-Staging-Failed");
            dds.setValidationMessage(validationException.getMessage());
            dds.setValidationException(validationException.getLocalizedMessage());
            logger.info(String.format("Observations file failed validation for dataset %s : %s", resourceId, validationException));
        } catch (GLLoadException loadException) {
//			this.dataset.setStatus("Loading of observations failed");
            dds.setStatus("1-Staging-Failed");
            dds.setValidationException(loadException.getMessage());
            dds.setLoadException(loadException.getMessage());
            logger.info(String.format("Loading of observations into staging was not successful for dataset %s : %s", resourceId, loadException));
        } finally {
            em.merge(dds);
        }
    }

    @Transactional
    public void parseCSV(EntityManager em, DimensionalDataSet dds, File inputFile) {

        // New format
        // Column 0/A Observation value (number) --observation
        // Column 1/B Data marking String --observation
        // Column 2/C Value domain  (previously measure type)
        // Column 3/D Observation type --observation
        // Column 4/E Observation type value --observation
        // Column 5/F Unit of measure --attribute
        // Column 6/G Geographic area
        // Column 7/H Time --dimension item (CL_TIME)
        // Column 8/I time type --classification item type (Year, Month, Quarter)
        // Column 9/J CDID
        // Column 10/K Dimension ID 1
        // Column 11/L Dimension value ID 1
        // Column 12/M Dimension ID 2
        // Column 13/N Dimension value ID 2

        logger.info("CSV parsing started at:" + new Date());
        String rowData[];
        String firstCellVal = null;
        BufferedReader csvReader = getCSVBufferedReader(inputFile);
        CSVParser csvParser = new CSVParser();
        if (csvReader != null) {
            try {
                csvReader.readLine();
                while (csvReader.ready() && (rowData = csvParser.parseLine(csvReader.readLine())) != null) {
                    firstCellVal = rowData[0];

                    parseRowdataDirectToTables(em, rowData, dds);

                }
                if (!END_OF_FILE.equals(firstCellVal)) {
                    throw new GLLoadException("End of File record not found");
                }
            } catch (IOException e) {
                logger.error("Failed to read the input CSV: ", e);
                throw new GLLoadException("Failed to read/write the input/output CSV: ", e);
            } catch (CSVValidationException ve) {
                throw ve;
            } catch (Exception e) {
                logger.error("Failed to load CSV file: ", e);
                throw new GLLoadException("Failed to load CSV file due to " + e.getMessage(), e);
            } finally {
                closeCSVReader(csvReader);
            }
            logger.info("CSV parsing completed at:" + new Date());
        }
    }


    public void parseRowdataDirectToTables(EntityManager em, String[] rowData, final DimensionalDataSet dds) {

        DimensionalDataPoint ddp = new DimensionalDataPoint();
        ddp.setDimensionalDataSet(dds);

        // todo - how to deal with categories more permanently
        List<Category> categories = createCategories(em, rowData, rowData.length, ddp);
        categories.forEach(category -> {
            em.persist(category);
            dds.addReferencedConceptSystem(category.getConceptSystemBean());
        });


        String observationValue = getStringValue(rowData[0], "0");
        if (END_OF_FILE.equals(observationValue)) {
            logger.info("Found end-of-file marker");
            return;
        }
        ddp.setValue(new BigDecimal(observationValue));

        String dataMarking = getStringValue(rowData[1], "");
        ddp.setDataMarking(dataMarking);


        String valueDomainName = getStringValue(rowData[2], "");
        ValueDomain valueDomain = em.find(ValueDomain.class, valueDomainName);
        if (valueDomain == null) {
            valueDomain = new ValueDomain(valueDomainName);
            em.persist(valueDomain);
        }


        String unitTypeEng = getStringValue(rowData[5], "");
        UnitType unitType = em.find(UnitType.class, unitTypeEng);
        if (unitType == null) {
            unitType = new UnitType(unitTypeEng);
            em.persist(unitType);  // todo fix cascade
        }

        String variableName = categories.stream().map(category -> category.getName()).collect(Collectors.joining(" | "));
        Variable variable;
        try {
            variable = em.createQuery("SELECT v FROM Variable v WHERE v.name = :name", Variable.class).setParameter("name", variableName).getSingleResult();
        } catch (NoResultException e) {
            variable = new Variable(variableName);
            variable.setUnitTypeBean(unitType);
            variable.setValueDomainBean(valueDomain);
            variable.setCategories(categories);
            em.persist(variable);  // todo fix cascade
        }

        ddp.setVariable(variable);

        //	todo - use these??
//        String observationType = getStringValue(rowData[6], "");
//		String observationTypeValue = getStringValue(rowData[8], "");
//		String timePeriodNameEng = getStringValue(rowData[18], "");

        String geographicCode = getStringValue(rowData[6], "K02000001");
        GeographicArea geographicArea = em.createQuery("SELECT a FROM GeographicArea a WHERE a.extCode = :ecode", GeographicArea.class).setParameter("ecode", geographicCode).getSingleResult();


        String timeClItemCode = getStringValue(rowData[7], "");
        List<TimePeriod> timePeriods = em.createQuery("SELECT t FROM TimePeriod t WHERE t.name = :tcode", TimePeriod.class).setParameter("tcode", timeClItemCode).getResultList();
        if (timePeriods.isEmpty()) {
            // todo what about multiple returns?  is it possible? doesn't appear to be a constraint on name.
            String timeType = getStringValue(rowData[8], "");
            timePeriods.add(createTimePeriod(em, timeClItemCode, timeType));
        }

        PopulationPK populationPK = new PopulationPK();
        populationPK.setGeographicAreaId(geographicArea.getGeographicAreaId());
        populationPK.setTimePeriodId(timePeriods.get(0).getTimePeriodId());

        Population population = em.find(Population.class, populationPK);
        if (population == null) {
            population = createPopulation(em, geographicArea, timePeriods.get(0));
        }
        ddp.setPopulation(population);

        em.persist(ddp);
    }


    private String getStringValue(String rowDatum, String defaultValue) {
        return rowDatum.trim().isEmpty() ? defaultValue : rowDatum.trim();
    }


    private Population createPopulation(EntityManager em, GeographicArea geographicArea, TimePeriod timePeriod) {
        Population population = new Population();
        population.setGeographicArea(geographicArea);
        population.setTimePeriod(timePeriod);
        population.setGeographicAreaExtCode(geographicArea.getExtCode());
        em.persist(population);
        return population;
    }

    private TimePeriod createTimePeriod(EntityManager em, String timePeriodCode, String timeType) {
        TimePeriod timePeriod;
        timePeriod = new TimePeriod();
        timePeriod.setName(timePeriodCode);
        timePeriod.setStartDate(timeHelper.getStartDate(timePeriodCode));
        timePeriod.setEndDate(timeHelper.getEndDate(timePeriodCode));
        if (timeType.equalsIgnoreCase("QUARTER")) {
            timePeriod.setTimeTypeBean(em.find(TimeType.class, "QUARTER"));
        } else if (timeType.equalsIgnoreCase("MONTH")) {
            timePeriod.setTimeTypeBean(em.find(TimeType.class, "MONTH"));
        } else {
            timePeriod.setTimeTypeBean(em.find(TimeType.class, "YEAR"));
        }
        em.persist(timePeriod);
        return timePeriod;
    }

    private ArrayList<Category> createCategories(EntityManager em, String[] rowData, int rowLength, DimensionalDataPoint ddp) {
        // todo - would be better to chunk this into 8 field blocks right up front, each representing a dimension
        int rowSub = 10;  // first dimension start
        ArrayList<Category> categories = new ArrayList<>();

        while (rowSub < rowLength) {
            String conceptName = "";
            String categoryName = "";

            if (rowData[rowSub] != null && rowData[rowSub].trim().length() > 0) {
                conceptName = (rowData[rowSub].trim());
            }
            rowSub++;
            if (rowData[rowSub] != null && rowData[rowSub].trim().length() > 0) {
                categoryName = rowData[rowSub].trim();
            }
            rowSub++;
            categories.add(createCategory(em, conceptName, categoryName));
        }
        return categories;
    }

    private Category createCategory(EntityManager em, String conceptName, String categoryName) {
        try {
            return em.createQuery("SELECT c FROM Category c WHERE c.name = :name AND c.conceptSystemBean.conceptSystem = :conceptSystem", Category.class)
                    .setParameter("name", categoryName)
                    .setParameter("conceptSystem", conceptName)
                    .getSingleResult();
        } catch (NoResultException e) {
            Category category = new Category(categoryName);

            ConceptSystem conceptSystem = em.find(ConceptSystem.class, conceptName);
            if (conceptSystem == null) {
                conceptSystem = new ConceptSystem(conceptName);
                em.persist(conceptSystem);
            }
            category.setConceptSystemBean(conceptSystem);
            return category;
        }

    }

    /**
     * Validate the row data passed.
     *
     * @param rowData  the row data array
     * @param rowCount
     */
    private void validate(String[] rowData, long rowCount) throws CSVValidationException {
        // Check if number of columns in CSV file does not conform to specification (is not 39 or additional of 8
        // thereon i.e 47, 55, 63 etc)
        if (rowData.length < CSV_MIN_COLUMN_COUNT || ((rowData.length - CSV_MIN_COLUMN_COUNT) % CSV_DIM_ITEM_COLUMN_COUNT != 0)) {
            throw new CSVValidationException(String.format("File badly formed. Row : %d.", rowCount));
        }
        // Validate attributes
        validateAttribute(rowData[ATTR_STAT_UNIT_ENGLISH], rowCount);
        validateAttribute(rowData[ATTR_STAT_UNIT_WELSH], rowCount);
        validateAttribute(rowData[ATTR_MEASURE_TYPE_ENGLISH], rowCount);
        validateAttribute(rowData[ATTR_MEASURE_TYPE_WELSH], rowCount);
        validateAttribute(rowData[ATTR_UNIT_MULTIPLIER], rowCount);
        validateAttribute(rowData[ATTR_MEASURE_UNIT_ENGLISH], rowCount);
        validateAttribute(rowData[ATTR_MEASURE_UNIT_WELSH], rowCount);
        //logger.info("row number " + rowCount + " OK");
    }


    private void validateAttribute(String value, long rowCount) {
        // Check if attribute contains invalid characters (anything not in [A-Z], [a-z], [0-9], [-, _],[$,�,�])
        if (value != null && !value.trim().isEmpty() && !value.matches(ALLOWED_ATTR_CHARACTERS)) {
            throw new CSVValidationException(String.format("%s. Failed for the attribute '%s' at Row : %d.", ALLOWED_ATTR_ERROR_MSG, value, rowCount));
        }
    }


    public BufferedReader getCSVBufferedReader(File inFile) {
//		logger.info("getCSVBufferedReader() [" + fileName+"]");
        BufferedReader csvReader = null;
        try {
            csvReader = new BufferedReader(new InputStreamReader(new FileInputStream(inFile), "UTF-8"), 32768);
        } catch (IOException e) {
            logger.error("Failed to get the BufferedReader: ", e);
            throw new GLLoadException("Failed to get the BufferedReader: ", e);
        }
        return csvReader;
    }


    public void closeCSVReader(BufferedReader reader) {
        logger.info("closeCSVReader(BufferedReader) - > Closing the reader ...");
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                logger.error("Failed while closing the CSVReader: ", e);
            }
        }
    }

}
