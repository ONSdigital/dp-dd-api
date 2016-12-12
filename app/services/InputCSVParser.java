package services;

import au.com.bytecode.opencsv.CSVParser;
import exceptions.CSVValidationException;
import exceptions.GLLoadException;
import models.Dataset;
import play.Logger;
import play.db.jpa.Transactional;
import uk.co.onsdigital.discovery.model.*;
import utils.TimeHelper;

import javax.persistence.EntityManager;
import java.io.*;
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
        // String
        // Column 0 Observation value (number) --observation
        // Column 1 Data marking String --observation
        // Column 2 Statistical Unit Eng value --attribute
        // Column 3 Statistical Unit welsh value --attribute
        // Column 4 Meas type eng --attribute
        // Column 5 Meas type welsh --attribute
        // Column 6-->obs type code --observation
        // Column 7-->empty --ignore
        // Column 8-->obs type val --observation
        // Column 9-->unit mult scalar(english value) --attribute
        // Column 10-->unit of meas eng --attribute
        // Column 11-->unit of meas welsh --attribute
        // Column 12-->confidentiality --observation
        // Column 13-->empty --ignore
        // Column 14-->geog --geog_item
        // Column 15-->empt --ignore
        // Column 16-->empt --ignore
        // Column 17-->Time Dim Item ID --dimension item (CL_TIME)
        // Column 18-->Time Dim Item Label Eng --dimension item
        // Column 19-->Time Dim Item Label Welsh --dimension item
        // Column 20-->time type --classification item type (Year, Month, Quarter)
        // Column 21-->emp --ignore
        // Column 22-->Statistical Population ID -- segment
        // Column 23-->Statistical Population Label Eng -- segment
        // Column 24-->Statistical Population Label welsh -- segment
        // Column 25-->CDID --dim_item_set
        // Column 26-->CDID Description --dim_item_set
        // Column 27-->empt --ignore
        // Column 28-->empt --ignore
        // Column 29-->empt --ignore
        // Column 30-->empt --ignore
        // Column 31-->empt --ignore
        // Column 32-->empt --ignore
        // Column 33-->empt --ignore
        // Column 34-->empt --ignore
        // Column 35-->Dim ID 1
        // Column 36-->Dimension Label Eng 1
        // Column 37-->Dimension Label Welsh 1
        // Column 38-->Dim Item ID 1
        // Column 39-->Dimension Item Label Eng 1
        // Column 40-->Dimension Item Label Cym 1
        // Column 41-->Is Total 1
        // Column 42-->Is Subtotal 1
        // 35-42 cols will be repeated if there are more dimensions
        logger.info("CSV parsing started at:" + new Date());
        String rowData[] = null;
        String firstCellVal = null;
        BufferedReader csvReader = getCSVBufferedReader(inputFile);
        CSVParser csvParser = new CSVParser();
        if (csvReader != null) {
            try {
                csvReader.readLine();
                while (csvReader.ready() && (rowData = csvParser.parseLine(csvReader.readLine())) != null) {

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


    public void parseRowdataDirectToTables(EntityManager em, String[] rowData, DimensionalDataSet dds) {

        DimensionalDataPoint ddp = new DimensionalDataPoint();
        ddp.setDimensionalDataSet(dds);

        // todo - how to deal with categories more permanently
        List<Category> categories = createCategories(em, rowData, rowData.length, ddp);
        categories.forEach(category -> em.persist(category));

        String observationValue = getStringValue(rowData[0], "");
        ddp.setValue(new BigDecimal(observationValue));

        String dataMarking = getStringValue(rowData[1], "");
        ddp.setDataMarking(dataMarking);

        String unitTypeEng = getStringValue(rowData[2], "Persons");  // todo remove the default of 'Persons'
        UnitType unitType = em.find(UnitType.class, unitTypeEng);
        if (unitType == null) {
            unitType = new UnitType(unitTypeEng);
            em.persist(unitType);  // todo fix cascade
        }

        String valueDomainName = getStringValue(rowData[4], "");
        ValueDomain valueDomain = em.find(ValueDomain.class, valueDomainName);
        if (valueDomain == null) {
            valueDomain = new ValueDomain(valueDomainName);
            em.persist(valueDomain);
        }

        String variableName = categories.stream().map(category -> category.getName()).collect(Collectors.joining(" | "));
        Variable variable = new Variable(variableName);  // todo - if doesn't exist
        variable.setUnitTypeBean(unitType);
        variable.setValueDomainBean(valueDomain);
        em.persist(variable);  // todo fix cascade

        ddp.setVariable(variable);

        //	todo - use these??
//        String observationType = getStringValue(rowData[6], "");
//		String observationTypeValue = getStringValue(rowData[8], "");
//		String timePeriodNameEng = getStringValue(rowData[18], "");

        String geographicCode = getStringValue(rowData[14], "K02000001");
        GeographicArea geographicArea = em.createQuery("SELECT a FROM GeographicArea a WHERE a.extCode = :ecode", GeographicArea.class).setParameter("ecode", geographicCode).getSingleResult();


        String timeClItemCode = getStringValue(rowData[17], "");
        List<TimePeriod> timePeriods = em.createQuery("SELECT t FROM TimePeriod t WHERE t.name = :tcode", TimePeriod.class).setParameter("tcode", timeClItemCode).getResultList();
        if (timePeriods.isEmpty()) {
            // todo what about multiple returns?  is it possible? doesn't appear to be a constraint on name.
            String timeType = getStringValue(rowData[20], "");
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
        int rowSub = 35;  // first dimension start
        ArrayList<Category> categories = new ArrayList<>();

        while (rowSub < rowLength) {

            String conceptName = "";
            String categoryName = "";

            rowSub = rowSub + 1;

            if (rowData[rowSub] != null && rowData[rowSub].trim().length() > 0) {
                conceptName = (rowData[rowSub].trim());
            }
            rowSub = rowSub + 3;
            if (rowData[rowSub] != null && rowData[rowSub].trim().length() > 0) {
                categoryName = rowData[rowSub].trim();
            }
            rowSub = rowSub + 4;
            categories.add(createCategory(em, conceptName, categoryName));
        }
        return categories;
    }

    private Category createCategory(EntityManager em, String conceptName, String categoryName) {
        Category category = new Category(categoryName); // todo - should this always be creating a new category - hmmm, probably not!

        ConceptSystem conceptSystem = em.find(ConceptSystem.class, conceptName);
        if (conceptSystem == null) {
            conceptSystem = new ConceptSystem(conceptName);
            em.persist(conceptSystem);
        }
        category.setConceptSystemBean(conceptSystem);
        return category;
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
