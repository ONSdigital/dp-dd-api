package services;

import exceptions.CSVValidationException;
import exceptions.GLLoadException;
import models.*;
import play.Logger;

import javax.persistence.EntityManager;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * The Class CSVGenerator.
 */
public class CSVGenerator {
    public static final byte[] UTF8_BOM = new byte[]{-17, -69, -65}; // Byte order marker helpful to Excel
    /**
     * The _logger.
     */
    private static final Logger.ALogger logger = Logger.of(CSVGenerator.class);
    protected static final String COPYRIGHT = "(C) Crown Copyright";
    protected static final String DOUBLE_QUOTE = "\"";
    protected static final Character COMMA = ',';
    protected static final String GEOGRAPHIC_ID = "Geographic ID";
    protected static final String GEOGRAPHIC_AREA = "Geographic Area";
    protected static final SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yy");
    //    protected CsvWriter output;
    protected Map<Long, Integer> offsetForDimItemSetId = new HashMap<Long, Integer>();
    protected boolean isGeogSig = true;
    protected String timeDimensionTitle = null;
    String outFile;
    Generate g1;

    public CSVGenerator(){}

    public CSVGenerator(String outfile, Generate gen) {
        this.g1 = gen;
        this.outFile = outfile;
    }


    public void run(EntityManager em, DimensionalDataSet dds, List<DimensionFilter> dimensionFilters, String outputFilePath) {
        logger.info(String.format("CSV Generation started for DDS Id: %s.", dds.getDimensionalDataSetId()));
        TimeZone tz = TimeZone.getTimeZone("Europe/London");
        TimeZone.setDefault(tz);
        try {

            buildCSV(dds, dimensionFilters, outputFilePath);

            String timeStamp = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date());
            dds.setModified(timeStamp);
            dds.setValidationException("");
            dds.setLoadException("");
            dds.setValidationMessage("Success");
            dds.setStatus("5-Generate-OK");
            logger.info(String.format("CSV Generation completed successfully for DDS Id: %s.", dds.getDimensionalDataSetId()));
        } catch (CSVValidationException validationException) {
            dds.setStatus("5-Generate-Failed");
            dds.setValidationMessage(validationException.getMessage());
            dds.setValidationException(validationException.getLocalizedMessage());
            logger.info(String.format("CSV Generation failed for DDS Id: %s : %s", dds.getDimensionalDataSetId(), validationException));
            g1.setStatus(String.format("CSV Generation failed for DDS Id: %s : %s", dds.getDimensionalDataSetId(), validationException));
        } catch (GLLoadException loadException) {
            dds.setStatus("5-Generate-Failed");
            dds.setValidationException(loadException.getMessage());
            dds.setLoadException(loadException.getMessage());
            logger.info(String.format("CSV Generation failed for DDS Id: %s : %s", dds.getDimensionalDataSetId(), loadException));
            g1.setStatus(String.format("CSV Generation failed for DDS Id: %s : %s", dds.getDimensionalDataSetId(), loadException));
        } finally {
            em.merge(dds);
        }
    }

    public void buildCSV(DimensionalDataSet dds, List<DimensionFilter> dimensionFilters, String outputFilePath) {
        try {
            FileOutputStream out = new FileOutputStream(new File(outputFilePath));
            out.write(UTF8_BOM);

            CsvWriter csvWriter = new CsvWriter(new PrintWriter(new OutputStreamWriter(out, "UTF-8")));
            //		writeHeader(results);
            List<DimensionalDataPoint> filteredData = filterData(dds.getDimensionalDataPoints(), dimensionFilters);
            filteredData.forEach(dp -> writeData(csvWriter, dp));

            csvWriter.close();
        } catch (IOException e) {
            logger.error("Failed to create the CSV: ", e);
            throw new GLLoadException("Failed to write the output CSV: ", e);
        } catch (CSVValidationException ve) {
            ve.printStackTrace();
            throw ve;
        } catch (Exception e) {
            logger.error("Failed to save CSV file: ", e);
            e.printStackTrace();
            throw new GLLoadException("Failed to save CSV file due to " + e.getMessage(), e);
        }
    }


    public List<DimensionalDataPoint> filterData(List<DimensionalDataPoint> dataPoints, List<DimensionFilter> filters) {
        for(DimensionFilter filter: filters) {
          dataPoints.removeIf(dp -> dp.getVariable().getCategories().stream().noneMatch(c -> categoryIsFiltered(c, filter)));
        }
        return dataPoints;
    }

    private boolean categoryIsFiltered(Category category, DimensionFilter dimensionFilter) {
        return category.getConceptSystemBean().getConceptSystem().equals(dimensionFilter.getConcept())
                && category.getName().contains(dimensionFilter.getVariable());
    }

    private void writeData(CsvWriter csvWriter, DimensionalDataPoint result) {
        outputStandardDataForRecord(csvWriter, result);
        outputDimesionalDataForRecord(csvWriter, result);
        csvWriter.endRow();
    }

    private void outputStandardDataForRecord(CsvWriter csvWriter, DimensionalDataPoint result) {
        csvWriter.outputNum(result.getValue().toString());
        csvWriter.outputNum(result.getPopulation().getGeographicArea().getExtCode());
        csvWriter.outputNum(result.getPopulation().getTimePeriod().getName());
        csvWriter.outputNum(result.getPopulation().getTimePeriod().getTimeTypeBean().getTimeType());
    }

    private void outputDimesionalDataForRecord(CsvWriter csvWriter, DimensionalDataPoint result) {
        List<Category> categories = result.getVariable().getCategories();
        categories.sort((c, c1) -> c.getName().compareTo(c1.getName()));

        categories.forEach(category -> {
            String dimensionName = category.getConceptSystemBean().getConceptSystem();
                csvWriter.outputNum(dimensionName);
                csvWriter.outputNum(category.getName());

        });
    }



    public void writeHeader(CsvWriter csvWriter, List<DataDTO> results) {
        csvWriter.outputField("Area Code");
        csvWriter.outputField("Area Name");
        csvWriter.outputField("Area Type");
        csvWriter.outputField("Time Period");
        Boolean samearea = true;
        Boolean sametime = true;
        String areaCode = "START";
        String timeCode = "START";
        int i = 0;
        while (samearea && sametime && i < results.size()) {
            DataDTO curData = (DataDTO) (results.get(i));
            String heading = curData.getVariableName();
            //	logger.info("heading = " + heading);
            String nextAreaCode = curData.getExtCode();
            //		logger.info("areaCode = " + nextAreaCode);
            String nextTimeCode = curData.getTimeName();
            //	logger.info("timeCode = " + nextTimeCode);
            if (areaCode.equals("START")) {
                areaCode = nextAreaCode;
            }
            if (timeCode.equals("START")) {
                timeCode = nextTimeCode;
            }
            if (!areaCode.equals(nextAreaCode)) {
                samearea = false;
            }
            if (!timeCode.equals(nextTimeCode)) {
                sametime = false;
            }
            if (sametime && samearea) {
                csvWriter.outputField(heading);
            }
            i++;
        }
        csvWriter.endRow();
    }


    protected class CsvWriter extends PrintWriter {
        private boolean firstColumn = true;

        public CsvWriter(PrintWriter output) {
            super(output);
        }

        public void outputField(String contents) {
            if (contents.length() > 0) {
                contents = DOUBLE_QUOTE + contents + DOUBLE_QUOTE;
            }
            if (!firstColumn) {
                print(COMMA);
            }
            firstColumn = false;
            print(contents);
        }

        public void outputNum(String contents) {
            if (!firstColumn) {
                print(COMMA);
            }
            firstColumn = false;
            print(contents);
        }

        public void endRow() {
            firstColumn = true;
            println();
        }
    }
}
