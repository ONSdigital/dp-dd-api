package services;

import exceptions.GLLoadException;
import play.Logger;
import uk.co.onsdigital.discovery.model.Dimension;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;
import uk.co.onsdigital.discovery.model.HierarchyEntry;

import javax.persistence.EntityManager;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static services.InputCSVParser.END_OF_FILE;

public class InputCSVParserV3 {

    private static final Logger.ALogger logger = Logger.of(InputCSVParserV3.class);

    public InputCSVParserV3() {
    }

    public void parseRowdataDirectToTablesFromTriplets(EntityManager em, String[] rowData, final DimensionalDataSet dds) {

        String observation = getStringValue(rowData[0], "0");
        if (END_OF_FILE.equals(observation)) {
            logger.info("Found end-of-file marker");
            return;
        }

        for (int i = 1; i < rowData.length; i = i + 3) {
            String hierarchyId = rowData[i];
            String dimensionName = rowData[i + 1];
            String dimensionValue = rowData[i + 2];

            logger.debug("Creating dimension for hierarchyId: " + hierarchyId + " and dimensionName: " + dimensionName + " and dimensionValue: " + dimensionValue + " ....");

            Dimension dimension = new Dimension(dds.getId(), dimensionName, dimensionValue);


            if(!hierarchyId.isEmpty()) {
                HierarchyEntry hierarchyEntry = em.createQuery("SELECT he FROM HierarchyEntry he where he.hierarchyId = :id and he.code = :code", HierarchyEntry.class)
                        .setParameter("id", hierarchyId)
                        .setParameter("code", dimensionValue)
                        .getSingleResult();

                dimension.setHierarchyEntry(hierarchyEntry);
            }

            List<Dimension> existing = em.createQuery("SELECT dim FROM Dimension dim WHERE dim.dimensionalDataSetId = :ddsId AND dim.name = :name AND dim.value = :value", Dimension.class)
                    .setParameter("ddsId", dds.getId())
                    .setParameter("name", dimensionName)
                    .setParameter("value", dimensionValue)
                    .getResultList();
            if(existing.isEmpty()) {
                em.persist(dimension);
            }
        }
    }






    // TODO - sort these out!
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


    private String getStringValue(String rowDatum, String defaultValue) {
        return rowDatum.trim().isEmpty() ? defaultValue : rowDatum.trim();
    }

}
