package services;

import exceptions.DatapointMappingException;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;

import javax.persistence.EntityManager;

/**
 * Capable of parsing a single row of a csv file and importing it in to the database.
 */
public interface DatapointParser {
    void parseRowdataDirectToTables(EntityManager em, String[] rowData, DimensionalDataSet dds) throws DatapointMappingException;
}
