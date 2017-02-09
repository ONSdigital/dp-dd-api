package services;

import exceptions.DatapointMappingException;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;

import javax.persistence.*;

/**
 * Capable of parsing a single row of a csv file and importing it in to the database.
 */
public interface DatapointParser extends AutoCloseable {
    void parseRowdataDirectToTables(EntityManager em, String[] rowData, DimensionalDataSet dds) throws DatapointMappingException;
    void close();
}
