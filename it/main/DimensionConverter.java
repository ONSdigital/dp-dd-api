package main;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import play.Logger;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;

/*
* This isn't really a test.
* It is however a really useful little utility for converting hierarchical dimension values to the codes.
* */
public class DimensionConverter extends TestNGSuite {

    static Logger.ALogger logger = Logger.of(DimensionConverter.class);

    private EntityManagerFactory emf;
    private EntityManager em;
    private PostgresTest postgresTest = new PostgresTest();

    UUID datasetId;
    DimensionalDataSet dimensionalDataSet;

    @BeforeClass
    public void setupDb() {
        emf = postgresTest.getEMFForProductionLikeDatabase();
        em = emf.createEntityManager();
    }

    @BeforeMethod
    public void setup() {
        datasetId = UUID.randomUUID();
        dimensionalDataSet = postgresTest.createEmptyDataset(em, datasetId.toString(), "dataset");
    }


    @Test(enabled = false)
    public void convertDimensionInFile() throws Exception {

        // Set these bits
        String inputFileName = "CPI_2016_12_SpecAgg_v3.csv";
        String outputFileName = "plop.csv";
        int[] tripletStartindices = new int[]{6};
        // End of set these bits

        EntityTransaction tx = em.getTransaction();
        tx.begin();
        try {

            for (int i = 0; i < tripletStartindices.length; i++) {
                int tripletStartindex = tripletStartindices[i];

                File inputFile;
                File outputFile;
                BufferedWriter writer;

                if (i == 0) {
                    inputFile = new File(new PostgresTest().getClass().getResource(inputFileName).getPath());
                    writer = Files.newBufferedWriter(Paths.get(new File(outputFileName + "_" + i).getAbsolutePath()));
                } else {
                    inputFile = new File(new PostgresTest().getClass().getResource(outputFileName + "_" + (i - 1)).getPath());
                    writer = Files.newBufferedWriter(Paths.get(new File(outputFileName + "_" + i).getAbsolutePath()));
                }

                ArrayList<String> lines = Files.lines(Paths.get(inputFile.getAbsolutePath())).collect(Collectors.toCollection(ArrayList::new));

                int counter = 0;

                for (String line : lines) {
                    if (line.isEmpty() || counter == 0) {
                        writer.write(line + "\n");
                    } else {

                        String[] lineParts = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                        String hierarchyId = lineParts[tripletStartindex];
                        String dimName = lineParts[tripletStartindex + 1];
                        String dimValue = lineParts[tripletStartindex + 2].replace("\"", "");

                        logger.debug("Looking up code for hierarchy: " + hierarchyId + " and dimValue: " + dimValue);

                        String convertedDimValue = em.createQuery("SELECT he.code FROM HierarchyEntry he WHERE he.hierarchy.id = :hierarchyId AND he.name = :dimValue", String.class)
                                .setParameter("hierarchyId", hierarchyId)
                                .setParameter("dimValue", dimValue)
                                .getSingleResult();

                        lineParts[tripletStartindex + 2] = convertedDimValue;
                        String newLine = Arrays.stream(lineParts).collect(Collectors.joining(","));
                        logger.debug("Outputting ammended line: " + newLine);
                        writer.write(newLine + "\n");
                        writer.flush();
                    }
                    counter++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            tx.commit();
        }
    }

}
