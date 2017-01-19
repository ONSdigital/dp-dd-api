package services;

import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.Test;

public class InputCSVParserTest extends TestNGSuite {

    InputCSVParser parser = new InputCSVParser();

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void rowDataValidationShouldFailIfTooShort() {
        String oldFormat = "676767,,,,,,,,,";
        parser.basicValidationOfRowData(oldFormat.split(","));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void rowDataValidationShouldFailIfUnevenNumberOfFields() {
        String oldFormat = "676767,,,,,,,,,,,";
        parser.basicValidationOfRowData(oldFormat.split(","));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void rowDataValidationShouldFailForOldFormat() {
        String oldFormat = "676767,,,,,,,,,,,,,,,,,2014,2014,,Year,,,,,,,,,,,,,,,NACE,NACE,,08,08 - Other mining and quarrying,,,,Prodcom Elements,Prodcom Elements,,UK manufacturer sales ID,UK manufacturer sales LABEL,,,";
        parser.basicValidationOfRowData(oldFormat.split(","));
    }

}