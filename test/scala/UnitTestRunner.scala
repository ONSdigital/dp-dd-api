import org.scalatest.testng.TestNGWrapperSuite

class UnitTestRunner extends TestNGWrapperSuite (
  List("test/resources/testng.xml")
)