package scala

import org.scalatest.testng.TestNGWrapperSuite

class IntTestRunner extends TestNGWrapperSuite (
  List("test/resources/int-testng.xml")
)