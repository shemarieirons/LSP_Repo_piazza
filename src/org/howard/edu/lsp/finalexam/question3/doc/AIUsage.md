AI Tools Used:
ChatGPT

Prompts Used (2–5 max):

1. what are some boundary test cases for a grading system in java
2. how do i use assertThrows in junit 5
3. check if these junit tests make sense and names are clear:
   /**
* JUnit 5 test class for GradeCalculator.
* Covers average(), letterGrade(), isPassing(), boundary values,
* and exception handling for invalid input.
  */
  public class GradeCalculatorTest {

  private GradeCalculator calc;

  @BeforeEach
  public void setUp() {
  calc = new GradeCalculator();
  }

  // -----------------------------------------------------------------------
  // 1. Test for average()
  // -----------------------------------------------------------------------

  @Test
  @DisplayName("average() returns correct result for three valid scores")
  public void testAverageWithValidScores() {
  double result = calc.average(80, 90, 100);
  assertEquals(90.0, result, 0.001, "Average of 80, 90, 100 should be 90.0");
  }

  // -----------------------------------------------------------------------
  // 2. Test for letterGrade()
  // -----------------------------------------------------------------------

  @Test
  @DisplayName("letterGrade() returns 'B' for an average of 85.0")
  public void testLetterGradeB() {
  assertEquals("B", calc.letterGrade(85.0));
  }

  // -----------------------------------------------------------------------
  // 3. Test for isPassing()
  // -----------------------------------------------------------------------

  @Test
  @DisplayName("isPassing() returns true when average is 60 or above")
  public void testIsPassingWhenAboveThreshold() {
  assertTrue(calc.isPassing(75.0), "75.0 should be a passing average");
  }

  // -----------------------------------------------------------------------
  // 4. Boundary-value tests
  // -----------------------------------------------------------------------

  @Test
  @DisplayName("Boundary: average of exactly 60 should return letter grade 'D' and isPassing true")
  public void testBoundaryExactly60() {
  double avg = calc.average(60, 60, 60);
  assertEquals(60.0, avg, 0.001);
  assertEquals("D", calc.letterGrade(avg));
  assertTrue(calc.isPassing(avg));
  }

  @Test
  @DisplayName("Boundary: average just below 60 (59.0) should return 'F' and isPassing false")
  public void testBoundaryJustBelow60() {
  double avg = calc.average(59, 59, 59);
  assertEquals(59.0, avg, 0.001);
  assertEquals("F", calc.letterGrade(avg));
  assertFalse(calc.isPassing(avg));
  }

  // -----------------------------------------------------------------------
  // 5. Exception tests
  // -----------------------------------------------------------------------

  @Test
  @DisplayName("average() throws IllegalArgumentException when a score is below 0")
  public void testExceptionScoreBelowZero() {
  assertThrows(IllegalArgumentException.class, () -> calc.average(-1, 80, 90),
  "Score of -1 should throw IllegalArgumentException");
  }

  @Test
  @DisplayName("average() throws IllegalArgumentException when a score is above 100")
  public void testExceptionScoreAbove100() {
  assertThrows(IllegalArgumentException.class, () -> calc.average(101, 80, 90),
  "Score of 101 should throw IllegalArgumentException");
  }
}

How AI Helped (2–3 sentences):
AI was mainly used to double check small things like boundary cases and how assertThrows works. It also helped me make sure my test names and structure looked okay. 

Reflection (1–2 sentences):
I got better at thinking about edge cases and how to test them properly. Also feel more comfortable writing junit tests now.