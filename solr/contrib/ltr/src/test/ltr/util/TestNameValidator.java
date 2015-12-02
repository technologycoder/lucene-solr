package ltr.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import ltr.util.NameValidator;

import org.junit.Test;

public class TestNameValidator {

  @Test
  public void testValidator() {
    assertTrue(NameValidator.check("test"));
    assertTrue(NameValidator.check("constant"));
    assertTrue(NameValidator.check("test_test"));
    assertTrue(NameValidator.check("TEst"));
    assertTrue(NameValidator.check("TEST"));
    assertTrue(NameValidator.check("328195082960784"));
    assertFalse(NameValidator.check("    "));
    assertFalse(NameValidator.check(""));
    assertFalse(NameValidator.check("test?"));
    assertFalse(NameValidator.check("??????"));
    assertFalse(NameValidator.check("_____-----"));
    assertFalse(NameValidator.check("12345,67890.31"));
    assertFalse(NameValidator.check("aasdasdadasdzASADADSAZ01239()[]|_-"));
    assertFalse(NameValidator.check(null));
    assertTrue(NameValidator.check("a"));
    assertTrue(NameValidator.check("test()"));
    assertTrue(NameValidator.check("test________123"));

  }
}
