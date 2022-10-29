package org.dandoy.dbpop.database;

import org.junit.jupiter.api.Test;

class ExpressionParserTest {
    @Test
    void testExpressionParser() {
        ExpressionParser parser = new ExpressionParser();
        parser.evaluate("{{now}}");
        parser.evaluate("{{yesterday}}");
        parser.evaluate("{{tomorrow}}");
        parser.evaluate("{{now - 1 minute}}");
        parser.evaluate("{{now - 2 minutes}}");
        parser.evaluate("{{now - 1 hour}}");
        parser.evaluate("{{now - 2 hours}}");
        parser.evaluate("{{now - 1 day}}");
        parser.evaluate("{{now - 2 days}}");
        parser.evaluate("{{now - 1 month}}");
        parser.evaluate("{{now - 2 months}}");
        parser.evaluate("{{now - 1 year}}");
        parser.evaluate("{{now - 2 years}}");
    }
}