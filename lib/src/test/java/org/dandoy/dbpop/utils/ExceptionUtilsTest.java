package org.dandoy.dbpop.utils;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExceptionUtilsTest {
    @Test
    void name() {
        try {
            doit1();
        } catch (Exception e) {
//            e.printStackTrace();

            {
                System.out.println("---No indentation");
                List<String> errorMessages = ExceptionUtils.getErrorMessages(e);
                errorMessages.forEach(System.out::println);
                assertEquals(3, errorMessages.size());
                assertTrue(errorMessages.get(0).contains("doit1"));
                assertTrue(errorMessages.get(1).contains("doit2"));
                assertTrue(errorMessages.get(2).contains("doit3"));
            }

            {
                System.out.println("---With indentation");
                List<String> errorMessages = ExceptionUtils.getErrorMessages(e, "  ");
                errorMessages.forEach(System.out::println);

                assertEquals(3, errorMessages.size());
                assertTrue(errorMessages.get(0).contains("doit1"));
                assertTrue(errorMessages.get(1).contains("doit2"));
                assertTrue(errorMessages.get(2).contains("doit3"));
            }
        }
    }

    private void doit1() {
        try {
            doit2();
        } catch (Exception e) {
            throw new RuntimeException("In doit1", e);
        }
    }

    private void doit2() {
        try {
            doit3();
        } catch (Exception e) {
            throw new RuntimeException("In doit2", e);
        }
    }

    private void doit3() {
        try (CloseIt ignored = new CloseIt()) {
            throw new RuntimeException("In doit3");
        }
    }

    private static class CloseIt implements AutoCloseable {
        @Override
        public void close() {
            throw new RuntimeException("CloseIt");
        }
    }
}