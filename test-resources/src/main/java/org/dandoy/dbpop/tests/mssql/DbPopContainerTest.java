package org.dandoy.dbpop.tests.mssql;

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith({DbPopContainerSetup.class})
public @interface DbPopContainerTest {
    boolean source();

    boolean target();

    boolean withTargetTables() default false;
}
