package io.stoys.spark.dq.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DqField {
    boolean nullable() default true;

    String[] enumValues() default {};

    String format() default "";

    String regexp() default "";
}
