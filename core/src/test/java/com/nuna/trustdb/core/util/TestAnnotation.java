package com.nuna.trustdb.core.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface TestAnnotation {
    String stringValue() default "default";

    int[] arrayValue() default {42};

    FooBarBazJava enumValue() default FooBarBazJava.FOO;

    Class<?> classValue() default TestAnnotation.class;

    TestAnnotationValue annotationValue() default @TestAnnotationValue();
}
