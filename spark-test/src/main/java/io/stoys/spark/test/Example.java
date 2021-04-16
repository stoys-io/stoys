package io.stoys.spark.test;


import org.scalatest.TagAnnotation;

import java.lang.annotation.*;

@TagAnnotation
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface Example {
}
