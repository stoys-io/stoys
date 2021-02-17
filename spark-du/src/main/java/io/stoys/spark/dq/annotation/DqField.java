package io.stoys.spark.dq.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Customize behaviour of dq reflection.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DqField {
    /**
     * @return Ignore the field.
     * <p>
     * No rules will be generated for given field.
     */
    boolean ignore() default false;

    /**
     * @return Is the field nullable?
     * <p>
     * Note: By default everything is nullable (just like spark, proto, json, csv, and others).
     */
    boolean nullable() default true;

    /**
     * @return List of all possible enum values.
     * <p>
     * Note: One should leave this empty and use different approach to check enums if the list is too large or dynamic.
     */
    String[] enumValues() default {};

    /**
     * @return String format of the field. For example: "MM/dd/yyyy"
     * <p>
     * Note: This makes sense only for some field types like dates and timestamps.
     */
    String format() default "";

    /**
     * @return Regular expression field has to match.
     * <p>
     * Note: Field is first casted to string hence the regexp rule works on all field types.
     */
    String regexp() default "";
}
