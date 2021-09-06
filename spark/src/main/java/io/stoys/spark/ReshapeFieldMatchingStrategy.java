package io.stoys.spark;

/**
 * How should be source fields mapped to target fields?
 */
public enum ReshapeFieldMatchingStrategy {
    /**
     * Same as {@link #NAME_DEFAULT}.
     */
    UNDEFINED,

    /**
     * Name based matching after trimming (dropping leading and trailing spaces) and lower casing field names.
     */
    NAME_DEFAULT,

    /**
     * Name based matching on exact field names.
     */
    NAME_EXACT,

    /**
     * Name based matching after trimming (dropping leading and trailing spaces), lower casing and replacing non-word
     * characters with underscores field names.
     * <p>
     * For details see how {@link io.stoys.scala.Strings#toSnakeCase} works.
     */
    NAME_NORMALIZED,

    /**
     * Index based mapping ignores source field names. It assume source fields map to target fields in given order.
     * <p>
     * Note:  This is good for example for csv files without headers.
     * <p>
     * BEWARE: This is quite error prone for production code if there is any change source type will change.
     * The changes in source structure may lead to wrong field mapping without any errors.
     */
    INDEX,
}
