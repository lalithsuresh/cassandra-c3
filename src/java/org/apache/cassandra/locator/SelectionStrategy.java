package org.apache.cassandra.locator;

public enum SelectionStrategy {
    BEST_QSZ,
    ED,
    POWER_OF_TWO_QSZ,
    DEFAULT;
}
