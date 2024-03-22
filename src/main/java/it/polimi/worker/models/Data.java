package it.polimi.worker.models;

/**
 * The Data class is a record used to store the data read from HDFS and the end flag which indicates if there is no more data to read.
 * It contains the data and the end flag.
 * @param data represents the data.
 * @param end represents the end flag.
 * @return the data and the end flag.
 */
public record Data(String data, boolean end) {

}
