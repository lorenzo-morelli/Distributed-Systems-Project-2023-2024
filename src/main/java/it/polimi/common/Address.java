package it.polimi.common;

/**
 * The Address class is a record used to represent the address of a node.
 * It contains the hostname and the port of the node.
 *
 * @param hostname represents the hostname of the node.
 * @param port     represents the port of the node.
 */
public record Address(String hostname, int port) {

}
