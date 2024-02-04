package it.polimi.common;

public class Address {
    private int port;
    private String hostname;

    public Address(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public String getHostname() {
        return hostname;
    }
}
