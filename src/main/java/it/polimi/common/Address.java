package it.polimi.common;

import java.util.Objects;

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
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Address address = (Address) o;
        return port == address.port && Objects.equals(hostname, address.hostname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(port, hostname);
    }
}
