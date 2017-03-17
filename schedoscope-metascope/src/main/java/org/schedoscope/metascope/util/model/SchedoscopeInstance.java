package org.schedoscope.metascope.util.model;

/**
 * Created by kas on 23.11.16.
 */
public class SchedoscopeInstance {

    private final String id;
    private final String host;
    private final int port;

    public SchedoscopeInstance(String id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchedoscopeInstance that = (SchedoscopeInstance) o;

        if (port != that.port) return false;
        if (!id.equals(that.id)) return false;
        return host.equals(that.host);

    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + host.hashCode();
        result = 31 * result + port;
        return result;
    }

}
