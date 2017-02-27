package org.schedoscope.metascope;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;

public class MetascopeDaemon implements Daemon {

    private Metascope metascope;
    private String[] args;

    @Override
    public void init(DaemonContext context) throws DaemonInitException, Exception {
        this.args = context.getArguments();
        this.metascope = new Metascope();
    }

    @Override
    public void start() throws Exception {
        if (args == null) {
            args = new String[]{};
        }
        this.metascope.start(args);
    }

    @Override
    public void stop() throws Exception {
        this.metascope.stop();
    }

    @Override
    public void destroy() {
        try {
            stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
