package org.schedoscope.metascope.util;

import java.util.List;

public class StatusUtil {

    private static final String FAILED_STATUS = "failed";
    private static final String WAITING_STATUS = "waiting";
    private static final String TRANSFORMING_STATUS = "transforming";
    private static final String INVALIDATED_STATUS = "invalidated";
    private static final String NODATA_STATUS = "nodata";
    private static final String RETRYING_STATUS = "retrying";
    private static final String RECEIVE_STATUS = "receive";
    private static final String MATERIALIZED_STATUS = "materialized";

    public static String getStatus(List<String> statuses) {
        for (String status : statuses) {
            if (status.equals(FAILED_STATUS)) {
                return FAILED_STATUS;
            } else if (status.equals(RETRYING_STATUS)) {
                return RETRYING_STATUS;
            } else if (status.equals(TRANSFORMING_STATUS)) {
                return TRANSFORMING_STATUS;
            } else if (status.equals(INVALIDATED_STATUS)) {
                return INVALIDATED_STATUS;
            } else if (status.equals(WAITING_STATUS)) {
                return WAITING_STATUS;
            } else if (status.equals(RECEIVE_STATUS)) {
                return RECEIVE_STATUS;
            } else if (status.equals(NODATA_STATUS)) {
                return NODATA_STATUS;
            }
        }

        return MATERIALIZED_STATUS;
    }

}
