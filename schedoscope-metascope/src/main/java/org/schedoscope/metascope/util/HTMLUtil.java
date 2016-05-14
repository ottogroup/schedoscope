/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.metascope.util;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class HTMLUtil {

	private static final Logger LOG = LoggerFactory.getLogger(HTMLUtil.class);

	private static final String BOOTSTRAP_PANEL_RED = "danger";
	private static final String BOOTSTRAP_PANEL_GREEN = "success";
	private static final String BOOTSTRAP_PANEL_YELLOW = "warning";
	private static final String BOOTSTRAP_PANEL_BLUE = "info";
	private static final String BOOTSTRAP_PANEL_GREY = "default";
	private static final String NO_DATE_AVAILABLE = "-";
	private static final String DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

	public String getColorForPanel(String status) {
		switch (status) {
		case SchedoscopeUtil.FAILED_STATUS:
			return BOOTSTRAP_PANEL_RED;
		case SchedoscopeUtil.RECEIVE_STATUS:
		case SchedoscopeUtil.MATERIALIZED_STATUS:
			return BOOTSTRAP_PANEL_GREEN;
		case SchedoscopeUtil.TRANSFORMING_STATUS:
			return BOOTSTRAP_PANEL_YELLOW;
		case SchedoscopeUtil.RETRYING_STATUS:
		case SchedoscopeUtil.NODATA_STATUS:
			return BOOTSTRAP_PANEL_BLUE;
		default:
			return BOOTSTRAP_PANEL_GREY;
		}
	}

	public String convertQueryResultToTable(HiveQueryResult result) {
		String table = "<table class=\"table table-bordered table-striped\">\n";
		table += "<tr class=\"info\">\n";
		if (result.getErrorMessage() != null) {
			return "<p>" + result.getErrorMessage() + "</p>";
		}
		for (String head : result.getHeader()) {
			table += "<th>" + head + "</th>";
		}
		table += "</tr>\n";
		for (int i = 0; i < result.getRows().size(); i++) {
			table += "<tr>";
			for (String body : result.getRows().get(i)) {
				table += "<td>" + body + "</td>";
			}
			table += "</tr>\n";
		}
		table += "</table>";
		return table;
	}

	public String convertLineageGraphToVisJsNetwork(List<LineageNode> graph) {
		for (int i = 0; i < graph.size(); i++) {
			LineageNode node = graph.get(i);
			node.setId(i);
			if (node.getWiredTo().size() == 0) {
				node.setLevel(maxLevel(graph));
			}
		}
		String nodes = "{\n \"nodes\": [\n";
		String edges = "\n \"edges\": [\n";
		int nodeCounter = 0;
		int edgeCounter = 0;
		for (LineageNode n : graph) {
			if (nodeCounter > 0) {
				nodes += ", ";
			}
			nodes += "{\n \"id\": " + n.getId() + ",\n \"label\": \""
					+ n.getLabel() + "\",\n \"group\": \"" + n.getType()
					+ "\",\n \"level\": " + n.getModifiedLevel()
					+ ",\n \"fqdn\": \"" + n.getFqdn() + "\"\n}";
			nodeCounter++;
			for (LineageNode d : n.getWiredTo()) {
				if (edgeCounter > 0) {
					edges += ", ";
				}
				edges += "{\n \"from\": " + n.getId() + ",\n \"to\": "
						+ d.getId() + ",\n \"arrows\": \"from\"\n}";
				edgeCounter++;
			}
		}
		nodes += "],";
		edges += "]}";
		return nodes + edges;
	}

	private int maxLevel(List<LineageNode> graph) {
		int max = -1;
		for (LineageNode node : graph) {
			if (node.getLevel() > max) {
				max = node.getLevel();
			}
		}
		return max;
	}

	public String getAboutTime(long ts) {
		long since = System.currentTimeMillis() - ts;
		if (since < 3600000) {
			int minutes = ((int) (since / 60000)) + 1;
			if (minutes == 1) {
				return "just now";
			} else {
				return "about " + minutes + " minutes ago";
			}
		} else if (since < 86400000) {
			int hours = ((int) (since / 3600000));
			if (hours == 1) {
				return "about an hour ago";
			} else {
				return "about " + hours + " hours ago";
			}
		} else if (since < 604800000) {
			int days = ((int) (since / 86400000));
			if (days == 1) {
				return "yesterday";
			} else {
				return "about " + days + " days ago";
			}
		} else {
			String month = new SimpleDateFormat("MMM").format(ts);
			String day = new SimpleDateFormat("d").format(ts);
			return month + " " + day;
		}
	}

	public String getTime(long ts) {
		if (ts == 0) {
			return NO_DATE_AVAILABLE;
		}
		String year = (new SimpleDateFormat("YYYY")).format(ts);
		String month = (new SimpleDateFormat("MMMM")).format(ts); //
		String day = (new SimpleDateFormat("EEEE")).format(ts);
		String d = (new SimpleDateFormat("d")).format(ts);
		String time = (new SimpleDateFormat("HH:mm:ss")).format(ts);
		return day + ", " + d + " " + month + " " + year + " at " + time;
	}

	public String getTime(String timestamp, String format) {
		if (timestamp == null) {
			return NO_DATE_AVAILABLE;
		}

		if (format == null || format.isEmpty()) {
			try {
				long ts = Long.parseLong(timestamp);
				if (ts == 0) {
					return NO_DATE_AVAILABLE;
				}
				return getTime(ts);
			} catch (NumberFormatException e) {
				return timestamp;
			}
		}

		try {
			return getTime(new SimpleDateFormat(format).parse(timestamp)
					.getTime());
		} catch (ParseException e) {
			LOG.debug(
					"Could not parse timestamp '{}' with format '{}', trying with default format '{}'",
					timestamp, format, DEFAULT_TIMESTAMP_FORMAT);
			try {
				return getTime(new SimpleDateFormat(DEFAULT_TIMESTAMP_FORMAT)
						.parse(timestamp).getTime());
			} catch (ParseException e1) {
				LOG.debug(
						"Could not parse timestamp '{}', displaying unparsed timestamp",
						timestamp, format);
				return timestamp;
			}
		}
	}

	public String getDuration(long start, long end) {
		if (end == 0 || start == 0) {
			return NO_DATE_AVAILABLE;
		}
		long millis = end - start;
		return String.format(
				"%02d:%02d:%02d",
				TimeUnit.MILLISECONDS.toHours(millis),
				TimeUnit.MILLISECONDS.toMinutes(millis)
						- TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS
								.toHours(millis)),
				TimeUnit.MILLISECONDS.toSeconds(millis)
						- TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS
								.toMinutes(millis)));
	}

	public String getSize(long size) {
		if (size <= 0) {
			return "0";
		}
		final String[] units = new String[] { "B", "kB", "MB", "GB", "TB" };
		int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
		return new DecimalFormat("#,##0.#").format(size
				/ Math.pow(1024, digitGroups))
				+ " " + units[digitGroups];
	}

}
