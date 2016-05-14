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
package org.schedoscope.metascope.controller;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.schedoscope.metascope.conf.MetascopeConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class StatusController {

	@Autowired
	private MetascopeConfig config;

	@RequestMapping(value = "/status/activetransactions", method = RequestMethod.GET)
	@ResponseBody
	public boolean activeTransactions() {
		return false;
	}

	@RequestMapping(value = "/log", method = RequestMethod.GET)
	@ResponseBody
	public String getLog() {
		int lastLines = 100;

		String path = config.getLogfilePath();
		File log = new File(path);
		if (!log.exists()) {
			return "Could not find logfile (" + path + ")";
		}

		BufferedReader bufferedReader;
		try {
			bufferedReader = new BufferedReader(new FileReader(log));
		} catch (FileNotFoundException e) {
			return e.getMessage();
		}

		List<String> lines = new ArrayList<String>();
		String line = null;
		try {
			while ((line = bufferedReader.readLine()) != null) {
				lines.add(line);
			}
			bufferedReader.close();
		} catch (IOException e) {
			return e.getMessage();
		}

		String logcontent = "";
		int size = lines.size();
		for (int i = size - lastLines; i < size; i++) {
			logcontent += lines.get(i) + "\n";
		}
		return logcontent;
	}

}
