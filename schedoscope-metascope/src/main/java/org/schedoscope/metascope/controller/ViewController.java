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

import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.RedirectView;

public abstract class ViewController {

	protected String templateURI;

	public ViewController() {
		setup();
	}

	protected void setup() {
		this.templateURI = getTemplateUri();
	}

	protected abstract String getTemplateUri();

	protected ModelAndView createView(String template) {
		return new ModelAndView(templateURI + "/" + template);
	}

	protected ModelAndView notFound() {
		return new ModelAndView(new RedirectView("/notfound"));
	}

	protected String failed() {
		return "Failed to load resource";
	}

}
