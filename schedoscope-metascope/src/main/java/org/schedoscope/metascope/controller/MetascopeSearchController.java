/**
 * Copyright 2015 Otto (GmbH & Co KG)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.metascope.controller;

import org.schedoscope.metascope.index.SolrFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

@Controller
public class MetascopeSearchController {

  @Autowired
  private SolrFacade solrIndex;

  @RequestMapping(value = "/solr/suggest", method = RequestMethod.GET)
  @ResponseBody
  public String suggest(String userInput) {
    String result = "[";
    if (userInput != null && userInput.length() > 2) {
      List<String> suggestions = solrIndex.suggest(userInput);
      for (int i = 0; i < suggestions.size(); i++) {
        result += "\"" + suggestions.get(i) + "\"";
        if (i < suggestions.size() - 1) {
          result += ", ";
        }
      }
    }
    return result + "]";
  }

}
