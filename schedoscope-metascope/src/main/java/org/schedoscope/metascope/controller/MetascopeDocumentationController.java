/**
 * Copyright 2017 Otto (GmbH & Co KG)
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

import org.schedoscope.metascope.model.MetascopeComment;
import org.schedoscope.metascope.model.MetascopeField;
import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.service.MetascopeDocumentationService;
import org.schedoscope.metascope.service.MetascopeFieldService;
import org.schedoscope.metascope.service.MetascopeTableService;
import org.schedoscope.metascope.service.MetascopeUserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import javax.servlet.http.HttpServletRequest;

@Controller
public class MetascopeDocumentationController {

    @Autowired
    private MetascopeDocumentationService documentationService;
    @Autowired
    private MetascopeTableService metascopeTableService;
    @Autowired
    private MetascopeFieldService metascopeFieldService;
    @Autowired
    private MetascopeUserService metascopeUserService;

    @RequestMapping(value = "/table/documentation", method = RequestMethod.POST)
    public String updateDocumentation(HttpServletRequest request, RedirectAttributes redirAttr, String fqdn,
                                      String documentation) {
        MetascopeTable table = metascopeTableService.findByFqdn(fqdn);
        documentationService.updateDocumentation(table, documentation, metascopeUserService.getUser());
        redirAttr.addFlashAttribute("documentation", true);
        return "redirect:" + request.getHeader("Referer");
    }

    @RequestMapping(value = "/table/documentation/comment/add", method = RequestMethod.POST)
    public String addComment(HttpServletRequest request, RedirectAttributes redirAttr, String fqdn, String comment) {
        MetascopeTable table = metascopeTableService.findByFqdn(fqdn);
        documentationService.addComment(table, comment, metascopeUserService.getUser());
        redirAttr.addFlashAttribute("comment", "created");
        return "redirect:" + request.getHeader("Referer");
    }

    @RequestMapping(value = "/table/documentation/comment/edit", method = RequestMethod.POST)
    public String editComment(HttpServletRequest request, RedirectAttributes redirAttr, String commentID,
                              String commentText) {
        MetascopeComment comment = documentationService.findById(commentID);
        documentationService.editComment(comment, commentText, metascopeUserService.getUser());
        redirAttr.addFlashAttribute("comment", "edited");
        return "redirect:" + request.getHeader("Referer");
    }

    @RequestMapping(value = "/table/documentation/comment/delete", method = RequestMethod.POST)
    public String deleteComment(HttpServletRequest request, RedirectAttributes redirAttr, String commentID) {
        MetascopeComment comment = documentationService.findById(commentID);
        MetascopeTable table = metascopeTableService.findByComment(comment);
        documentationService.deleteComment(table, comment, metascopeUserService.getUser());
        redirAttr.addFlashAttribute("comment", "deleted");
        return "redirect:" + request.getHeader("Referer");
    }

    @RequestMapping(value = "/field/documentation", method = RequestMethod.POST)
    public String updateDocumentation(HttpServletRequest request, RedirectAttributes redirAttr, String fqdn,
                                      String fieldname, boolean parameter, String documentation) {
        MetascopeField fieldEntity = metascopeFieldService.findById(fqdn + "." + fieldname);
        documentationService.updateDocumentation(fieldEntity, documentation, metascopeUserService.getUser());
        redirAttr.addFlashAttribute("documentation", true);
        return getReferer(request, fieldname, parameter);
    }

    @RequestMapping(value = "/field/documentation/comment/add", method = RequestMethod.POST)
    public String addComment(HttpServletRequest request, String fqdn, RedirectAttributes redirAttr, String fieldname,
                             boolean parameter, String comment) {
        MetascopeField fieldEntity = metascopeFieldService.findById(fqdn + "." + fieldname);
        documentationService.addComment(fieldEntity, comment, metascopeUserService.getUser());
        redirAttr.addFlashAttribute("comment", "created");
        return getReferer(request, fieldname, parameter);
    }

    @RequestMapping(value = "/field/documentation/comment/edit", method = RequestMethod.POST)
    public String editFieldComment(HttpServletRequest request, RedirectAttributes redirAttr, String fieldname,
                                   boolean parameter, String commentID, String commentText) {
        MetascopeComment commentEntity = documentationService.findById(commentID);
        documentationService.editComment(commentEntity, commentText, metascopeUserService.getUser());
        redirAttr.addFlashAttribute("comment", "edited");
        return getReferer(request, fieldname, parameter);
    }

    @RequestMapping(value = "/field/documentation/comment/delete", method = RequestMethod.POST)
    public String deleteComment(HttpServletRequest request, RedirectAttributes redirAttr, String fieldname,
                                boolean parameter, String commentID) {
        MetascopeComment commentEntity = documentationService.findById(commentID);
        MetascopeField fieldEntity = metascopeFieldService.findByComment(commentEntity);
        documentationService.deleteComment(fieldEntity, commentEntity, metascopeUserService.getUser());
        redirAttr.addFlashAttribute("comment", "deleted");
        return getReferer(request, fieldname, parameter);
    }

    private String getReferer(HttpServletRequest request, String fieldname, boolean parameter) {
        String refererString = "redirect:" + request.getHeader("Referer");
        if (parameter) {
            return refererString + "#parameterContent-" + fieldname;
        } else {
            return refererString + "#schemaContent-" + fieldname;
        }
    }

    @RequestMapping(value = "/table/documentation/autosave", method = RequestMethod.POST)
    @ResponseBody
    public boolean autosaveDocumentation(HttpServletRequest request, RedirectAttributes redirAttr, String fqdn, String documentation) {
        MetascopeTable table = metascopeTableService.findByFqdn(fqdn);
        return documentationService.autosaveDocumentation(table, documentation, metascopeUserService.getUser());
    }

    @RequestMapping(value = "/table/documentation/autosave/get", method = RequestMethod.GET)
    @ResponseBody
    public String getDraft(HttpServletRequest request, RedirectAttributes redirAttr, String fqdn) {
        MetascopeTable table = metascopeTableService.findByFqdn(fqdn);
        return documentationService.getDraft(table, metascopeUserService.getUser()).getText();
    }

}
