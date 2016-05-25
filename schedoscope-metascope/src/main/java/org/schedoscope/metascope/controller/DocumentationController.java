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

import javax.servlet.http.HttpServletRequest;

import org.schedoscope.metascope.model.CommentEntity;
import org.schedoscope.metascope.model.FieldEntity;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.service.DocumentationService;
import org.schedoscope.metascope.service.FieldEntityService;
import org.schedoscope.metascope.service.TableEntityService;
import org.schedoscope.metascope.service.UserEntityService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

@Controller
public class DocumentationController {

	@Autowired
	private DocumentationService documentationService;
	@Autowired
	private TableEntityService tableEntityService;
	@Autowired
	private FieldEntityService fieldEntityService;
	@Autowired
	private UserEntityService userEntityService;

	@RequestMapping(value = "/table/documentation", method = RequestMethod.POST)
	public String updateDocumentation(HttpServletRequest request,
			RedirectAttributes redirAttr, String fqdn, String documentation) {
		TableEntity tableEntity = tableEntityService.findByFqdn(fqdn);
		documentationService.updateDocumentation(tableEntity, documentation,
				userEntityService.getUser());
		redirAttr.addFlashAttribute("documentation", true);
		return "redirect:" + request.getHeader("Referer");
	}

	@RequestMapping(value = "/table/documentation/comment/add", method = RequestMethod.POST)
	public String addComment(HttpServletRequest request,
			RedirectAttributes redirAttr, String fqdn, String comment) {
		TableEntity tableEntity = tableEntityService.findByFqdn(fqdn);
		documentationService.addComment(tableEntity, comment,
				userEntityService.getUser());
		redirAttr.addFlashAttribute("comment", "created");
		return "redirect:" + request.getHeader("Referer");
	}

	@RequestMapping(value = "/table/documentation/comment/edit", method = RequestMethod.POST)
	public String editComment(HttpServletRequest request,
			RedirectAttributes redirAttr, String commentID, String commentText) {
		CommentEntity commentEntity = documentationService.findById(commentID);
		documentationService.editComment(commentEntity, commentText,
				userEntityService.getUser());
		redirAttr.addFlashAttribute("comment", "edited");
		return "redirect:" + request.getHeader("Referer");
	}

	@RequestMapping(value = "/table/documentation/comment/delete", method = RequestMethod.POST)
	public String deleteComment(HttpServletRequest request,
			RedirectAttributes redirAttr, String commentID) {
		CommentEntity commentEntity = documentationService.findById(commentID);
		TableEntity tableEntity = tableEntityService
				.findByComment(commentEntity);
		documentationService.deleteComment(tableEntity, commentEntity,
				userEntityService.getUser());
		redirAttr.addFlashAttribute("comment", "deleted");
		return "redirect:" + request.getHeader("Referer");
	}

	@RequestMapping(value = "/field/documentation", method = RequestMethod.POST)
	public String updateDocumentation(HttpServletRequest request,
			RedirectAttributes redirAttr, String fqdn, String fieldname,
			boolean parameter, String documentation) {
		FieldEntity fieldEntity = fieldEntityService.findByFqdnAndName(fqdn,
				fieldname);
		documentationService.updateDocumentation(fieldEntity, documentation,
				userEntityService.getUser());
		redirAttr.addFlashAttribute("documentation", true);
		return getReferer(request, fieldname, parameter);
	}

	@RequestMapping(value = "/field/documentation/comment/add", method = RequestMethod.POST)
	public String addComment(HttpServletRequest request, String fqdn,
			RedirectAttributes redirAttr, String fieldname, boolean parameter,
			String comment) {
		FieldEntity fieldEntity = fieldEntityService.findByFqdnAndName(fqdn,
				fieldname);
		documentationService.addComment(fieldEntity, comment,
				userEntityService.getUser());
		redirAttr.addFlashAttribute("comment", "created");
		return getReferer(request, fieldname, parameter);
	}

	@RequestMapping(value = "/field/documentation/comment/edit", method = RequestMethod.POST)
	public String editFieldComment(HttpServletRequest request,
			RedirectAttributes redirAttr, String fieldname, boolean parameter,
			String commentID, String commentText) {
		CommentEntity commentEntity = documentationService.findById(commentID);
		documentationService.editComment(commentEntity, commentText,
				userEntityService.getUser());
		redirAttr.addFlashAttribute("comment", "edited");
		return getReferer(request, fieldname, parameter);
	}

	@RequestMapping(value = "/field/documentation/comment/delete", method = RequestMethod.POST)
	public String deleteComment(HttpServletRequest request,
			RedirectAttributes redirAttr, String fieldname, boolean parameter,
			String commentID) {
		CommentEntity commentEntity = documentationService.findById(commentID);
		FieldEntity fieldEntity = fieldEntityService
				.findByComment(commentEntity);
		documentationService.deleteComment(fieldEntity, commentEntity,
				userEntityService.getUser());
		redirAttr.addFlashAttribute("comment", "deleted");
		return getReferer(request, fieldname, parameter);
	}

	private String getReferer(HttpServletRequest request, String fieldname,
			boolean parameter) {
		String refererString = "redirect:" + request.getHeader("Referer");
		if (parameter) {
			return refererString + "#parameterContent-" + fieldname;
		} else {
			return refererString + "#schemaContent-" + fieldname;
		}
	}

}
