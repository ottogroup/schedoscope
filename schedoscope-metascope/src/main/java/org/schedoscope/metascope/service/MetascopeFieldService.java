package org.schedoscope.metascope.service;

import org.schedoscope.metascope.model.MetascopeComment;
import org.schedoscope.metascope.model.MetascopeField;
import org.schedoscope.metascope.repository.MetascopeFieldRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class MetascopeFieldService {

  @Autowired
  private MetascopeFieldRepository metascopeFieldRepository;

  public List<String> findDistinctParameters() {
    List<String> list = new ArrayList<String>();
    List<Object[]> parameters = metascopeFieldRepository.findDistinctParameters();
    for (Object[] field : parameters) {
      list.add((String) field[0]);
    }
    return list;
  }

  public MetascopeField findById(String id) {
    return metascopeFieldRepository.findOne(id);
  }

  public MetascopeField findByComment(MetascopeComment commentEntity) {
    return metascopeFieldRepository.findByComment(commentEntity);
  }

  public void setMetascopeFieldRepository(MetascopeFieldRepository metascopeFieldRepository) {
    this.metascopeFieldRepository = metascopeFieldRepository;
  }
}
