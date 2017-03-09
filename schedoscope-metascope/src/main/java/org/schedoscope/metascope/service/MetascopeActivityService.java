package org.schedoscope.metascope.service;

import org.schedoscope.metascope.model.MetascopeActivity;
import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.repository.MetascopeActivityRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Created by kas on 24.11.16.
 */
@Service
public class MetascopeActivityService {

  @Autowired
  private MetascopeActivityRepository activityEntityRepository;

  public List<MetascopeActivity> getActivities() {
    return activityEntityRepository.findFirst10ByOrderByTimestampDesc();
  }

  @Transactional
  public void createUpdateDocumentActivity(MetascopeTable table, String username) {
    createAndSave(MetascopeActivity.ActivityType.UPDATE_DOCUMENTATION, table, username);
  }

  @Transactional
  public void createNewCommentActivity(MetascopeTable tableEntity, String username) {
    createAndSave(MetascopeActivity.ActivityType.NEW_COMMENT, tableEntity, username);
  }

  @Transactional
  public void createUpdateTaxonomyActivity(MetascopeTable tableEntity, String username) {
    createAndSave(MetascopeActivity.ActivityType.UPDATE_TAGS, tableEntity, username);
  }

  @Transactional
  public void createUpdateTableMetadataActivity(MetascopeTable tableEntity, String username) {
    createAndSave(MetascopeActivity.ActivityType.UPDATE_TABLE_METADATA, tableEntity, username);
  }

  private void createAndSave(MetascopeActivity.ActivityType activityType, MetascopeTable table, String username) {
    String activityKey = table.getFqdn() + "." + username + "." + activityType.toString();
    MetascopeActivity activityEntity = activityEntityRepository.findOne(activityKey);
    if (activityEntity == null) {
      activityEntity = new MetascopeActivity();
      activityEntity.setActivityId(activityKey);
      activityEntity.setType(activityType);
      activityEntity.setUsername(username);
      activityEntity.setTable(table);
    }
    activityEntity.setTimestamp(System.currentTimeMillis());
    activityEntityRepository.save(activityEntity);
  }

  public void setActivityEntityRepository(MetascopeActivityRepository activityEntityRepository) {
    this.activityEntityRepository = activityEntityRepository;
  }

}
