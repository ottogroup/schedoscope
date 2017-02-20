package org.schedoscope.metascope.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.model.*;
import org.schedoscope.metascope.repository.MetascopeCategoryObjectRepository;
import org.schedoscope.metascope.repository.MetascopeTableRepository;
import org.schedoscope.metascope.repository.MetascopeViewRepository;
import org.schedoscope.metascope.util.HiveQueryExecutor;
import org.schedoscope.metascope.util.LineageUtil;
import org.schedoscope.metascope.util.SampleCacheLoader;
import org.schedoscope.metascope.util.model.CategoryMap;
import org.schedoscope.metascope.util.model.HiveQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.handler.component.StatsField.Stat.distinctValues;

@Service
public class MetascopeTableService {

  private static final Logger LOG = LoggerFactory.getLogger(MetascopeTableService.class);

  @Autowired
  private MetascopeTableRepository metascopeTableRepository;

  @Autowired
  private MetascopeUserService metascopeUserService;

  @Autowired
  private MetascopeCategoryObjectRepository metascopeCategoryObjectRepository;

  @Autowired
  private MetascopeActivityService metascopeActivityService;

  @Autowired
  private MetascopeViewRepository metascopeViewRepository;

  @Autowired
  @Lazy
  private SolrFacade solr;

  @Autowired
  private HiveQueryExecutor hiveQueryExecutor;

  /** sample cache */
  private LoadingCache<String, HiveQueryResult> sampleCache;

  @PostConstruct
  public void init() {
    this.sampleCache = CacheBuilder.newBuilder().maximumSize(1000).expireAfterWrite(1, TimeUnit.DAYS)
            .build(new SampleCacheLoader(this, hiveQueryExecutor));
  }

  public MetascopeTable findByFqdn(String fqdn) {
    if (fqdn == null) {
      return null;
    }
    return metascopeTableRepository.findOne(fqdn);
  }

  public List<MetascopeTable> getTopFiveTables() {
    return metascopeTableRepository.findTop5ByOrderByViewCountDesc();
  }

  public MetascopeTable findByComment(MetascopeComment comment) {
    return metascopeTableRepository.findByComment(comment);
  }

  @Transactional
  public void save(MetascopeTable table) {
    this.metascopeTableRepository.save(table);
  }

  public Map<String, CategoryMap> getTableTaxonomies(MetascopeTable table) {
    Map<String, CategoryMap> taxonomies = new LinkedHashMap<String, CategoryMap>();

    for (MetascopeCategoryObject categoryObjectEntity : table.getCategoryObjects()) {
      String taxonomyName = categoryObjectEntity.getCategory().getTaxonomy().getName();
      CategoryMap categoryMap = taxonomies.get(taxonomyName);
      if (categoryMap == null) {
        categoryMap = new CategoryMap();
      }
      categoryMap.addToCategories(categoryObjectEntity.getCategory());
      categoryMap.addToCategoryObjects(categoryObjectEntity);
      taxonomies.put(taxonomyName, categoryMap);
    }
    return taxonomies;
  }

  public Set<String> getAllOwner() {
    return metascopeTableRepository.getAllOwner();
  }

  public void addOrRemoveFavourite(String fqdn) {
    MetascopeUser user = metascopeUserService.getUser();
    if (user.getFavourites() == null) {
      user.setFavourites(new ArrayList<String>());
    }
    boolean removed = user.getFavourites().remove(fqdn);
    if (!removed) {
      user.getFavourites().add(fqdn);
    }
    metascopeUserService.save(user);
  }

  @Transactional
  public void setCategoryObjects(String fqdn, Map<String, String[]> parameterMap) {
    MetascopeTable table = metascopeTableRepository.findOne(fqdn);

    if (table == null) {
      return;
    }

    table.getCategoryObjects().clear();

    String categoryObjectList = "";
    if (parameterMap != null) {
      for (Map.Entry<String, String[]> e : parameterMap.entrySet()) {
        if (!e.getKey().endsWith("CategoryObjects")) {
          continue;
        }

        String categoryObjectIds = e.getValue()[0];
        String[] categoryObjects = categoryObjectIds.split(",");
        for (String categoryObjectId : categoryObjects) {
          if (categoryObjectId.isEmpty()) {
            continue;
          }

          MetascopeCategoryObject categoryObjectEntity = metascopeCategoryObjectRepository.findOne(Long
                  .parseLong(categoryObjectId));
          if (categoryObjectEntity != null) {
            table.getCategoryObjects().add(categoryObjectEntity);
            if (!categoryObjectList.isEmpty()) {
              categoryObjectList += ", ";
            }
            categoryObjectList += categoryObjectEntity.getName();
          }
        }
      }
    }

    metascopeTableRepository.save(table);
    solr.updateTableEntityAsync(table, true);
    LOG.info("User '{}' changed category objects for table '{}' to '{}'", metascopeUserService.getUser().getUsername(),
            fqdn, categoryObjectList);
    metascopeActivityService.createUpdateTaxonomyActivity(table, metascopeUserService.getUser().getUsername());
  }

  @Transactional
  public void setTags(String fqdn, String tagsCommaDelimited) {
    MetascopeTable table = metascopeTableRepository.findOne(fqdn);

    if (table == null) {
      return;
    }

    if (tagsCommaDelimited == null) {
      tagsCommaDelimited = "";
    }

    String[] tags = tagsCommaDelimited.split(",");
    table.getTags().clear();
    for (String tag : tags) {
      if (!tag.isEmpty()) {
        table.getTags().add(tag);
      }
    }
    metascopeTableRepository.save(table);
    solr.updateTableEntityAsync(table, true);
    LOG.info("User '{}' changed tags for table '{}' to '{}'", metascopeUserService.getUser().getUsername(), fqdn,
            tagsCommaDelimited);
    metascopeActivityService.createUpdateTaxonomyActivity(table, metascopeUserService.getUser().getUsername());
  }

  @Transactional
  public void setPersonResponsible(String fqdn, String fullname) {
    MetascopeTable table = metascopeTableRepository.findOne(fqdn);
    MetascopeUser user = metascopeUserService.findByFullname(fullname);
    if (table != null) {
      if (table.getPersonResponsible() == null || !table.getPersonResponsible().equals(fullname)) {
        if (user != null) {
          table.setPersonResponsible(user.getFullname());
          metascopeTableRepository.save(table);
          LOG.info("User '{}' changed responsible person for table '{}' to '{}'", metascopeUserService.getUser()
                  .getUsername(), fqdn, fullname);
          metascopeActivityService.createUpdateTableMetadataActivity(table, metascopeUserService.getUser()
                  .getUsername());
        } else if (!fullname.isEmpty()) {
          table.setPersonResponsible(fullname);
          metascopeTableRepository.save(table);
          LOG.info("User '{}' changed responsible person for table '{}' to '{}'", metascopeUserService.getUser()
                  .getUsername(), fqdn, fullname);
          metascopeActivityService.createUpdateTableMetadataActivity(table, metascopeUserService.getUser()
                  .getUsername());
        }
      }
    }
  }

  @Transactional
  public void setTimestampField(String fqdn, String dataTimestampField, String dataTimestampFieldFormat) {
    MetascopeTable table = metascopeTableRepository.findOne(fqdn);
    if (table != null && !dataTimestampField.isEmpty()) {
      String oldTimestampField = table.getTimestampField();
      table.setTimestampField(dataTimestampField);
      if (!dataTimestampFieldFormat.isEmpty()) {
        table.setTimestampFieldFormat(dataTimestampFieldFormat);
      }
      metascopeTableRepository.save(table);
      LOG.info("User '{}' changed timestamp field for table '{}' to '{}' with format '{}'", metascopeUserService.getUser()
              .getUsername(), fqdn, dataTimestampField, dataTimestampFieldFormat);
      metascopeActivityService.createUpdateTableMetadataActivity(table, metascopeUserService.getUser().getUsername());
      if (oldTimestampField != dataTimestampField) {
        //TjobSchedulerService.updateLastDataForTable(tableEntity);
      }
    }
  }

  @Transactional
  public void increaseViewCount(String fqdn) {
    MetascopeTable table = metascopeTableRepository.findOne(fqdn);
    table.setViewCount(table.getViewCount() + 1);
    metascopeTableRepository.save(table);
  }

  @Transactional
  public String getLineage(MetascopeTable table) {
    return LineageUtil.getDependencyGraph(table);
  }

  @Async
  @Transactional
  public Future<HiveQueryResult> getSample(String fqdn, Map<String, String> params) {
    if (params == null || params.isEmpty()) {
      return new AsyncResult<HiveQueryResult>(sampleCache.getUnchecked(fqdn));
    } else {
      MetascopeTable table = metascopeTableRepository.findOne(fqdn);
      return new AsyncResult<HiveQueryResult>(hiveQueryExecutor.executeQuery(table.getDatabaseName(), table.getTableName(),
              table.getFieldsCommaDelimited(), table.getParameters(), params));
    }
  }

  public Page<MetascopeView> getRequestedViewPage(String fqdn, Pageable pageable) {
    return metascopeViewRepository.findByTableFqdnOrderByViewId(fqdn, pageable);
  }

  @Transactional
  public String getRandomParameterValue(MetascopeTable table, MetascopeField parameter) {
    MetascopeView viewEntity = metascopeViewRepository.findFirstByTableFqdn(table.getFqdn());
    return viewEntity.getParameters().get(parameter.getFieldName());
  }

  public List<MetascopeTable> getTransitiveDependencies(MetascopeTable table) {
    List<MetascopeTable> dependencies = new ArrayList<>();
    for (MetascopeTable dependency : table.getDependencies()) {
      getRecursiveDependencies(dependencies, dependency);
    }
    return dependencies;
  }

  private void getRecursiveDependencies(List<MetascopeTable> dependencies, MetascopeTable table) {
    if (!dependencies.contains(table)) {
      dependencies.add(table);
      for (MetascopeTable dependency : table.getDependencies()) {
        getRecursiveDependencies(dependencies, dependency);
      }
    }
  }

  public List<MetascopeTable> getTransitiveSuccessors(MetascopeTable table) {
    List<MetascopeTable> successors = new ArrayList<>();
    for (MetascopeTable successor : table.getSuccessors()) {
      getRecursiveSuccessors(successors, successor);
    }
    return successors;
  }

  private void getRecursiveSuccessors(List<MetascopeTable> successors, MetascopeTable table) {
    if (!successors.contains(table)) {
      successors.add(table);
      for (MetascopeTable successor : table.getSuccessors()) {
        getRecursiveSuccessors(successors, successor);
      }
    }
  }

  public Map<String, Set<String>> getParameterValues(MetascopeTable table) {
    Map<String, Set<String>> parameterValues = new HashMap<>();
    List<String> parameterStrings = metascopeViewRepository.findParameterStringsForTable(table.getFqdn());
    for (String parameterString : parameterStrings) {
      if (parameterString != null && !parameterString.isEmpty()) {
        String[] params = parameterString.split("/");
        for (int i = 1; i < params.length; i++) {
          String[] kv = params[i].split("=");
          Set<String> list = parameterValues.get(kv[0]);
          if (list == null) {
            list = new LinkedHashSet<>();
          }
          list.add(kv[1]);
          parameterValues.put(kv[0], list);
        }
      }
    }
    return parameterValues;
  }

}
