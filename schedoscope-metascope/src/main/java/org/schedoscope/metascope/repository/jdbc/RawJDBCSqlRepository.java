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
package org.schedoscope.metascope.repository.jdbc;

import org.schedoscope.metascope.model.*;
import org.schedoscope.metascope.repository.jdbc.entity.*;
import org.schedoscope.metascope.task.model.Dependency;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class RawJDBCSqlRepository {

    private final JDBCMetascopeTableRepository jdbcMetascopeTableRepository;
    private final JDBCMetascopeViewRepository jdbcMetascopeViewRepository;
    private final JDBCMetascopeFieldRepository jdbcMetascopeFieldRepository;
    private final JDBCMetascopeExportRepository jdbcMetascopeExportRepository;
    private final JDBCMetascopeMetadataRepository jdbcMetascopeMetadataRepository;

    public RawJDBCSqlRepository(boolean isMySQLDatabase, boolean isH2Database) {
        this.jdbcMetascopeTableRepository = new JDBCMetascopeTableRepository(isMySQLDatabase, isH2Database);
        this.jdbcMetascopeViewRepository = new JDBCMetascopeViewRepository(isMySQLDatabase, isH2Database);
        this.jdbcMetascopeFieldRepository = new JDBCMetascopeFieldRepository(isMySQLDatabase, isH2Database);
        this.jdbcMetascopeExportRepository = new JDBCMetascopeExportRepository(isMySQLDatabase, isH2Database);
        this.jdbcMetascopeMetadataRepository = new JDBCMetascopeMetadataRepository(isMySQLDatabase, isH2Database);
    }

    /*### MetascopeTable ###*/
    public MetascopeTable findTable(Connection connection, String fqdn) {
        return this.jdbcMetascopeTableRepository.findTable(connection, fqdn);
    }

    public void saveTable(Connection connection, MetascopeTable table) {
        this.jdbcMetascopeTableRepository.save(connection, table);
    }

    public void saveTransformation(Connection connection, MetascopeTransformation transformation, String fqdn) {
        this.jdbcMetascopeTableRepository.saveTransformation(connection, transformation, fqdn);
    }

    public void insertTableDependencies(Connection connection, Collection<MetascopeTable> currentTables, List<Dependency> tables) {
        this.jdbcMetascopeTableRepository.saveTableDependency(connection, currentTables, tables);
    }

    public List<MetascopeTable> findAllTables(Connection connection) {
        return this.jdbcMetascopeTableRepository.findAll(connection);
    }

    /*### MetascopeView ###*/
    public List<MetascopeView> findViews(Connection connection, String fqdn) {
        return this.jdbcMetascopeViewRepository.findAll(connection, fqdn);
    }

    public void insertOrUpdateViews(Connection connection, Iterable<MetascopeView> views) {
        this.jdbcMetascopeViewRepository.insertOrUpdateViews(connection, views);
    }

    public void insertOrUpdateViewMetadata(Connection connection, Iterable<MetascopeView> views) {
        this.jdbcMetascopeViewRepository.insertOrUpdateViewMetadata(connection, views);
    }

    public void insertViewDependencies(Connection connection, List<Dependency> viewDependencies) {
        this.jdbcMetascopeViewRepository.insertViewDependencies(connection, viewDependencies);
    }

    public void updateStatus(Connection connection, Iterable<MetascopeView> views) {
        this.jdbcMetascopeViewRepository.updateStatus(connection, views);
    }

    /*### MetascopeField ###*/
    public MetascopeField findField(Connection connection, String fieldFqdn) {
        return this.jdbcMetascopeFieldRepository.findField(connection, fieldFqdn);
    }

    public void saveFields(Connection connection, Set<MetascopeField> fields, String fqdn, boolean isParameter) {
        this.jdbcMetascopeFieldRepository.saveFields(connection, fields, fqdn, isParameter);
    }

    public void insertFieldDependencies(Connection connection, Collection<MetascopeTable> currentTables, List<Dependency> fieldDependencies) {
        this.jdbcMetascopeFieldRepository.insertFieldDependencies(connection, currentTables, fieldDependencies);
    }

    /*### MetascopeExport ###*/
    public MetascopeExport findExport(Connection connection, String exportFqdn) {
        return this.jdbcMetascopeExportRepository.findExport(connection, exportFqdn);
    }

    public void saveExports(Connection connection, List<MetascopeExport> exports, String fqdn) {
        this.jdbcMetascopeExportRepository.save(connection, exports, fqdn);
    }

    /*### MetascopeMetadata ###*/
    public void saveMetadata(Connection connection, String key, String value) {
        this.jdbcMetascopeMetadataRepository.saveMetadata(connection, key, value);
    }

}