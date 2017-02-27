package org.schedoscope.metascope.model;

import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.LazyCollection;
import org.hibernate.annotations.LazyCollectionOption;

import javax.persistence.*;
import java.util.*;

/**
 * Created by kas on 22.11.16.
 */
@Entity
public class MetascopeView {

    /* fields */
    @Id
    @Column(columnDefinition = "varchar(766)")
    private String viewId;
    @Column(columnDefinition = "varchar(5000)")
    private String viewUrl;
    @Column(columnDefinition = "varchar(5000)")
    private String parameterString;
    @Column(columnDefinition = "bigint default 0")
    private long numRows;
    @Column(columnDefinition = "bigint default 0")
    private long totalSize;
    @Column(columnDefinition = "bigint default 0")
    private long lastTransformation;

    @ManyToMany(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    @JoinTable(name = "metascope_view_relationship",
            joinColumns = @JoinColumn(name = "dependency"),
            inverseJoinColumns = @JoinColumn(name = "successor"),
            uniqueConstraints = @UniqueConstraint(columnNames = {"dependency", "successor"})
    )
    private List<MetascopeView> dependencies;

    @ManyToMany(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    @JoinTable(name = "metascope_view_relationship",
            joinColumns = @JoinColumn(name = "successor"),
            inverseJoinColumns = @JoinColumn(name = "dependency"),
            uniqueConstraints = @UniqueConstraint(columnNames = {"dependency", "successor"})
    )
    private List<MetascopeView> successors;

    @ManyToOne(fetch = FetchType.EAGER)
    private MetascopeTable table;

    public String getViewId() {
        return viewId;
    }

    public void setViewId(String viewId) {
        this.viewId = viewId;
    }

    public String getViewUrl() {
        return viewUrl;
    }

    public void setViewUrl(String viewUrl) {
        this.viewUrl = viewUrl;
    }

    public List<MetascopeView> getDependencies() {
        return dependencies;
    }

    public void setDependencies(List<MetascopeView> dependencies) {
        this.dependencies = dependencies;
    }

    public List<MetascopeView> getSuccessors() {
        return successors;
    }

    public void setSuccessors(List<MetascopeView> successors) {
        this.successors = successors;
    }

    public String getParameterString() {
        return parameterString;
    }

    public void setParameterString(String parameterString) {
        this.parameterString = parameterString;
    }

    public MetascopeTable getTable() {
        return table;
    }

    public void setTable(MetascopeTable table) {
        this.table = table;
    }

    public long getNumRows() {
        return numRows;
    }

    public void setNumRows(long numRows) {
        this.numRows = numRows;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(long totalSize) {
        this.totalSize = totalSize;
    }

    public long getLastTransformation() {
        return lastTransformation;
    }

    public void setLastTransformation(long lastTransformation) {
        this.lastTransformation = lastTransformation;
    }

    public void addToDependencies(MetascopeView view) {
        if (dependencies == null) {
            this.dependencies = new ArrayList<>();
        }
        if (!dependencies.contains(view)) {
            this.dependencies.add(view);
        }
    }

    public void addToSuccessors(MetascopeView view) {
        if (successors == null) {
            this.successors = new ArrayList<>();
        }
        if (!successors.contains(view)) {
            this.successors.add(view);
        }
    }

    public String getValueForParameterName(String name) {
        Map<String, String> parameter = getParameters();
        for (Map.Entry<String, String> e : parameter.entrySet()) {
            if (e.getKey().equals(name)) {
                return e.getValue();
            }
        }
        return null;
    }

    public Map<String, String> getParameters() {
        Map<String, String> paramMap = new HashMap<>();
        if (parameterString != null && !parameterString.isEmpty()) {
            String[] params = parameterString.split("/");
            for (int i = 1; i < params.length; i++) {
                String[] kv = params[i].split("=");
                paramMap.put(kv[0], kv[1]);
            }
        }
        return paramMap;
    }

    public List<String> getParameterValues() {
        List<String> parameters = new ArrayList<>();
        if (parameterString != null && !parameterString.isEmpty()) {
            String[] params = parameterString.split("/");
            for (int i = 1; i < params.length; i++) {
                String[] kv = params[i].split("=");
                parameters.add(kv[1]);
            }
        }
        return parameters;
    }

    public String internalViewId() {
        String internalViewId = null;
        for (String value : getParameterValues()) {
            if (internalViewId == null) {
                internalViewId = new String();
            }
            internalViewId += value;
        }
        if (internalViewId == null) {
            return "root";
        }
        return internalViewId;
    }

    public Map<MetascopeTable, Set<MetascopeView>> getDependencyMap() {
        Map<MetascopeTable, Set<MetascopeView>> depMap = new HashMap<>();
        for (MetascopeView dependency : dependencies) {
            Set<MetascopeView> metascopeViews = depMap.get(dependency.getTable());
            if (metascopeViews == null) {
                metascopeViews = new HashSet<>();
            }
            metascopeViews.add(dependency);
            depMap.put(dependency.getTable(), metascopeViews);
        }
        return depMap;
    }

    public Map<MetascopeTable, Set<MetascopeView>> getSuccessorMap() {
        Map<MetascopeTable, Set<MetascopeView>> sucMap = new HashMap<>();
        for (MetascopeView successor : successors) {
            Set<MetascopeView> metascopeViews = sucMap.get(successor.getTable());
            if (metascopeViews == null) {
                metascopeViews = new HashSet<>();
            }
            metascopeViews.add(successor);
            sucMap.put(successor.getTable(), metascopeViews);
        }
        return sucMap;
    }

}
