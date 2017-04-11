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
package org.schedoscope.metascope.model;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

@Entity
public class MetascopeUser {

    public enum Role {
        ROLE_ADMIN, ROLE_USER
    }

    public enum Group {
        BUSINESS_USER, TECHNICAL_USER, OPS_USER
    }

    @Id
    private String username;
    private String fullname;
    private String email;
    private String passwordHash;
    @Enumerated(EnumType.STRING)
    private Group usergroup;
    @Enumerated(EnumType.STRING)
    private Role userrole;

    @ElementCollection
    private List<String> favourites;

    public MetascopeUser() {
        this.favourites = new ArrayList<String>();
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getFullname() {
        return fullname;
    }

    public void setFullname(String fullname) {
        this.fullname = fullname;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPasswordHash() {
        return passwordHash;
    }

    public void setPasswordHash(String passwordHash) {
        this.passwordHash = passwordHash;
    }

    public Group getUsergroup() {
        return usergroup;
    }

    public void setUsergroup(Group usergroup) {
        this.usergroup = usergroup;
    }

    public Role getUserrole() {
        return userrole;
    }

    public void setUserrole(Role userrole) {
        this.userrole = userrole;
    }

    public void setUserrole(String roleString) {
        Role role = Role.valueOf(roleString);
        if (role != null) {
            this.userrole = role;
        }
    }

    public void setUsergroup(String groupString) {
        if (groupString == null) {
            this.usergroup = Group.BUSINESS_USER;
            return;
        }
        Group group = Group.valueOf(groupString);
        if (group != null) {
            this.usergroup = group;
        }
    }

    public List<String> getFavourites() {
        return favourites;
    }

    public void setFavourites(List<String> favourites) {
        this.favourites = favourites;
    }

    public boolean isAdmin() {
        return userrole.equals(Role.ROLE_ADMIN);
    }

}
