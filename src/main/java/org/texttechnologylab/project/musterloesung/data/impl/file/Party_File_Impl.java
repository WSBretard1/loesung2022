package org.texttechnologylab.project.musterloesung.data.impl.file;

import org.texttechnologylab.project.musterloesung.data.Party;
import org.texttechnologylab.project.musterloesung.data.Speaker;

import java.util.HashSet;
import java.util.Set;

/**
 * Implementation of the representation of a party
 * @author Giuseppe Abrami
 */
public class Party_File_Impl implements Party {

    private String sName = "";
    private Set<Speaker> pMembers = new HashSet<>(0);

    /**
     * Constructor based on the Name of a Party
     * @param sName
     */
    public Party_File_Impl(String sName){
        this.setName(sName);
    }

    @Override
    public String getName() {
        return this.sName;
    }

    @Override
    public void setName(String sValue) {
        this.sName = sValue;
    }

    @Override
    public Set<Speaker> getMembers() {
        return this.pMembers;
    }

    @Override
    public void addMember(Speaker pMember) {
        this.pMembers.add(pMember);
    }

    @Override
    public void addMembers(Set<Speaker> pSet) {
        this.pMembers.addAll(pSet);
    }

    @Override
    public int compareTo(Party party) {
        return this.getName().compareTo(party.getName());
    }

    @Override
    public boolean equals(Object o) {
        return this.hashCode() == o.hashCode();
    }

    @Override
    public int hashCode() {
        return this.getName().hashCode();
    }

    @Override
    public String toString() {
        return this.getName();
    }
}
