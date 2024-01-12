package org.texttechnologylab.project.musterloesung.data;

import java.util.Set;

/**
 * Interface for mapping parties
 * @author Giuseppe Abrami
 */
public interface Party extends Comparable<Party> {

    /**
     * Return the name of the party
     * @return
     */
    String getName();

    /**
     * Set the name of the party
     * @param sValue
     */
    void setName(String sValue);

    /**
     * Return all members of the party
     * @return
     */
    Set<Speaker> getMembers();

    /**
     * Add a speaker as a member of the party
     * @param pMember
     */
    void addMember(Speaker pMember);

    /**
     * Add multiple speakers as members of the party
     * @param pSet
     */
    void addMembers(Set<Speaker> pSet);


}
