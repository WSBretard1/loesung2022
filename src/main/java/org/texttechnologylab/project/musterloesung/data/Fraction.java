package org.texttechnologylab.project.musterloesung.data;

import java.util.Set;

/**
 * Interface for mapping fractions
 * @author Giuseppe Abrami
 */
public interface Fraction extends Comparable<Fraction> {

    /**
     * Return the Name of the Fraction
     * @return
     */
    String getName();

    /**
     * Add a Member of the Fraction
     * @param pSpeaker
     */
    void addMember(Speaker pSpeaker);

    /**
     * Get all Members of the Fraction
     * @return
     */
    Set<Speaker> getMembers();

}
