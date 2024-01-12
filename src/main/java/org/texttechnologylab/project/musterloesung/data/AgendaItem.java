package org.texttechnologylab.project.musterloesung.data;

import java.util.List;
import java.util.Set;

/**
 * Class for mapping agenda items
 * @author Giuseppe Abrami
 */
public interface AgendaItem extends PlenaryObject {

    /**
     * Get all Speeches
     * @return
     */
    List<Speech> getSpeeches();

    /**
     * Add a speech to this agenda
     * @param pValue
     */
    void addSpeech(Speech pValue);

    /**
     * Add multiple speeches to this agenda
     * @param pSet
     */
    void addSpeeches(Set<Speech> pSet);

    /**
     * Return the index of the agenda
     * @return
     */
    String getIndex();

    /**
     * Set the index of the agenda
     * @param pValue
     */
    void setIndex(String pValue);

    /**
     * Get the title of the agenda
     * @return
     */
    String getTitle();

    /**
     * Set the title of the agenda
     * @param sValue
     */
    void setTitle(String sValue);

    /**
     * Return the protocol to which the agenda item belongs.
     * @return
     */
    PlenaryProtocol getProtocol();

}
