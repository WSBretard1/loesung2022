package org.texttechnologylab.project.musterloesung.data;

import org.apache.uima.UIMAException;
import org.apache.uima.jcas.JCas;

import java.util.List;

/**
 * Interface for mapping a speech
 * @author Giuseppe Abrami
 */
public interface Speech extends PlenaryObject {

    /**
     * Return the agenda item the speech belongs to
     * @return
     */
    AgendaItem getAgendaItem();

    /**
     * Return all comments in this speech
     * @return
     */
    List<Comment> getComments();

    /**
     * Return the full text of the speech
     * @return
     */
    String getText();

    String getPlainText();

    /**
     * Return the plenary protocol this speechs belongs to
     * @return
     */
    PlenaryProtocol getProtocol();

    /**
     * Return the speaker of tis speech
     * @return
     */
    Speaker getSpeaker();

    /**
     * Set the speaker of this speech
     * @param pSpeaker
     */
    void setSpeaker(Speaker pSpeaker);

    /**
     * Return the length of this speech
     * @return
     */
    int getLength();

    /**
     * Return all insertions which are no comments
     * @return
     */
    List<Speech> getInsertions();

    /**
     * Add a text-segment to the speech
     * @param pText
     */
    void addText(Text pText);

    JCas toCas() throws UIMAException;

    void setCas(JCas pCas);
}
