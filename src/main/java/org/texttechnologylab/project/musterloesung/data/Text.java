package org.texttechnologylab.project.musterloesung.data;

import org.apache.uima.jcas.JCas;

/**
 * Interface for mapping a text. A text is a segment of a speech.
 * @author Giuseppe Abrami
 */
public interface Text extends PlenaryObject {

    /**
     * Return the speech the text belongs to
     * @return
     */
    Speech getSpeech();

    /**
     * Return the speaker of this text
     * @return
     */
    Speaker getSpeaker();

    /**
     * Set the speech the text belongs to
     * @param pSpeech
     */
    void setSpeech(Speech pSpeech);

    /**
     * Set the speaker of this text
     * @param pSpeaker
     */
    void setSpeaker(Speaker pSpeaker);

    /**
     * Return the content of this text
     * @return
     */
    String getContent();

}
