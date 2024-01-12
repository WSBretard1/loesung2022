package org.texttechnologylab.project.musterloesung.data;

import org.apache.uima.UIMAException;
import org.apache.uima.jcas.JCas;

/**
 * Specialization of the interface: Text
 * @author Giuseppe Abrami
 * @see Text
 */
public interface Comment extends Text {
    JCas toCas() throws UIMAException;
}
