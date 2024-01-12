package org.texttechnologylab.project.musterloesung.data;

import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.UIMAException;
import org.apache.uima.jcas.JCas;
import org.hucompute.textimager.uima.type.Sentiment;
import org.hucompute.textimager.uima.type.category.CategoryCoveredTagged;

import java.util.List;
import java.util.Set;

/**
 * Interface for interacting with NLP information
 * @author Giuseppe Abrami
 */
public interface NLPDocument {

    /**
     * Transform to JCas
     * @return
     * @throws UIMAException
     */
    JCas toCas() throws UIMAException;

    /**
     * Transform to JCas and if boolean is true, preprocess if not already done
     * @param bNLP
     * @return
     * @throws UIMAException
     */
    JCas toCas(boolean bNLP) throws UIMAException;

    /**
     * Get Named Entities
     * @return
     */
    List<NamedEntity> getNamedEntities();

    /**
     * Get Token
     * @return
     */
    List<Token> getTokens();

    /**
     * Get POS
     * @return
     */
    List<POS> getPOS();

    /**
     * Get Sentiments
     * @return
     */
    List<Sentiment> getSentiments();

    /**
     * Get DDC
     * @return
     */
    List<CategoryCoveredTagged> getDDC();

    /**
     * Get Document Sentiment
     * @return
     */
    Sentiment getDocumentSentiment();

    /**
     * Get Lemmas
     * @return
     */
    List<Lemma> getLemmas();

    /**
     * Get Sentences
     * @return
     */
    List<Sentence> getSentences();

    /**
     * Is NLP Procecced?
     * @return
     */
    boolean isProcessed();

    /**
     * Has the Document an uima attribute?
     * @return
     */
    boolean hasUIMA();
}
