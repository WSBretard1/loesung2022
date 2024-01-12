package org.texttechnologylab.project.musterloesung.data.impl.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.bson.BsonDocument;
import org.bson.Document;
import org.hucompute.textimager.uima.type.Sentiment;
import org.hucompute.textimager.uima.type.category.CategoryCoveredTagged;
import org.texttechnologylab.annotation.AnnotationComment;
import org.texttechnologylab.project.musterloesung.data.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Giuseppe Abrami
 */
public class Comment_MongoDB_Impl extends Text_MongoDB_Impl implements Comment, NLPDocument {

    private JCas pCas = null;

    public Comment_MongoDB_Impl(ParliamentFactory pFactory, Speaker pSpeaker, Speech pSpeech, String sText) {
        super(pFactory, pSpeaker, pSpeech, sText);
    }

    public Comment_MongoDB_Impl(ParliamentFactory pFactory, Document pDocument) {
        super(pFactory, pDocument);
    }

    @Override
    public String getContent() {
        return pDocument.getString("text");
    }

    @Override
    public Speaker getSpeaker() {
        return this.getFactory().getSpeaker(pDocument.getString("speaker"));
    }

    @Override
    public Speech getSpeech() {
        if(pDocument.containsKey("speach")){
            pDocument.put("speech", pDocument.getString("speach"));
        }
        return this.getFactory().getSpeech(pDocument.getString("speech"));
    }

    @Override
    public JCas toCas() throws UIMAException {
        return toCas(false);
    }

    @Override
    public JCas toCas(boolean bNLP) throws UIMAException {
        if(this.pCas==null){
            JCas pCas = JCasFactory.createJCas();

            pCas.setDocumentText(this.getContent());
            pCas.setDocumentLanguage("de");

            this.pCas = pCas;
        }

        if(bNLP && JCasUtil.selectAll(pCas).size()<=1){
            pCas = this.getFactory().getNLPHelper().analyse(this);
        }

        return pCas;
    }

    public List<NamedEntity> getNamedEntities() {
        try {
            return JCasUtil.select(this.toCas(), NamedEntity.class).stream().collect(Collectors.toList());
        } catch (UIMAException e) {
            e.printStackTrace();
        }
        return new ArrayList<>(0);
    }

    @Override
    public List<Token> getTokens() {
        try {
            return JCasUtil.select(this.toCas(), Token.class).stream().collect(Collectors.toList());
        } catch (UIMAException e) {
            e.printStackTrace();
        }
        return new ArrayList<>(0);
    }

    @Override
    public List<POS> getPOS() {
        try {
            return JCasUtil.select(this.toCas(), POS.class).stream().collect(Collectors.toList());
        } catch (UIMAException e) {
            e.printStackTrace();
        }
        return new ArrayList<>(0);
    }

    @Override
    public List<Sentiment> getSentiments() {
        try {
            return JCasUtil.select(this.toCas(), Sentiment.class).stream().collect(Collectors.toList());
        } catch (UIMAException e) {
            e.printStackTrace();
        }
        return new ArrayList<>(0);
    }

    @Override
    public List<CategoryCoveredTagged> getDDC() {
        try {
            return JCasUtil.select(this.toCas(), CategoryCoveredTagged.class).stream().collect(Collectors.toList());
        } catch (UIMAException e) {
            e.printStackTrace();
        }
        return new ArrayList<>(0);
    }

    @Override
    public Sentiment getDocumentSentiment() {
        try {
            List<AnnotationComment> annoComments = JCasUtil.select(this.toCas(), AnnotationComment.class).stream().filter(d->{

                if(d.getReference() instanceof Sentiment){
                    return d.getValue().equalsIgnoreCase("text");
                }
                return false;

            }).collect(Collectors.toList());

            if(annoComments.size()>=1){
                return (Sentiment)annoComments.get(0).getReference();
            }

        } catch (UIMAException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<Lemma> getLemmas() {
        try {
            return JCasUtil.select(this.toCas(), Lemma.class).stream().collect(Collectors.toList());
        } catch (UIMAException e) {
            e.printStackTrace();
        }
        return new ArrayList<>(0);
    }

    @Override
    public List<Sentence> getSentences() {
        try {
            return JCasUtil.select(this.toCas(), Sentence.class).stream().collect(Collectors.toList());
        } catch (UIMAException e) {
            e.printStackTrace();
        }
        return new ArrayList<>(0);
    }

    @Override
    public boolean isProcessed() {
        try {
            return JCasUtil.selectAll(toCas()).size()>1;
        } catch (UIMAException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean hasUIMA() {
        return pDocument.containsKey("uima");
    }

    @Override
    public String getID() {
        return pDocument.getString("_id");
    }


}
