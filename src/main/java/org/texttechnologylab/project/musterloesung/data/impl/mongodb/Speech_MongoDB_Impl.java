package org.texttechnologylab.project.musterloesung.data.impl.mongodb;

import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.bson.Document;
import org.hucompute.textimager.uima.type.Sentiment;
import org.hucompute.textimager.uima.type.category.CategoryCoveredTagged;
import org.texttechnologylab.annotation.AnnotationComment;
import org.texttechnologylab.project.musterloesung.data.*;
import org.texttechnologylab.project.musterloesung.data.impl.file.Speech_File_Impl;
import org.texttechnologylab.project.musterloesung.helper.NLPHelper;
import org.texttechnologylab.uimadb.UIMADatabaseInterface;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Giuseppe Abrami
 */
public class Speech_MongoDB_Impl extends Speech_File_Impl implements Speech, NLPDocument {

    private Document pDocument = null;

    private JCas pCas = null;

    public Speech_MongoDB_Impl(ParliamentFactory pFactory, Document pMongoDocument) {
        super(pFactory);
        this.pDocument = pMongoDocument;
    }

    public Document getDocument(){
        return pDocument;
    }

    public Speech_MongoDB_Impl(AgendaItem pAgenda, Node pNode) {
        super(pAgenda, pNode);
    }

    public Speech_MongoDB_Impl(AgendaItem pAgenda, String sID) {
        super(pAgenda, sID);
    }

    @Override
    public String getID() {
        return pDocument.getString("_id");
    }

    @Override
    public String getText() {
        return pDocument.getString("text");
    }

    @Override
    public String getPlainText() {
        return getText();
    }

    @Override
    public Speaker getSpeaker() {
        String sSpeaker = this.pDocument.getString("speaker");

        return getFactory().getSpeaker(sSpeaker);

    }

    @Override
    public List<Comment> getComments() {

        List<Comment> rList = new ArrayList<>(0);

        this.pDocument.getList("comments", String.class);

        return rList;
    }

    @Override
    public PlenaryProtocol getProtocol() {
        return new PlenaryProtocol_MongoDB_Impl(this.getFactory(), (Document) pDocument.get("protocol"));
    }

    public void update(){
        try {
            this.pFactory.updateSpeech(this);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public AgendaItem getAgendaItem() {
        return new AgendaItem_MongoDB_Impl(getProtocol(), pDocument.get("agenda", Document.class));
    }

    @Override
    public JCas toCas() throws UIMAException {

        if(this.pCas==null){
            if (pDocument.containsKey("uima")) {
                pCas = UIMADatabaseInterface.deserializeJCas(pDocument.getString("uima"));
            } else {
                pCas = JCasFactory.createText(pDocument.getString("text"), "de");
            }
        }

        return pCas;

    }

    @Override
    public JCas toCas(boolean bNLP) throws UIMAException {

        if(this.pCas==null){
            if (pDocument.containsKey("uima")) {
                pCas = UIMADatabaseInterface.deserializeJCas(pDocument.getString("uima"));
            } else {
                pCas = JCasFactory.createText(pDocument.getString("text"), "de");
                if(bNLP){
                    NLPHelper nlpHelper = new NLPHelper();
                    pCas = nlpHelper.analyse(this);
                }
            }
        }
        else{
            if(JCasUtil.selectAll(pCas).size()<=1 && bNLP){
                NLPHelper nlpHelper = new NLPHelper();
                pCas = nlpHelper.analyse(this);
            }
        }

        return pCas;

    }

    @Override
    public void setCas(JCas pCas) {
        this.pCas = pCas;
    }

    @Override
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
}
