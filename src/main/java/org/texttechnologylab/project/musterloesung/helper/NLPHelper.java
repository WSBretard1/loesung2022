package org.texttechnologylab.project.musterloesung.helper;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS_VERB;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Lemma;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.bson.BsonDocument;
import org.bson.Document;
import org.hucompute.textimager.fasttext.labelannotator.LabelAnnotatorDocker;
import org.hucompute.textimager.uima.gervader.GerVaderSentiment;
import org.hucompute.textimager.uima.spacy.SpaCyMultiTagger3;
import org.hucompute.textimager.uima.type.category.CategoryCoveredTagged;
import org.junit.Test;
import org.texttechnologylab.annotation.AnnotationComment;
import org.texttechnologylab.project.musterloesung.ParAnSy;
import org.texttechnologylab.project.musterloesung.data.Comment;
import org.texttechnologylab.project.musterloesung.data.NLPDocument;
import org.texttechnologylab.project.musterloesung.data.ParliamentFactory;
import org.texttechnologylab.project.musterloesung.data.Speech;
import org.texttechnologylab.project.musterloesung.data.impl.file.ParliamentFactory_Impl;
import org.texttechnologylab.project.musterloesung.data.impl.mongodb.Comment_MongoDB_Impl;
import org.texttechnologylab.project.musterloesung.data.impl.mongodb.Speech_MongoDB_Impl;
import org.texttechnologylab.project.musterloesung.database.MongoDBConfig;
import org.texttechnologylab.project.musterloesung.database.MongoDBConnectionHandler;
import org.texttechnologylab.project.musterloesung.exceptions.ParamException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

/**
 * Class for encapsulating NLP methods and processing.
 * @author Giuseppe Abrami
 * @date 10.12.2022
 */
public class NLPHelper {

    /**
     * Method to supress Logging
     */
    public static void setupLogging() {
        System.setProperty("org.apache.commons.logging.Log",
                "org.apache.commons.logging.impl.NoOpLog");
    }

    // AnalysisEngine for performing the processing. Is initialized only once.
    private AnalysisEngine pAE = null;

    /**
     * Constructor
     */
    public NLPHelper(){
        setupLogging();
        //  creating a Pipeline
        AggregateBuilder pipeline = new AggregateBuilder();

        // add different Engines to the Pipeline
        try {
            pipeline.add(createEngineDescription(SpaCyMultiTagger3.class,
                SpaCyMultiTagger3.PARAM_REST_ENDPOINT, "http://spacy.lehre.texttechnologylab.org"

            ));
            String sPOSMapFile = ParAnSy.class.getClassLoader().getResource("am_posmap.txt").getPath();

            pipeline.add(createEngineDescription(LabelAnnotatorDocker.class,
                    LabelAnnotatorDocker.PARAM_FASTTEXT_K, 100,
                    LabelAnnotatorDocker.PARAM_CUTOFF, false,
                    LabelAnnotatorDocker.PARAM_SELECTION, "text",
                    LabelAnnotatorDocker.PARAM_TAGS, "ddc3",
                    LabelAnnotatorDocker.PARAM_USE_LEMMA, true,
                    LabelAnnotatorDocker.PARAM_ADD_POS, true,
                    LabelAnnotatorDocker.PARAM_POSMAP_LOCATION, sPOSMapFile,
                    LabelAnnotatorDocker.PARAM_REMOVE_FUNCTIONWORDS, true,
                    LabelAnnotatorDocker.PARAM_REMOVE_PUNCT, true,
                    LabelAnnotatorDocker.PARAM_REST_ENDPOINT, "http://ddc.lehre.texttechnologylab.org"
            ));

            pipeline.add(createEngineDescription(GerVaderSentiment.class,
                    GerVaderSentiment.PARAM_REST_ENDPOINT, "http://gervader.lehre.texttechnologylab.org",
                GerVaderSentiment.PARAM_SELECTION, "text,de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence"
            ));

            // create an AnalysisEngine for running the Pipeline.
            pAE = pipeline.createAggregate();
        } catch (ResourceInitializationException e) {
            e.printStackTrace();
        }

    }

    /**
     * Another Test-Function for Students :-)
     * @throws UIMAException
     */
    @Test
    public void test() throws UIMAException {
        NLPHelper nlp = new NLPHelper();
        JCas tCas = JCasFactory.createText("Diese Blume ist sehr schön!", "de");
        SimplePipeline.runPipeline(tCas, nlp.getEngine());
        JCasUtil.select(tCas, CategoryCoveredTagged.class).forEach(t->{
            System.out.println(t);
        });
    }

    /**
     * Method to return the AnalysisEngine
     * @return
     */
    public AnalysisEngine getEngine(){
        return this.pAE;
    }

    /**
     * Method zu process the Text of a Speech
     * @param pSpeech
     * @return JCas
     * @throws UIMAException
     */
    public JCas analyse(Speech pSpeech) throws UIMAException {

        // get JCas of Speech
        JCas jCas = pSpeech.toCas();

        // run default pipeline
        try {
            SimplePipeline.runPipeline(jCas, this.getEngine());
        } catch (UIMAException e) {
            e.printStackTrace();
        }

        pSpeech.setCas(jCas);

        // return the resulting JCas
        return jCas;

    }

    /**
     * Method zu process the Text of a Comment
     * @param pComment
     * @return JCas
     * @throws UIMAException
     */
    public JCas analyse(Comment pComment) throws UIMAException {

        // get JCas of Comment
        JCas jCas = pComment.toCas();

        // run default pipeline
        try {
            SimplePipeline.runPipeline(jCas, this.getEngine());
        } catch (UIMAException e) {
            e.printStackTrace();
        }

        // return the resulting JCas
        return jCas;

    }

    /**
     * Helper Method
     * @param iSet
     * @return
     */
    public static List<Document> annotationsToDocumentList(List<Annotation> iSet){

        List<Document> sList = new ArrayList<>(0);

        iSet.forEach(i->{
            Document pDocument = new Document();
            pDocument.put("begin", i.getBegin());
            pDocument.put("end", i.getEnd());
            if(i instanceof POS){
                pDocument.put("value", ((POS) i).getPosValue());
                pDocument.put("type", i.getType().getShortName());
            }
            else if (i instanceof CategoryCoveredTagged){
                CategoryCoveredTagged pTemp = (CategoryCoveredTagged)i;
                pDocument.put("value", pTemp.getValue());
                pDocument.put("score", pTemp.getScore());
            }
            else if (i instanceof Lemma){
                Lemma pTemp = (Lemma)i;
                pDocument.put("value", pTemp.getValue());
                POS p = JCasUtil.selectCovered(POS.class, i).get(0);
                if (p != null) {
                    pDocument.put("pos", p.getType().getShortName());
                }
            }
            else if (i instanceof org.hucompute.textimager.uima.type.GerVaderSentiment){
                org.hucompute.textimager.uima.type.GerVaderSentiment pTemp = (org.hucompute.textimager.uima.type.GerVaderSentiment) i;
                pDocument.put("value", pTemp.getSentiment());
                pDocument.put("subjectivity", pTemp.getSubjectivity());
                pDocument.put("positive", pTemp.getPos());
                pDocument.put("negative", pTemp.getNeg());
                pDocument.put("neutral", pTemp.getNeu());
            }
            else{
                pDocument.put("value", i.getCoveredText());
            }
            sList.add(pDocument);

        });

        return sList;

    }

    /**
     * Test-Method for Students :-)
     * @throws IOException
     */
    @Test
    public void testNLP() throws IOException {

        String pTarget = ParAnSy.class.getClassLoader().getResource("dbconnection_rw_target.txt").getPath();
        MongoDBConfig dbConfigTarget = new MongoDBConfig(pTarget);
        MongoDBConnectionHandler targetDB = new MongoDBConnectionHandler(dbConfigTarget);
        ParliamentFactory pFactory = new ParliamentFactory_Impl(targetDB);

        Speech pSpeech = pFactory.getSpeech("ID1917800100");
        NLPDocument nlp = (NLPDocument)pSpeech;
        try {
            JCas pCas = nlp.toCas(true);

            Set<String> verbs = new HashSet<>(0);
            JCasUtil.select(pCas, POS_VERB.class).forEach(t->{
                verbs.add(t.getPosValue());
            });
            verbs.forEach(v->{
                System.out.println(v);
            });

            JCasUtil.select(pCas, AnnotationComment.class).stream().filter(ac->{
                if(ac.getReference() instanceof org.hucompute.textimager.uima.type.GerVaderSentiment){
                    return true;
                }
                return false;
            }).filter(ac->{
                return ac.getValue().equalsIgnoreCase("text");
            }).forEach(ac->
            {
                System.out.println(ac.getReference());
                System.out.println(ac.getKey()+"\t"+ac.getValue());
                System.out.println("================================");
            });

        } catch (UIMAException e) {
            throw new RuntimeException(e);
        }
        nlp.getSentiments().forEach(s->{
            System.out.println(s);
        });

    }

    /**
     * Method to Update NLP-Informations and Preprocess if not already done.
     * The idea here is, all documents, are checked during the update, if they are already NLP processed, if yes, the NLP information is read out and set. If not, an NLP analysis is performed beforehand.
     * @param pFactory
     * @param sCollection
     */
    public static void processNLP(ParliamentFactory pFactory, String sCollection){

        MongoCollection pCollection = pFactory.getMongoConnection().getCollection(sCollection);
        int iCount=0;
        int iSkip=500;

        boolean isFinish = true;

        while(isFinish){
            try {
                MongoCursor<Document> pDocuments = pCollection.find(BsonDocument.parse("{}")).skip(iSkip*iCount).limit(iSkip).sort(BsonDocument.parse("{_id : 1}")).cursor();

                isFinish = pDocuments.hasNext();

                pDocuments.forEachRemaining(d->{

                    if(sCollection.equalsIgnoreCase("comment")){
                        Comment pComment = new Comment_MongoDB_Impl(pFactory, d);
                        try {
                            pFactory.updateComment(pComment);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    else if(sCollection.equalsIgnoreCase("speech")){
                        Speech pSpeech = new Speech_MongoDB_Impl(pFactory, d);
                        try {
                            pFactory.updateSpeech(pSpeech);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    else{
                        try {
                            throw new ParamException("Collection is invalid!");
                        } catch (ParamException e) {
                            throw new RuntimeException(e);
                        }
                    }


                });
            }
            catch (Exception e){
                e.printStackTrace();
                System.out.println(iCount);
            }
            iCount++;
        }


    }

}
