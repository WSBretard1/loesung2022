package org.texttechnologylab.project.musterloesung.database;

import com.mongodb.BasicDBObject;
import org.apache.uima.UIMAException;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.bson.Document;
import org.json.JSONException;
import org.json.JSONObject;
import org.texttechnologylab.project.musterloesung.data.*;
import org.texttechnologylab.project.musterloesung.helper.NLPHelper;
import org.texttechnologylab.uimadb.wrapper.mongo.MongoSerialization;
import org.texttechnologylab.uimadb.wrapper.mongo.serilization.exceptions.CasSerializationException;
import org.texttechnologylab.uimadb.wrapper.mongo.serilization.exceptions.SerializerInitializationException;
import org.texttechnologylab.uimadb.wrapper.mongo.serilization.exceptions.UnknownFactoryException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class for MongoHelper Methods
 * @author Giuseppe Abrami
 */
public class MongoHelper {

    public static NLPHelper nlpHelper = new NLPHelper();

    public static final int MAX_DOCUMENT_SIZE = 1500000;


    public static Document toMongoDocument(Speaker speaker)  {

        Document mongoDocument = new Document();
        mongoDocument.put("_id", speaker.getID());
        mongoDocument.put("name", speaker.getName());
        mongoDocument.put("firstName", speaker.getFirstName());
        mongoDocument.put("title", speaker.getTitle());
        mongoDocument.put("geburtsdatum", speaker.getGeburtsdatum());
        mongoDocument.put("geburtsort", speaker.getGeburtsort());
        mongoDocument.put("sterbedatum", speaker.getSterbedatum());
        mongoDocument.put("geschlecht", speaker.getGeschlecht());
        mongoDocument.put("beruf", speaker.getBeruf());
        mongoDocument.put("akademischertitel", speaker.getAkademischerTitel());
        mongoDocument.put("familienstand", speaker.getFamilienstand());
        mongoDocument.put("religion", speaker.getReligion());

        List<Integer> iAbsendes = new ArrayList<>();
        for (PlenaryProtocol absence : speaker.getAbsences()) {
            iAbsendes.add(absence.getIndex());
        }

        mongoDocument.put("absence", iAbsendes);
        if(speaker.getParty()!=null){
            mongoDocument.put("party", speaker.getParty().getName());
        }
        if(speaker.getFraction()!=null){
            mongoDocument.put("fraction", speaker.getFraction().getName());
        }
        mongoDocument.put("role", speaker.getRole());
        return mongoDocument;


    }

    /**
     * Method to convert a Speech to an MongoDocument
     * @param pSpeech
     * @return
     */
    public static Document toMongoDocument(Speech pSpeech) throws JSONException, UIMAException {

        // creating a empty MongoDocument and add attributes
        Document mongoDocument = new Document();
        mongoDocument.put("_id", pSpeech.getID());
        mongoDocument.put("text", pSpeech.getPlainText());
        mongoDocument.put("speaker", pSpeech.getSpeaker().getID());

        BasicDBObject protocolObject = new BasicDBObject();
        PlenaryProtocol pProtocol = pSpeech.getProtocol();

        protocolObject.put("date", pProtocol.getDate().getTime());
        protocolObject.put("starttime", pProtocol.getStartTime().getTime());
        protocolObject.put("endtime", pProtocol.getEndTime().getTime());
        protocolObject.put("index", pProtocol.getIndex());
        protocolObject.put("title", pProtocol.getTitle());
        protocolObject.put("place", pProtocol.getPlace());
        protocolObject.put("wp", pProtocol.getWahlperiode());

        mongoDocument.put("protocol", protocolObject);

        AgendaItem pItem = pSpeech.getAgendaItem();

        JSONObject agendaItem = new JSONObject();
        agendaItem.put("id", pItem.getID());
        agendaItem.put("index", pItem.getIndex());
        agendaItem.put("title", pItem.getTitle());

        List comments = new ArrayList<>();

        for (Comment c : pSpeech.getComments()) {
            comments.add(c.getID());
        }

        mongoDocument.put("comments", comments);
        mongoDocument.put("agenda", Document.parse(agendaItem.toString()));

        mongoDocument.put("speaker", pSpeech.getSpeaker().getID());

        // Extract the CAS from the document
        JCas pCas = pSpeech.toCas();

        // Check if already processed with NLP
        if (JCasUtil.selectAll(pCas).size() <= 1) {
            nlpHelper.analyse(pSpeech);
            pCas = pSpeech.toCas();

        }

        // If CAS is not null, then proceed
        if (pCas != null) {
            String sJCas = null;
            // Serialize the CAS
            try {
                sJCas = MongoSerialization.serializeJCas(pSpeech.toCas());
            } catch (UnknownFactoryException e) {
                e.printStackTrace();
            } catch (SerializerInitializationException e) {
                e.printStackTrace();
            } catch (CasSerializationException e) {
                e.printStackTrace();
            }

            // Check if already processed with NLP, if not: NLP
            if (sJCas.length() <= MAX_DOCUMENT_SIZE) {
                System.out.println(sJCas.length());
                // write the serialized CAS into the Mongo Document
                mongoDocument.put("uima", new JSONObject(sJCas).toString());
            }
            NLPDocument nlpDoc = (NLPDocument) pSpeech;

            try {
                // add some more results of the NLP-Process
                mongoDocument.put("persons", NLPHelper.annotationsToDocumentList(nlpDoc.getNamedEntities().stream().filter(ne -> ne.getValue().equals("PER")).collect(Collectors.toList())));
                mongoDocument.put("locations", NLPHelper.annotationsToDocumentList(nlpDoc.getNamedEntities().stream().filter(ne -> ne.getValue().equals("LOC")).collect(Collectors.toList())));
                mongoDocument.put("organisations", NLPHelper.annotationsToDocumentList(nlpDoc.getNamedEntities().stream().filter(ne -> ne.getValue().equals("ORG")).collect(Collectors.toList())));
                mongoDocument.put("token", NLPHelper.annotationsToDocumentList(nlpDoc.getTokens().stream().collect(Collectors.toList())));
                mongoDocument.put("sentences", NLPHelper.annotationsToDocumentList(nlpDoc.getSentences().stream().collect(Collectors.toList())));
                if (nlpDoc.getDocumentSentiment() != null) {
                    mongoDocument.put("sentiment", nlpDoc.getDocumentSentiment().getSentiment());
                } else {
                    mongoDocument.put("sentiment", nlpDoc.getSentiments().stream().mapToDouble(sentiment -> sentiment.getSentiment()).sum());
                }
                mongoDocument.put("lemma", NLPHelper.annotationsToDocumentList(nlpDoc.getLemmas().stream().collect(Collectors.toList())));
                mongoDocument.put("ddc", NLPHelper.annotationsToDocumentList(nlpDoc.getDDC().stream().collect(Collectors.toList())));
                mongoDocument.put("pos", NLPHelper.annotationsToDocumentList(nlpDoc.getPOS().stream().collect(Collectors.toList())));

            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

        }


        return mongoDocument;

    }


    /**
     * Method to convert a Comment to an MongoDocument
     * @param pComment
     * @return
     */
    public static Document toMongoDocument(Comment pComment) throws UIMAException {

        // creating a empty MongoDocument and add attributes
        Document mongoDocument = new Document();
        mongoDocument.put("_id", pComment.getID());
        mongoDocument.put("text", pComment.getContent());
        mongoDocument.put("speaker", pComment.getSpeaker()!=null ? pComment.getSpeaker().getID() : "");
        mongoDocument.put("speech", pComment.getSpeech()!=null ? pComment.getSpeech().getID() : "");

        // if the Speech already contains a CAS-Object
        JCas pCas = pComment.toCas();

        // Check if already processed with NLP, if not: NLP
        if (JCasUtil.selectAll(pCas).size() <= 1) {
            nlpHelper.analyse(pComment);
            pCas = pComment.toCas();
        }

        if (pCas != null) {

            String sJCas = null;
            // Serialize the CAS
            try {
                sJCas = MongoSerialization.serializeJCas(pCas);
            } catch (UnknownFactoryException e) {
                e.printStackTrace();
            } catch (SerializerInitializationException e) {
                e.printStackTrace();
            } catch (CasSerializationException e) {
                e.printStackTrace();
            }

            // check if document is to large!
            if (sJCas.length() <= MAX_DOCUMENT_SIZE) {
                System.out.println(sJCas.length());
                // write the serialized CAS into the Mongo Document
                mongoDocument.put("uima", new JSONObject(sJCas).toString());
            }
            try {
                NLPDocument nlpDoc = (NLPDocument) pComment;

                // add some more results of the NLP-Process
                mongoDocument.put("persons", NLPHelper.annotationsToDocumentList(nlpDoc.getNamedEntities().stream().filter(ne -> ne.getValue().equals("PER")).collect(Collectors.toList())));
                mongoDocument.put("locations", NLPHelper.annotationsToDocumentList(nlpDoc.getNamedEntities().stream().filter(ne -> ne.getValue().equals("LOC")).collect(Collectors.toList())));
                mongoDocument.put("organisations", NLPHelper.annotationsToDocumentList(nlpDoc.getNamedEntities().stream().filter(ne -> ne.getValue().equals("ORG")).collect(Collectors.toList())));
                mongoDocument.put("token", NLPHelper.annotationsToDocumentList(nlpDoc.getTokens().stream().collect(Collectors.toList())));
                mongoDocument.put("sentences", NLPHelper.annotationsToDocumentList(nlpDoc.getSentences().stream().collect(Collectors.toList())));
                if (nlpDoc.getDocumentSentiment() != null) {
                    mongoDocument.put("sentiment", nlpDoc.getDocumentSentiment().getSentiment());
                } else {
                    mongoDocument.put("sentiment", nlpDoc.getSentiments().stream().mapToDouble(sentiment -> sentiment.getSentiment()).sum());
                }
                mongoDocument.put("lemma", NLPHelper.annotationsToDocumentList(nlpDoc.getLemmas().stream().collect(Collectors.toList())));
                mongoDocument.put("ddc", NLPHelper.annotationsToDocumentList(nlpDoc.getDDC().stream().collect(Collectors.toList())));
                mongoDocument.put("pos", NLPHelper.annotationsToDocumentList(nlpDoc.getPOS().stream().collect(Collectors.toList())));
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }


        return mongoDocument;

    }

}
