package org.texttechnologylab.project.musterloesung.data.impl.file;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BsonField;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.uima.UIMAException;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.texttechnologylab.project.musterloesung.data.*;
import org.texttechnologylab.project.musterloesung.data.impl.mongodb.*;
import org.texttechnologylab.project.musterloesung.database.MongoDBConfig;
import org.texttechnologylab.project.musterloesung.database.MongoDBConnectionHandler;
import org.texttechnologylab.project.musterloesung.helper.NLPHelper;
import org.w3c.dom.Node;

import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Accumulators.push;
import static com.mongodb.client.model.Accumulators.sum;

/**
 * Implementation of the ParliamantFactory for access to the individual components
 * @author Giuseppe Abrami
 */
public class ParliamentFactory_Impl implements ParliamentFactory {

    private MongoDBConnectionHandler dbConnectionHandler = null;

    private NLPHelper nlpHelper = null;

    @Override
    public NLPHelper getNLPHelper() {
        return this.nlpHelper;
    }

    @Override
    public MongoDBConnectionHandler getMongoConnection() {
        return this.dbConnectionHandler;
    }

    public ParliamentFactory_Impl(MongoDBConnectionHandler dbConnectionHandler){
        this();
        this.dbConnectionHandler = dbConnectionHandler;
    }

    public ParliamentFactory_Impl(){
        this.nlpHelper = new NLPHelper();
    }

    public Set<Speaker> getSpeakers(){
        Set<Speaker> rSet = new HashSet<>(0);

        MongoCursor<Document> rCursor = this.dbConnectionHandler.doQueryIterator("{}", "speaker");

        rCursor.forEachRemaining(d->{
            rSet.add(new Speaker_MongoDB_Impl(this, d));
        });


        return rSet;
    }

    @Override
    public Set<Speaker> getSpeakers(Fraction pFraction) {

        Set<Speaker> rSet = new HashSet<>(0);

            BasicDBObject query = new BasicDBObject();
            query.put("fraction", pFraction.getName());

            MongoCursor<Document> pDocument = dbConnectionHandler.doQueryIterator(query, "speaker");

            pDocument.forEachRemaining(d->{
                rSet.add(new Speaker_MongoDB_Impl(this, d));
            });

        return rSet;

    }

    @Override
    public Set<Speaker> getSpeakers(PlenaryProtocol pProtocol) {

        Set<Speaker> rSet = new HashSet<>(0);

            BasicDBObject query = new BasicDBObject();
            query.put("protocol.index", pProtocol.getIndex());

            MongoCursor<Document> rDocuments = dbConnectionHandler.doQueryIterator(query, "speech");

            rDocuments.forEachRemaining(d->{
                rSet.add(getSpeaker(d.getString("speaker")));
            });

        return rSet;

    }

    @Override
    public Set<PlenaryProtocol> getProtocols(){
        Set<PlenaryProtocol> rSet = new HashSet<>(0);

        MongoCursor<Document> rCursor = this.dbConnectionHandler.doQueryIteratorDistinct("protocol", Document.class, "speech");

        rCursor.forEachRemaining(d->{
            rSet.add(new PlenaryProtocol_MongoDB_Impl(this, d));
        });


        return rSet;
    }

    @Override
    public Set<PlenaryProtocol> getProtocols(int iWP) {
        return null;
    }

    @Override
    public PlenaryProtocol getProtocol(int iIndex) {

        PlenaryProtocol rProtocol = null;

        MongoCursor<Document> documents = this.dbConnectionHandler.doQueryIterator(BasicDBObject.parse("{ \"protocol.index\": "+iIndex+"}"), "speech");


        while(documents.hasNext()){
            rProtocol = new PlenaryProtocol_MongoDB_Impl(this, documents.next().get("protocol", Document.class));
            break;
        }

        return rProtocol;
    }

    @Override
    public void addProtocol(PlenaryProtocol pProtocol) {

    }

    @Override
    public Set<Fraction> getFractions(){
        Set<Fraction> rSet = new HashSet<>(0);

        MongoCursor<String> rCursor = this.dbConnectionHandler.doQueryIteratorDistinct("fraction", String.class, "speaker");

        rCursor.forEachRemaining(s->{
            rSet.add(new Fraction_MongoDB_Impl(this, s));
        });

        return rSet;
    }

    @Override
    public Set<Party> getParties(){
        Set<Party> rSet = new HashSet<>(0);

        MongoCursor<String> rCursor = this.dbConnectionHandler.doQueryIteratorDistinct("party", String.class, "speaker");

        rCursor.forEachRemaining(s->{
            rSet.add(new Party_MongoDB_Impl(this, s));
        });

        return rSet;
    }

    @Override
    public Party getParty(String sName) {
        Optional<Party> pReturn = getParties().stream().filter(p->p.getName().equalsIgnoreCase(sName)).findFirst();
        if(pReturn.isPresent()){
            return pReturn.get();
        }


        Party pParty = new Party_MongoDB_Impl(this, sName);
        return pParty;
    }

    @Override
    public Speaker getSpeaker(String sId) {

        Speaker pSpeaker = null;

        Document pDocument = this.dbConnectionHandler.getObject(sId, "speaker");

        if(pDocument!=null){
            pSpeaker = new Speaker_MongoDB_Impl(this, pDocument);
        }

        return pSpeaker;

    }


    public Speaker getSpeakerByName(String sValue){

        List<Speaker> sList = this.getSpeakers().stream().filter(s->{
            return s.getName().equalsIgnoreCase(Speaker_Plain_File_Impl.transform(sValue));
        }).collect(Collectors.toList());

        if(sList.size()==1){
            return sList.get(0);
        }
        return null;

    }

    @Override
    public Speaker getSpeaker(Node pNode) {

        Speaker pSpeaker = null;

        // if speaker is a complex node
        if(!pNode.getNodeName().equalsIgnoreCase("name")){
            String sID = pNode.getAttributes().getNamedItem("id").getTextContent();

            pSpeaker= getSpeaker(sID);

            if(pSpeaker==null){
                Speaker_File_Impl nSpeaker = new Speaker_File_Impl(this, pNode);
                pSpeaker = nSpeaker;
            }
        }
        // if not...
        else{
            pSpeaker = getSpeakerByName(pNode.getTextContent());

            if(pSpeaker==null){
                Speaker_Plain_File_Impl plainSpeaker = new Speaker_Plain_File_Impl(this);
                plainSpeaker.setName(pNode.getTextContent());
                pSpeaker = plainSpeaker;
            }

        }

        return pSpeaker;
    }

    @Override
    public Fraction getFraction(String sName) {
        /*
         * search in fractions if there is a fraction with this name?
         * Attention: Since in Bündnis 90/Die Grünen partly other characters are used, here a small trick is used and
         * not checked for the simultaneity of the name of the faction but only for their same beginning.
         */
        List<Fraction> sList = this.getFractions().stream().filter(s->{
            if(s.getName().startsWith(sName.substring(0, 3))){
                return true;
            }
            return s.getName().equalsIgnoreCase(sName.trim());
        }).collect(Collectors.toList());

        if(sList.size()==1){
            return sList.get(0);
        }

        return null;
    }

    @Override
    public Fraction getFraction(Node pNode) {
        String sName = pNode.getTextContent();

        Fraction pFraction = getFraction(sName);

        if(pFraction==null){
            // if fraction not exist, create
            pFraction = new Fraction_File_Impl(pNode);
        }

        return pFraction;
    }

    @Override
    public void createDatabaseConnection(MongoDBConfig pConfig) {
        this.dbConnectionHandler = new MongoDBConnectionHandler(pConfig);
    }

    @Override
    public void addSpeech(Speech pSpeech) throws UIMAException {
        this.dbConnectionHandler.insertSpeech(pSpeech);
        addSpeaker(pSpeech.getSpeaker());
    }

    @Override
    public void insertSpeech(Speech pSpeech) throws UIMAException {
        this.dbConnectionHandler.insertSpeech(pSpeech);
    }

    @Override
    public void addComment(Comment pComment) throws UIMAException {
        this.dbConnectionHandler.insertComment(pComment);
    }

    @Override
    public void addSpeaker(Speaker pSpeaker)  {
        this.dbConnectionHandler.insertSpeaker(pSpeaker);
    }

    @Override
    public Set<Speaker> getMembers(Party pParty){
        Set<Speaker> rSet = new HashSet<>(0);

        Iterator<Document> dIterator = this.dbConnectionHandler.doQueryIterator(BasicDBObject.parse("{ \"party\": \""+pParty.getName()+"\""), "speaker");
        dIterator.forEachRemaining(d->{
            rSet.add(new Speaker_MongoDB_Impl(this, d));
        });

        return rSet;
    }

    @Override
    public Set<Speaker> getMembers(Fraction pFraction){
        Set<Speaker> rSet = new HashSet<>(0);

        Iterator<Document> dIterator = this.dbConnectionHandler.doQueryIterator(BasicDBObject.parse("{ \"fraction\": \""+pFraction.getName()+"\""), "speaker");
        dIterator.forEachRemaining(d->{
            rSet.add(new Speaker_MongoDB_Impl(this, d));
        });

        return rSet;
    }

    @Override
    public List<AgendaItem> getAgendaItem(PlenaryProtocol pProtocol) {
        List<AgendaItem> rList = new ArrayList<>(0);

        Iterator<Document> dIterator = this.dbConnectionHandler.doQueryIterator(BasicDBObject.parse("{ \"protocol.index\": "+pProtocol.getIndex()+"}"), "speech");
        dIterator.forEachRemaining(d->{
            rList.add(new AgendaItem_MongoDB_Impl(pProtocol, d.get("agenda", Document.class)));
        });

        return rList;
    }

    @Override
    public Speech getSpeech(String sID){

        Speech rSpeech = null;

        Document pDocument = this.dbConnectionHandler.getObject(sID, "speech");

        if(pDocument!=null){
            rSpeech = new Speech_MongoDB_Impl(this, pDocument);
        }

        return rSpeech;

    }

    @Override
    public List<Speech> getSpeeches(PlenaryProtocol pProtocol, AgendaItem pItem){

        List<Speech> rSet = new ArrayList<>(0);

        Iterator<Document> dIterator = this.dbConnectionHandler.doQueryIterator(BasicDBObject.parse("{ \"protocol.index\": "+pProtocol.getIndex()+", \"agenda.index\": \""+pItem.getIndex()+"\"}"), "speech");
        dIterator.forEachRemaining(d->{
            rSet.add(new Speech_MongoDB_Impl(this, d));
        });

        return rSet;

    }

    @Override
    public MongoCursor<Document> getSpeeches() {
        return getSpeeches("{}");
    }

    @Override
    public MongoCursor<Document> getSpeeches(String sQuery) {
        return this.dbConnectionHandler.doQueryIterator(BasicDBObject.parse(sQuery), "speech");
    }

    @Override
    public List<Speech> getSpeeches(Speaker pSpeaker) {

        List<Speech> rList = new ArrayList<>(0);

            BasicDBObject query = new BasicDBObject();
            query.put("speaker", pSpeaker.getID());

            MongoCursor<Document> rResult = this.dbConnectionHandler.doQueryIterator(query, "speaker");

            rResult.forEachRemaining(d->{
                rList.add(new Speech_MongoDB_Impl(this, d));
            });

        return rList;

    }


    @Override
    public List<Comment> getComments() {
        List<Comment> rList = new ArrayList<>(0);
        MongoCursor<Document> dCursor = this.dbConnectionHandler.doQueryIterator(BasicDBObject.parse("{}"), "comment");
        dCursor.forEachRemaining(document->{
            rList.add(new Comment_MongoDB_Impl(this, document));
        });
        return rList;
    }

    @Override
    public List<Comment> getComments(Speaker pSpeaker){

        List<Comment> rList = new ArrayList<>(0);

        BasicDBObject query = new BasicDBObject();
        query.put("speaker", pSpeaker.getID());

        MongoCursor<Document> dCursor = this.dbConnectionHandler.doQueryIterator(query, "comment");
        dCursor.forEachRemaining(document->{
            rList.add(new Comment_MongoDB_Impl(this, document));
        });
        return rList;

    }

    @Override
    public void updateSpeech(Speech pSpeech)  {
        this.dbConnectionHandler.update(pSpeech);
    }

    @Override
    public void updateComment(Comment pComment) throws UIMAException {
        this.dbConnectionHandler.update(pComment);
    }

    @Override
    public void updateSpeaker(Speaker pSpeaker)  {
        this.dbConnectionHandler.update(pSpeaker);
    }

    @Override
    public List<PlenaryProtocol> listByDate(boolean bDescending) {
        ArrayList<PlenaryProtocol> rList = new ArrayList<>();

        List<Bson> query = Arrays.asList(
                Aggregates.project(BsonDocument.parse("{ _id: \"$protocol.index\", \"protocol\": 1, result: { $subtract: [ { $convert: { input: \"$protocol.endtime\", to: \"date\"} }, { $convert: { input: \"$protocol.starttime\", to: \"date\"} } ] } }")),
                Aggregates.group("$_id", new BsonField("result", BsonDocument.parse("{$first: \"$result\"}")), new BsonField("protocol", BsonDocument.parse("{ $first: \"$protocol\" }"))),
                Aggregates.sort(bDescending ? Sorts.descending("result") : Sorts.ascending("result")));

        AggregateIterable it = dbConnectionHandler.getCollection("speech").aggregate(query).allowDiskUse(true).maxTime(3, TimeUnit.MINUTES);

        MongoCursor<Document> cursor = it.iterator();

        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

        cursor.forEachRemaining(d->{
            rList.add(new PlenaryProtocol_MongoDB_Impl(this, d.get("protocol", Document.class)));

            // Direkte Ausgabe!
            System.out.println(d.get("protocol", Document.class).getString("title").replaceAll("\n", "")+"\t"+sdf.format(new Time(d.getLong("result"))));
        });

        return rList;
    }

    @Override
    public List<Speech> fullTextSearch(String sValue) {

        List<Speech> rList = new ArrayList<>();

        MongoCursor<Document> result = this.dbConnectionHandler.getCollection("speech").find(BsonDocument.parse("{$text: { $search: \""+sValue+"\" }}")).sort(BsonDocument.parse("{ score: { $meta: \"textScore\"}}")).cursor();

        result.forEachRemaining(d->{
            rList.add(new Speech_MongoDB_Impl(this, d));
        });

        return rList;

    }

    @Override
    public List<Document> getSpeakerBySpeeches(Party pParty, Fraction pFraction) {
        return getSpeakerBySpeeches(null, null, pParty, pFraction);
    }

    @Override
    public List<Document> getSpeakerBySpeeches(Date pDateFrom, Date pDateTo, Party pParty, Fraction pFraction){

        List<Document> rSet = new ArrayList<>(0);

        List<Bson> query = new ArrayList<>();
        if(pDateFrom!=null && pDateTo!=null){
            query.add(Aggregates.match(BsonDocument.parse("{\"protocol.date\": { $gt: NumberLong("+pDateFrom.getTime()+"), $lt: NumberLong("+pDateTo.getTime()+") } } ")));
        }
        else if(pDateFrom!=null){
            query.add(Aggregates.match(BsonDocument.parse("{\"protocol.date\": NumberLong("+pDateFrom.getTime()+")}  ")));
        }
        query.add(Aggregates.group("$speaker", new BsonField("count", BsonDocument.parse("{ $sum: 1 }"))));
        query.add(Aggregates.lookup("speaker", "_id", "_id", "speaker"));
        query.add(Aggregates.sort(Sorts.descending("count")));

        if(pParty!=null){
            query.add(Aggregates.match(BsonDocument.parse("{\"speaker.party\": \""+pParty.getName()+"\"}")));
        }
        if(pFraction!=null){
            query.add(Aggregates.match(BsonDocument.parse("{\"speaker.fraction\": \""+pFraction.getName()+"\"}")));
        }

        AggregateIterable it = dbConnectionHandler.getCollection("speech").aggregate(query).allowDiskUse(true).maxTime(3, TimeUnit.MINUTES);

        MongoCursor<Document> cursor = it.iterator();

        cursor.forEachRemaining(d->{
            d.put("speaker", d.getList("speaker", Document.class).get(0));
            rSet.add(d);

        });

        return rSet;

    }

    @Override
    public List<Document> getAvgSpeeches(Party pParty, Fraction pFraction) {
        return getAvgSpeeches(null, null, pParty, pFraction);
    }

    @Override
    public List<Document> getAvgSpeeches(Date pDateFrom, Date pDateTo, Party pParty, Fraction pFraction) {
        List<Document> rSet = new ArrayList<>();

        List<Bson> query = new ArrayList<>();

        if(pDateFrom!=null && pDateTo!=null){
            query.add(Aggregates.match(BsonDocument.parse("{\"protocol.date\": { $gt: NumberLong("+pDateFrom.getTime()+"), $lt: NumberLong("+pDateTo.getTime()+") } } ")));
        }
        else if(pDateFrom!=null){
            query.add(Aggregates.match(BsonDocument.parse("{\"protocol.date\": NumberLong("+pDateFrom.getTime()+")}  ")));
        }

        query.add(Aggregates.project(BsonDocument.parse("{ \"length\": { $strLenCP: \"$text\" }, \"speaker\": 1 }")));
        query.add(Aggregates.group("$speaker", new BsonField("avg", BsonDocument.parse("{ $avg: \"$length\" }"))));

        query.add(Aggregates.lookup("speaker", "_id", "_id", "name"));

        if(pParty!=null){
            query.add(Aggregates.group("$name.party", new BsonField("avg", BsonDocument.parse("{ $avg: \"$avg\" }"))));
        }
        if(pFraction!=null){
            query.add(Aggregates.group("$name.fraction", new BsonField("avg", BsonDocument.parse("{ $avg: \"$avg\" }"))));
        }
        query.add(Aggregates.sort(Sorts.descending("avg")));


        AggregateIterable it = dbConnectionHandler.getCollection("speech").aggregate(query);

        MongoCursor<Document> cursor = it.iterator();

        cursor.forEachRemaining(d->{
            rSet.add(d);
        });

        return rSet;
    }

    @Override
    public List<Document> getAvgSpeeches(Date pDate, Party pParty, Fraction pFraction) {
        return getAvgSpeeches(pDate, null, pParty, pFraction);
    }

    /**
     * In this method, the query should run similarly to the variant with the comments, but here the runtime is too long. The reason for this is that "unwind" increases the number of entries per document, depending on how many lemmas are present. Therefore another way was chosen here.
     * @param sPos
     * @return
     */
    @Override
    public List<Document> getSpeechLemmaByPos(String sPos) {

        List<Document> rDocuments = new ArrayList<>(0);

        List<Bson> query = new ArrayList<>();

        query.add(Aggregates.unwind("$lemma"));
        query.add(Aggregates.match(BsonDocument.parse("{\"lemma.pos\" : \""+sPos+"\"}")));
        query.add(Aggregates.group("$lemma.value", new BsonField("count", BsonDocument.parse("{ $sum: 1 }"))));
        query.add(Aggregates.sort(Sorts.descending("count")));
        query.add(Aggregates.limit(50));

        AggregateIterable it = dbConnectionHandler.getCollection("speech").aggregate(query).bypassDocumentValidation(false).allowDiskUse(true).maxAwaitTime(5l, TimeUnit.MINUTES).maxTime(5l, TimeUnit.MINUTES);

        MongoCursor<Document> cursor = it.iterator();

        cursor.forEachRemaining(d->{
            rDocuments.add(d);
        });


        return rDocuments;

    }

    /**
     * In this method, the matches between lemma and pos, in terms of their begin and end values, are matched by performing an "unwind" beforehand. This is a good solution and does not require any special adjustment. The runtime is acceptable, but only because the number of lemmas is very short.
     * @see getSpeechLemmaByPos
     * @param sPos
     * @return
     */
    @Override
    public List<Document> getCommentLemmaByPos(String sPos) {
        List<Document> rDocuments = new ArrayList<>(0);

        List<Bson> query = new ArrayList<>();

        query.add(Aggregates.unwind("$pos"));
        query.add(Aggregates.unwind("$lemma"));

        query.add(Aggregates.match(Filters.expr(Document.parse("{ $eq: [ \"$lemma.begin\", \"$pos.begin\" ] }"))));
        query.add(Aggregates.match(Filters.expr(Document.parse("{ $eq: [ \"$lemma.end\", \"$pos.end\" ] }"))));
        query.add(Aggregates.match(Filters.eq("pos.type", sPos)));

        query.add(Aggregates.group("$lemma.value", new BsonField("count", BsonDocument.parse("{ $sum: 1 }"))));

        query.add(Aggregates.sort(Sorts.descending("count")));
        query.add(Aggregates.limit(50));

        AggregateIterable it = dbConnectionHandler.getCollection("comment").aggregate(query).bypassDocumentValidation(false).allowDiskUse(true).maxAwaitTime(5l, TimeUnit.MINUTES).maxTime(5l, TimeUnit.MINUTES);

        MongoCursor<Document> cursor = it.iterator();

        cursor.forEachRemaining(d->{
            rDocuments.add(d);
        });


        return rDocuments;
    }

    @Override
    public Map<String, Map<String, Integer>> getNamedEntities() {

        Map<String, Map<String, Integer>> rMap = new HashMap<>(0);

        rMap.put("persons", sumMap("persons", "speech"));
        rMap.put("locations", sumMap("locations", "speech"));
        rMap.put("organisations", sumMap("organisations", "speech"));

        return rMap;
    }

    private Map<String, Integer> sumMap(String sElement, String sCollection) {

        Map<String, Integer> rMap = new LinkedMap<>(0);

        List<Bson> query = Arrays.asList(
                Aggregates.unwind("$" + sElement),
                Aggregates.group("$" + sElement+".value", sum("count", 1)),
                Aggregates.sort(Sorts.descending("count"))
        );

        AggregateIterable it = dbConnectionHandler.getCollection(sCollection).aggregate(query).maxTime(5, TimeUnit.MINUTES);


        MongoCursor<Document> dList = it.iterator();
        dList.forEachRemaining(d -> {
            rMap.put(d.getString("_id"), d.getInteger("count"));
        });

        return rMap;

    }

    @Override
    public List<Document> getSentimentCorpus(String sCollection){

        List<Document> rList = new ArrayList<>();

        List<Bson> query = Arrays.asList(
                Aggregates.group("$" + "sentiment", sum("count", 1)),
                Aggregates.sort(Sorts.descending("_id"))
        );

        MongoCursor<Document> pResult = dbConnectionHandler.getCollection(sCollection).aggregate(query).cursor();

        pResult.forEachRemaining(d->{
            rList.add(d);
        });

        return rList;

    }
    @Override
    public List<Document> getDDC(int iTop){

        List<Document> rList = new ArrayList<>();

        List<Bson> query = Arrays.asList(
                Aggregates.unwind("$ddc"),
                Aggregates.group("$" + "_id", push("results", "$ddc")),
                Aggregates.project(Document.parse("{\n" +
                        "      \"_id\": 1,\n" +
                        "      \"result\": { $slice: [ \"$results\",  3 ]\n" +
                        "      }\n" +
                        "    }")),
                Aggregates.unwind("$result"),
                Aggregates.group("$result.value", sum("count", 1)),
                Aggregates.sort(Sorts.descending("count")),
                Aggregates.limit(30)
        );

        MongoCursor<Document> pResult = dbConnectionHandler.getCollection("speech").aggregate(query).allowDiskUse(true).maxTime(10, TimeUnit.HOURS).maxAwaitTime(10, TimeUnit.MINUTES).cursor();

        pResult.forEachRemaining(d->{
            rList.add(d);
        });

        return rList;

    }

    @Override
    public List<Document> getSentimentComment(Bson pFilter, int iLimit){

        List<Document> rList = new ArrayList<>();


        List<Bson> query = Arrays.asList(
                Aggregates.match(pFilter),
                Aggregates.sort(Sorts.descending("sentiment")),
                Aggregates.group("$speech", sum("count", 1)),
                Aggregates.sort(Sorts.descending("count")),
                Aggregates.limit(iLimit)
        );

        MongoCursor<Document> pResult = dbConnectionHandler.getCollection("comment").aggregate(query).allowDiskUse(true).maxTime(5, TimeUnit.HOURS).cursor();

        pResult.forEachRemaining(d->{
            rList.add(d);
        });



        return rList;

    }



}
