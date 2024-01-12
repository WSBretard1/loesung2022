package org.texttechnologylab.project.musterloesung.data.impl.file;

import org.texttechnologylab.project.musterloesung.data.*;
import org.texttechnologylab.project.musterloesung.exceptions.InputException;
import org.texttechnologylab.project.musterloesung.helper.StringHelper;
import org.texttechnologylab.project.musterloesung.helper.XMLHelper;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Implementation of a plenary protocol
 * @author Giuseppe Abrami
 */
public class PlenaryProtocol_File_Impl extends PlenaryObject_File_Impl implements PlenaryProtocol {

    // The DOM document is stored
    private Document pDocument = null;

    // variable declaration
    private int iIndex = -1;
    private Date pDate = null;
    private Timestamp pStartTime = null;
    private Timestamp pEndTime = null;
    private String sTitle = "";
    private String sPlace = "";

    private List<AgendaItem> pAgendaItems = new ArrayList<>(0);

    private SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.YYYY");

    private SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm");


    public PlenaryProtocol_File_Impl(ParliamentFactory pFactory){
        super(pFactory);
    }

    /**
     * Constuctor. The constructor needs a ParliamentFactory and the file of the plenary protocol
     * @param pFactory
     * @param pFile
     */
    public PlenaryProtocol_File_Impl(ParliamentFactory pFactory, File pFile){
        super(pFactory);
        try {
            init(pFile);
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    /**
     * Initialization based on a file
     * @param pFile
     */
    private void init(File pFile) throws ParserConfigurationException, IOException, SAXException, ParseException {

        // Define Date and Time Format
        SimpleDateFormat sdfDate = new SimpleDateFormat("dd.MM.yyyy");
        SimpleDateFormat sdfTime = new SimpleDateFormat("HH:mm");

        // create a document builder factory for the parsing of XML documents
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

        // parse XML file
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document pDocument = db.parse(pFile);
        this.pDocument=pDocument;

        /**
         * Extract information of the header of the file, by use of a helper method
         */
        Node nWahlperiode = getNodeFromXML(pDocument, "wahlperiode");
        this.setWahlperiode(Integer.valueOf(nWahlperiode.getTextContent()));

        Node nSitzungsnummer = getNodeFromXML(pDocument, "sitzungsnr");
        this.setIndex(Integer.valueOf(nSitzungsnummer.getTextContent()));

        Node nTitle = getNodeFromXML(pDocument, "plenarprotokoll-nummer");
        this.setTitle(nTitle.getTextContent());

        Node nOrt = getNodeFromXML(pDocument, "ort");
        this.setPlace(nOrt.getTextContent());

        Node nDatum = getNodeFromXML(pDocument, "datum");
        Date pDate = new Date(sdfDate.parse(nDatum.getAttributes().getNamedItem("date").getTextContent()).getTime());
        this.setDate(pDate);

        // extract the start of a session
        Node nStart = getNodeFromXML(pDocument, "sitzungsbeginn");
        Time pStartTime = null;
        String sStartTime = nStart.getAttributes().getNamedItem("sitzung-start-uhrzeit").getTextContent();
        sStartTime = sStartTime.replaceAll("\\.", ":");
        sStartTime = sStartTime.replace(" Uhr", "");
        try {
            pStartTime = new Time(sdfTime.parse(sStartTime).getTime());
        }
        catch (ParseException pe){
            pStartTime = new Time(sdfTime.parse(nStart.getAttributes().getNamedItem("sitzung-start-uhrzeit").getTextContent().replaceAll("\\.", ":")).getTime());
        }
        this.setStarttime(pStartTime);

        // extract the start of a session
        Node nEnde = getNodeFromXML(pDocument, "sitzungsende");
        String sEndTime = nEnde.getAttributes().getNamedItem("sitzung-ende-uhrzeit").getTextContent();
        sEndTime = sEndTime.replaceAll("\\.", ":");
        sEndTime = sEndTime.replace(" Uhr", "");
        Time pEndTime = null;
        try {
            pEndTime = new Time(sdfTime.parse(sEndTime).getTime());
        }
        catch (ParseException pe){
            try {
                pEndTime = new Time(sdfTime.parse(nEnde.getAttributes().getNamedItem("sitzung-ende-uhrzeit").getTextContent().replaceAll("\\.", ":")).getTime());
            }
            catch (ParseException peFinal){
                System.err.println(peFinal.getMessage());
            }
        }
        if(pEndTime!=null) {
            this.setEndTime(pEndTime);
        }
        else{
            this.setEndTime(pStartTime);
        }

        pStartTime = new Time(this.getDate().getTime()+this.getStartTime().getTime());

        if(this.getEndTime().before(this.getStartTime())){
            Calendar pCalender = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));
            pCalender.setTime(this.getDate());
            pCalender.add(Calendar.DAY_OF_YEAR, 1);
            pEndTime = new Time(pCalender.getTime().getTime()+this.getEndTime().getTime());
        }
        else{
            pEndTime = new Time(this.getDate().getTime()+this.getEndTime().getTime());
        }

        this.setStarttime(pStartTime);
        this.setEndTime(pEndTime);

        // Processing AgendaIgems
        NodeList pBlocks = pDocument.getElementsByTagName("ivz-block");

        for(int b=0; b<pBlocks.getLength(); b++){
            Node n = pBlocks.item(b);

            AgendaItem pItem = new AgendaItem_File_Impl(this, n);
            if(pItem.getSpeeches().size()>0){
                this.addAgendaItem(pItem);
            }

        }


    }

    private void addAbsences(NodeList pAnlagen){

        for(int a=0; a<pAnlagen.getLength(); a++){
            Node pNode = XMLHelper.getSingleNodesFromXML(pAnlagen.item(0), "table");

            if(pNode!=null) {

                List<Node> trNodes = XMLHelper.getNodesFromXML(pNode, "tr");

                for (Node trNode : trNodes) {

                    List<Node> tNodes = XMLHelper.getNodesFromXML(trNode, "td");
                    for (Node tNode : tNodes) {

                        String sContent = tNode.getTextContent();
                        sContent = sContent.replaceAll("\\*", "");
                        if (sContent.contains(", ")) {
                            String[] sNames = sContent.split(", ");
                            String lastName = sNames[0];
                            String firstName = sNames[1];

                            firstName = StringHelper.replaceList(firstName, StringHelper.FIRSTNAME);

                            lastName = StringHelper.replaceList(lastName, StringHelper.REGEXCLEAN);

                            String finalFirstName = firstName;
                            String finalLastName = lastName;
                            Set<Speaker> speakerSet = this.getFactory().getSpeakers().stream().filter(s -> {
                                return s.getName().equalsIgnoreCase(finalLastName) && s.getFirstName().equalsIgnoreCase(finalFirstName);
                            }).collect(Collectors.toSet());

                                speakerSet.forEach(pSpeaker -> {
                                    pSpeaker.addAbsense(this);
                                });


                        }

                    }

                }
            }

        }


    }

    /**
     *
     */
    public void setupAbsense(){
        // Processing anlagen
        NodeList pAnlagen = pDocument.getElementsByTagName("anlagen");

        addAbsences(pAnlagen);

    }

    /**
     * Return the XML-document
     * @return
     */
    protected Document getFile(){
        return this.pDocument;
    }

    /**
     * Extract a Node by a tag-name
     * @param pDocument
     * @param sTag
     * @return
     */
    private Node getNodeFromXML(Document pDocument, String sTag){
        return pDocument.getElementsByTagName(sTag).item(0);
    }

    @Override
    public int getIndex() {
        return this.iIndex;
    }

    @Override
    public void setIndex(int iValue) {
        this.iIndex = iValue;
    }

    @Override
    public Date getDate() {
        return this.pDate;
    }

    @Override
    public String getDateFormated() {
        return dateFormat.format(getDate());
    }

    @Override
    public void setDate(Date pDate) {
        this.pDate = pDate;
    }

    @Override
    public Timestamp getStartTime() {
        return this.pStartTime;
    }

    @Override
    public String getStartTimeFormated() throws InputException {
        if(getStartTime()!=null) {
            return timeFormat.format(getStartTime())+" Uhr";
        }
        throw new InputException("not valid time");
    }

    @Override
    public void setStarttime(Time pTime) {
        this.pStartTime = new Timestamp(pTime.getTime());
    }

    @Override
    public Timestamp getEndTime() {
        return this.pEndTime;
    }

    @Override
    public String getEndTimeFormated() throws InputException {
        if(getEndTime()!=null) {
            return timeFormat.format(getEndTime())+" Uhr";
        }
        throw new InputException("not valid time");
    }

    @Override
    public void setEndTime(Time pTime) {
        if(pTime!=null) {
            this.pEndTime = new Timestamp(pTime.getTime());
        }
    }

    @Override
    public String getTitle() {
        return this.sTitle;
    }

    @Override
    public void setTitle(String sValue) {
        this.sTitle = sValue;
    }

    @Override
    public List<AgendaItem> getAgendaItems() {
        return this.pAgendaItems;
    }

    @Override
    public void addAgendaItem(AgendaItem pItem) {
        this.pAgendaItems.add(pItem);
    }

    @Override
    public void addAgendaItems(Set<AgendaItem> pSet) {
        pSet.forEach(i->{
            this.addAgendaItem(i);
        });
    }

    @Override
    public String getPlace() {
        return this.sPlace;
    }

    @Override
    public void setPlace(String sValue) {
        this.sPlace = sValue;
    }

    @Override
    public Set<Speaker> getSpeakers() {
        Set<Speaker> rSet = new HashSet<>(0);

            getAgendaItems().forEach(ai->{
                ai.getSpeeches().forEach(speech -> {
                    rSet.add(speech.getSpeaker());
                });
            });

        return rSet;
    }

    @Override
    public Set<Speaker> getSpeakers(Party pParty) {
        return getSpeakers().stream().filter(s->s.getParty().equals(pParty)).collect(Collectors.toSet());
    }

    @Override
    public Set<Speaker> getSpeakers(Fraction pFraction) {
        return getSpeakers().stream().filter(s->{
            try {
                return s.getFraction().equals(pFraction);
            }
            catch (NullPointerException npe){

            }
            return false;
        }).collect(Collectors.toSet());
    }

    @Override
    public Set<Speaker> getLeaders() {

        /*
        * Search all agenda items, in their speech, who interrupted a speech and if they are a president.
        */
        Set<Speaker> rSet = new HashSet<>(0);
        this.getAgendaItems().forEach(ai->{
            ai.getSpeeches().forEach(s->{
                s.getInsertions().forEach(i->{
                    if(i.getSpeaker().isLeader()){
                        rSet.add(i.getSpeaker());
                    }
                });
            });
        });

        return rSet;
    }

    @Override
    public long getDuration() {

        long pTime = -1l;

        if(this.getEndTime()==null || this.getStartTime()==null){
            return pTime;
        }

        if(this.getEndTime().before(this.getStartTime())){
            Calendar pCalender = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));
            pCalender.setTime(this.getEndTime());
            pCalender.add(Calendar.DAY_OF_YEAR, 1);
            pTime = Math.abs(pCalender.getTime().getTime() - getStartTime().getTime());
        }
        else{
            pTime = Math.abs(getEndTime().getTime() - getStartTime().getTime());
        }


        return pTime;
    }

    @Override
    public String getDurationFormated(){
        SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
        timeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date pDate = new Date(getDuration());
        return timeFormat.format(pDate);
    }

    @Override
    public String toString() {
        return this.getIndex()+"\t"+this.getDateFormated()+"\t"+this.getPlace();
    }

    @Override
    public boolean equals(Object o) {
        return this.hashCode()==o.hashCode();
    }

    /**
     * Special hashCode method
     * @return
     */
    @Override
    public int hashCode() {
        return this.getIndex();
    }

    /**
     * Special compareTo method
     * @param plenaryObject
     * @return
     */
    @Override
    public int compareTo(PlenaryObject plenaryObject) {
        if(plenaryObject instanceof PlenaryProtocol){
            return ((PlenaryProtocol)plenaryObject).getDate().compareTo(this.getDate());
        }
        return super.compareTo(plenaryObject);
    }
}
