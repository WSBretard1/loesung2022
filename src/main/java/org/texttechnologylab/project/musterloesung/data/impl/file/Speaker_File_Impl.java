package org.texttechnologylab.project.musterloesung.data.impl.file;

import org.texttechnologylab.project.musterloesung.data.*;
import org.texttechnologylab.project.musterloesung.helper.XMLHelper;
import org.w3c.dom.Node;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Giuseppe Abrami
 */
public class Speaker_File_Impl extends PlenaryObject_File_Impl implements Speaker {

    // Speaker variables
    protected String sName = "";
    protected String sFirstName = "";
    protected String sTitle = "";
    protected String sRole = "";

    protected Set<Speech> pSpeeches = new HashSet<>();

    protected Set<PlenaryProtocol> pAbsences = new HashSet<>(0);

    protected Fraction pFraction = null;
    protected Party pParty = null;

    protected String sBeruf = "";
    protected String sReligion = "";
    protected String sFamilienstand = "";
    protected String sGeburtsort = "";

    protected Date pGeburtsdatum = null;
    protected String sAkademischerTitel = "";
    protected String sGeschlecht = "";
    protected Date pSterbedatum = null;

    /**
     * Constructor
     * @param pFactory
     */
    public Speaker_File_Impl(ParliamentFactory pFactory){
        super(pFactory);
    }

    /**
     * Constructor
     * @param pFactory
     * @param pNode
     */
    public Speaker_File_Impl(ParliamentFactory pFactory, Node pNode){
        super(pFactory);
        this.setID(pNode.getAttributes().getNamedItem("id").getTextContent());
        init(pNode);
    }

    /**
     * Constructor
     * @param pSpeech
     * @param pNode
     */
    public Speaker_File_Impl(Speech pSpeech, Node pNode){
        super(pSpeech.getFactory());
        this.setID(pNode.getAttributes().getNamedItem("id").getTextContent());

        this.addSpeech(pSpeech);

        init(pNode);

    }

    /**
     * Internal init method based on XML-node
     * @param pNode
     */
    private void init(Node pNode){

        Node nSurname = XMLHelper.getSingleNodesFromXML(pNode, "nachname");
        Node nFirstName = XMLHelper.getSingleNodesFromXML(pNode, "vorname");
        Node nNamensZusatz = XMLHelper.getSingleNodesFromXML(pNode, "namenszusatz");
        Node nTitle = XMLHelper.getSingleNodesFromXML(pNode, "titel");
        Node nRole = XMLHelper.getSingleNodesFromXML(pNode, "rolle_lang");
        Node nFraction = XMLHelper.getSingleNodesFromXML(pNode, "fraktion");

        if(nSurname!=null){
            this.setName(nSurname.getTextContent());
        }
        if(nNamensZusatz!=null){
            this.setName(nNamensZusatz.getTextContent()+" "+this.getName());
        }
        if(nFirstName!=null){
            this.setFirstName(nFirstName.getTextContent());
        }
        if(nTitle!=null){
            this.setTitle(nTitle.getTextContent());
        }
        if(nRole!=null){
            this.setRole(nRole.getTextContent());
        }
        if(nFraction!=null){
            this.setFraction(this.getFactory().getFraction(nFraction));
            this.getFraction().addMember(this);
        }

        // clean up
        this.setFirstName(this.getFirstName().replaceAll(this.getRole(), ""));

    }

    @Override
    public Party getParty() {
        return this.pParty;
    }

    @Override
    public void setParty(Party pParty) {
        this.pParty = pParty;
        pParty.addMember(this);
    }

    @Override
    public Fraction getFraction() {
        return this.pFraction;
    }

    @Override
    public void setFraction(Fraction pFraction) {
        this.pFraction = pFraction;
    }

    @Override
    public String getRole() {
        return this.sRole;
    }

    @Override
    public void setRole(String sValue) {
        this.sRole = sValue;
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
    public String getName() {
        return this.sName;
    }

    @Override
    public void setName(String sValue) {
        this.sName = sValue;
    }

    @Override
    public String getFirstName() {
        return this.sFirstName;
    }

    @Override
    public void setFirstName(String sValue) {
        this.sFirstName = sValue;
    }

    @Override
    public Set<Speech> getSpeeches() {
        return this.pSpeeches;
    }

    @Override
    public void addSpeech(Speech pSpeech) {
        this.pSpeeches.add(pSpeech);
    }

    @Override
    public void addSpeeches(Set<Speech> pSet) {
        pSet.forEach(s->{
            addSpeech(s);
        });
    }

    @Override
    public Set<Comment> getComments() {
        Set<Comment> rSet = new HashSet<>(0);
        this.getSpeeches().stream().forEach(speech -> {
            rSet.addAll(speech.getComments());
        });
        return rSet;
    }


    @Override
    public float getAvgLength() {
        float rFloat = 0.f;

            int iSum = this.getSpeeches().stream().mapToInt(s->s.getLength()).sum();

            if(this.getSpeeches().size()>0){
                rFloat = iSum / this.getSpeeches().size();
            }

        return rFloat;
    }

    @Override
    public String toString() {
        return this.getTitle()+"\t"+this.getFirstName()+"\t"+this.getName()+"\t"+(this.getRole().length()>0 ? ", "+this.getRole() : ""+"\t"+(this.getFraction()!=null ? "Fraction: ("+this.getFraction()+")" : "")+"\t"+this.getParty()!=null ? "Party: "+this.getParty() : "");
    }

    @Override
    public boolean isLeader(){
        boolean rBool = false;

//            if(this instanceof Speaker_Plain_File_Impl){
//                System.out.println("Stop");
//            };

            rBool = this.getRole().startsWith("Präsident") || this.getRole().startsWith("Vizepräsident") || this.getRole().toLowerCase().startsWith("Alters");

            if (!rBool) {
                rBool = this.getName().startsWith("Präsident") || this.getName().startsWith("Vizepräsident");
            }

        return rBool;
    }

    @Override
    public boolean isGovernment(){

        boolean rBool = false;

            if(this.getRole().contains("minister")){
                rBool = true;
            }
            if(this.getRole().contains("kanzler")){
                rBool = true;
            }

            if(this.getRole().contains("Staat")){
                rBool = false;
            }

        return rBool;

    }

    @Override
    public Set<PlenaryProtocol> getAbsences() {
        return this.pAbsences;
    }

    @Override
    public float getAbsences(int iWP) {

        long lAmount = getAbsences().stream().filter(p->p.getWahlperiode()==iWP).count();
        long lMax = this.getFactory().getProtocols(iWP).stream().count();

        return (float) (lAmount*(float)100/lMax)/100;

    }

    @Override
    public void addAbsense(PlenaryProtocol pProtocol) {
        this.pAbsences.add(pProtocol);
    }

    @Override
    public String getAkademischerTitel() {
        return this.sAkademischerTitel;
    }

    @Override
    public void setAkademischerTitel(String sValue) {
        this.sAkademischerTitel=sValue;
    }

    @Override
    public Date getGeburtsdatum() {
        return this.pGeburtsdatum;
    }

    @Override
    public void setGeburtsdatum(Date pDate) {
        this.pGeburtsdatum = pDate;
    }

    @Override
    public String getGeburtsort() {
        return this.sGeburtsort;
    }

    @Override
    public void setGeburtsort(String sValue) {
        this.sGeburtsort = sValue;
    }

    @Override
    public String getFamilienstand() {
        return this.sFamilienstand;
    }

    @Override
    public void setFamilienstand(String sValue) {
        this.sFamilienstand = sValue;
    }

    @Override
    public String getReligion() {
        return this.sReligion;
    }

    @Override
    public void setReligion(String sValue) {
        this.sReligion = sValue;
    }

    @Override
    public String getBeruf() {
        return this.sBeruf;
    }

    @Override
    public void setBeruf(String sValue) {
        this.sBeruf = sValue;
    }

    @Override
    public void setGeschlecht(String sValue) {
        this.sGeschlecht = sValue;
    }

    @Override
    public String getGeschlecht() {
        return this.sGeschlecht;
    }

    @Override
    public void setSterbedatum(Date pValue) {
        this.pSterbedatum = pValue;
    }

    @Override
    public Date getSterbedatum() {
        return this.pSterbedatum;
    }
}
