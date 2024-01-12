package org.texttechnologylab.project.musterloesung.data.impl.mongodb;

import org.bson.Document;
import org.texttechnologylab.project.musterloesung.data.*;
import org.texttechnologylab.project.musterloesung.data.impl.file.Speaker_File_Impl;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Giuseppe Abrami
 */
public class Speaker_MongoDB_Impl extends Speaker_File_Impl {

    private Document pDocument = null;

    public Speaker_MongoDB_Impl(ParliamentFactory pFactory, Document pDocument) {
        super(pFactory);
        this.pDocument = pDocument;
    }

    @Override
    public String getID() {
        return pDocument.getString("_id");
    }

    @Override
    public String getName() {
        return this.pDocument.getString("name");
    }

    @Override
    public String getFirstName() {
        return this.pDocument.getString("firstName");
    }

    @Override
    public String getRole() {
        return this.pDocument.getString("role");
    }

    @Override
    public String getTitle() {
        return this.pDocument.getString("title");
    }

    @Override
    public Party getParty() {
        if(this.pDocument.containsKey("party")){
            return new Party_MongoDB_Impl(this.getFactory(), this.pDocument.getString("party"));
        }
        return null;
    }

    @Override
    public Fraction getFraction() {
        if(this.pDocument.containsKey("fraction")){
            return new Fraction_MongoDB_Impl(this.getFactory(), this.pDocument.getString("fraction"));
        }
        return null;
    }

    @Override
    public void setParty(Party pParty) {
        this.pDocument.put("party", pParty.getName());
    }

    @Override
    public Date getGeburtsdatum() {
        return super.getGeburtsdatum()!=null ? super.getGeburtsdatum() : this.pDocument.getDate("geburtsdatum");
    }

    @Override
    public Date getSterbedatum() {
        return super.getSterbedatum()!=null ? super.getSterbedatum() :this.pDocument.getDate("sterbedatum");
    }

    @Override
    public String getAkademischerTitel() {
        return super.getAkademischerTitel().length()==0 ? super.getAkademischerTitel() : this.pDocument.getString("akademischertitel");
    }

    @Override
    public String getBeruf() {
        return super.getBeruf().length()>0 ? super.getBeruf() : this.pDocument.getString("beruf");
    }

    @Override
    public String getReligion() {
        return super.getReligion().length()>0 ? super.getReligion() : this.pDocument.getString("religion");
    }

    @Override
    public String getGeschlecht() {
        return super.getGeschlecht().length()>0 ? super.getGeschlecht() : this.pDocument.getString("geschlecht");
    }

    @Override
    public String getGeburtsort() {
        return super.getGeburtsort().length()>0 ? super.getGeburtsort() : this.pDocument.getString("geburtsort");
    }

    @Override
    public String getFamilienstand() {
        return super.getFamilienstand().length()>0 ? super.getFamilienstand() : this.pDocument.getString("familienstand");
    }

    @Override
    public void addAbsense(PlenaryProtocol pProtocol) {
        List<Integer> iList = new ArrayList<>();
        if(this.pDocument.containsKey("absence")){
            iList = this.pDocument.getList("absence", Integer.class);
        }
        iList.add(pProtocol.getIndex());
        this.pDocument.put("absence", iList);
        update();
    }

    @Override
    public Set<PlenaryProtocol> getAbsences() {

        Set<PlenaryProtocol> rSet = new HashSet<>(0);

        if(!this.pDocument.containsKey("absence")){
            this.pDocument.put("absence", new ArrayList<Integer>());
        }

        List<Integer> iList = this.pDocument.getList("absence", Integer.class);

        for (Integer iValue : iList) {
            rSet.add(pFactory.getProtocol(iValue));
        }

        return rSet;

    }

    private void update(){

        pFactory.updateSpeaker(this);

    }

    @Override
    public Set<Speech> getSpeeches() {
        return pFactory.getSpeeches(this).stream().collect(Collectors.toSet());
    }

    @Override
    public Set<Comment> getComments() {
        return pFactory.getComments(this).stream().collect(Collectors.toSet());
    }
}
