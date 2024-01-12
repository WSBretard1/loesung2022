package org.texttechnologylab.project.musterloesung.data.impl.mongodb;

import org.bson.Document;
import org.texttechnologylab.project.musterloesung.data.AgendaItem;
import org.texttechnologylab.project.musterloesung.data.ParliamentFactory;
import org.texttechnologylab.project.musterloesung.data.PlenaryProtocol;
import org.texttechnologylab.project.musterloesung.data.Speaker;
import org.texttechnologylab.project.musterloesung.data.impl.file.PlenaryProtocol_File_Impl;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Giuseppe Abrami
 */
public class PlenaryProtocol_MongoDB_Impl extends PlenaryProtocol_File_Impl implements PlenaryProtocol {

    private Document pMongoDocument = null;

    /**
     * Constuctor. The constructor needs a ParliamentFactory and the Mongo document
     *
     * @param pFactory
     */
    public PlenaryProtocol_MongoDB_Impl(ParliamentFactory pFactory, Document pMongoDocument) {
        super(pFactory);
        this.pMongoDocument = pMongoDocument;
    }

    /**
     * Constuctor. The constructor needs a ParliamentFactory and the id of the Protocol
     *
     * @param pFactory
     */
    public PlenaryProtocol_MongoDB_Impl(ParliamentFactory pFactory, String sID) {
        super(pFactory);
    }

    @Override
    public Date getDate() {

        return new Date(pMongoDocument.getLong("date"));

    }

    @Override
    public Timestamp getStartTime() {
        return new Timestamp(pMongoDocument.getLong("starttime"));
    }

    @Override
    public Timestamp getEndTime() {
        return new Timestamp(pMongoDocument.getLong("endtime"));
    }

    @Override
    public int getIndex() {
        return pMongoDocument.getInteger("index");
    }

    @Override
    public String getTitle() {
        return pMongoDocument.getString("title");
    }

    @Override
    public int getWahlperiode() {
        return pMongoDocument.getInteger("wp");
    }

    @Override
    public String getPlace() {
        return pMongoDocument.getString("place");
    }

    @Override
    public List<AgendaItem> getAgendaItems() {

        List<AgendaItem> rItems = pFactory.getAgendaItem(this);

        return rItems;

    }

    @Override
    public Set<Speaker> getLeaders() {
        Set<Speaker> rSet = new HashSet<>(0);

        for (Speaker speaker : pFactory.getSpeakers(this)) {
            if(speaker.isLeader()){
                rSet.add(speaker);
            }

        }

        return rSet;
    }
}
