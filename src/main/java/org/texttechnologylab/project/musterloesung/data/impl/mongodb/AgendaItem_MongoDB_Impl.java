package org.texttechnologylab.project.musterloesung.data.impl.mongodb;

import org.bson.Document;
import org.texttechnologylab.project.musterloesung.data.AgendaItem;
import org.texttechnologylab.project.musterloesung.data.PlenaryProtocol;
import org.texttechnologylab.project.musterloesung.data.Speech;
import org.texttechnologylab.project.musterloesung.data.impl.file.AgendaItem_File_Impl;

import java.util.List;

/**
 * @author Giuseppe Abrami
 */
public class AgendaItem_MongoDB_Impl extends AgendaItem_File_Impl implements AgendaItem {

    private Document pDocument = null;

    public AgendaItem_MongoDB_Impl(PlenaryProtocol pProtocol, Document pDocument) {
        super(pProtocol.getFactory());
        this.pDocument = pDocument;
    }

    @Override
    public List<Speech> getSpeeches() {
        return getFactory().getSpeeches(this.getProtocol(), this);
    }

    @Override
    public String getIndex() {
        return pDocument.getString("index");
    }

    @Override
    public String getID() {
        return pDocument.getString("id");
    }

    @Override
    public String getTitle() {
        return pDocument.getString("title");
    }
}
