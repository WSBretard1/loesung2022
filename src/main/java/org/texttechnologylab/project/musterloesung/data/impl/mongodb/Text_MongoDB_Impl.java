package org.texttechnologylab.project.musterloesung.data.impl.mongodb;

import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.bson.Document;
import org.texttechnologylab.project.musterloesung.data.ParliamentFactory;
import org.texttechnologylab.project.musterloesung.data.Speaker;
import org.texttechnologylab.project.musterloesung.data.Speech;
import org.texttechnologylab.project.musterloesung.data.Text;
import org.texttechnologylab.project.musterloesung.data.impl.file.Text_File_Impl;

/**
 * @author Giuseppe Abrami
 */
public class Text_MongoDB_Impl extends Text_File_Impl implements Text {

    protected Document pDocument = null;

    public Text_MongoDB_Impl(ParliamentFactory parliamentFactory, Document pDocument){
        super(parliamentFactory);
        this.pDocument = pDocument;

    }

    public Text_MongoDB_Impl(ParliamentFactory pFactory, Speaker pSpeaker, Speech pSpeech, String sText) {
        super(pSpeaker, pSpeech, sText);
        this.pFactory = pFactory;
    }

}
