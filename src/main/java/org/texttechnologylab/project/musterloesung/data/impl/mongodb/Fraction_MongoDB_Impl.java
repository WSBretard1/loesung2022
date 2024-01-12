package org.texttechnologylab.project.musterloesung.data.impl.mongodb;

import org.texttechnologylab.project.musterloesung.data.ParliamentFactory;
import org.texttechnologylab.project.musterloesung.data.Speaker;
import org.texttechnologylab.project.musterloesung.data.impl.file.Fraction_File_Impl;

import java.util.Set;

/**
 * @author Giuseppe Abrami
 */
public class Fraction_MongoDB_Impl extends Fraction_File_Impl {

    ParliamentFactory parliamentFactory = null;
    /**
     * Constructed based on a node.
     *
     * @param sName
     */
    public Fraction_MongoDB_Impl(ParliamentFactory pFactory, String sName) {
        super(sName);
        this.parliamentFactory = pFactory;
    }

    @Override
    public Set<Speaker> getMembers() {
        return this.parliamentFactory.getMembers(this);
    }


}
