package org.texttechnologylab.project.musterloesung.data.impl.mongodb;

import org.texttechnologylab.project.musterloesung.data.ParliamentFactory;
import org.texttechnologylab.project.musterloesung.data.Speaker;
import org.texttechnologylab.project.musterloesung.data.impl.file.Party_File_Impl;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Giuseppe Abrami
 */
public class Party_MongoDB_Impl extends Party_File_Impl {

    ParliamentFactory pFactory = null;
    /**
     * Constructor based on the Name of a Party
     *
     * @param pFactory
     * @param sName
     */
    public Party_MongoDB_Impl(ParliamentFactory pFactory, String sName) {
        super(sName);
        this.pFactory = pFactory;
    }

    @Override
    public Set<Speaker> getMembers() {
        Set<Speaker> rSet = new HashSet<>(0);

        return rSet;
    }
}
