package org.texttechnologylab.project.musterloesung.data.impl.file;

import org.texttechnologylab.project.musterloesung.data.ParliamentFactory;
import org.texttechnologylab.project.musterloesung.data.PlenaryObject;

/**
 * Implementation of the plenary object
 * @author Giuseppe Abrami
 */
public class PlenaryObject_File_Impl implements PlenaryObject {

    // Variables declarations
    private String sID = "";
    private int iWahlperiode = -1;

    //ParlamentaryFactory, all inheriting classes can also access it directly.
    protected ParliamentFactory pFactory = null;

    /**
     * Construcot
     * @param pFactory
     */
    public PlenaryObject_File_Impl(ParliamentFactory pFactory){
        this.pFactory = pFactory;
    }

    public PlenaryObject_File_Impl(){

    }

    @Override
    public String getID() {
        return this.sID;
    }

    @Override
    public void setID(String lID) {
        this.sID = lID;
    }

    @Override
    public int getWahlperiode() {
        return this.iWahlperiode;
    }

    @Override
    public void setWahlperiode(int iValue) {
        this.iWahlperiode=iValue;
    }

    @Override
    public ParliamentFactory getFactory() {
        return this.pFactory;
    }

    @Override
    public int compareTo(PlenaryObject plenaryObject) {
        return getID().compareTo(plenaryObject.getID());
    }

    @Override
    public boolean equals(Object o) {
        return o.hashCode()==this.hashCode();
    }

    @Override
    public int hashCode() {
        return getID().hashCode();
    }
}
