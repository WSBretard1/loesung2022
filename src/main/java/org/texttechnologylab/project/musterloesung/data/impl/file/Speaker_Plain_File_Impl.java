package org.texttechnologylab.project.musterloesung.data.impl.file;

import org.texttechnologylab.project.musterloesung.data.ParliamentFactory;

/**
 * Special speaker implementation, for all speakers that are not complex nodes.
 * @author Giuseppe Abrami
 */
public class Speaker_Plain_File_Impl extends Speaker_File_Impl {

    /**
     * Constructor
     * @param pFactory
     */
    public Speaker_Plain_File_Impl(ParliamentFactory pFactory) {
        super(pFactory);
    }

    @Override
    public void setName(String sValue) {
        super.setName(transform(sValue));
    }

    @Override
    public String toString() {
        return this.getName();
    }

    @Override
    public boolean isLeader() {
        return super.isLeader();
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    /**
     * Transform-Method for some "errors" in the data
     * @param sValue
     * @return
     */
    public static String transform(String sValue){

        String sReturn = sValue;

        sReturn = sReturn.replaceAll("Vizepräsident in", "Vizepräsidentin");
        sReturn = sReturn.replaceAll("Vizepräsiden t", "Vizepräsident");
        sReturn = sReturn.replaceAll(":", "");

        return sReturn;

    }
}
