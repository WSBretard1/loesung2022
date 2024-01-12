package org.texttechnologylab.project.musterloesung.data;

import java.util.Date;
import java.util.Set;

/**
 * Interface for mapping speakers
 * @author Giuseppe Abrami
 */
public interface Speaker extends PlenaryObject {

    /**
     * Return the party of the speaker
     * @return
     */
    Party getParty();

    /**
     * Set the party of the speaker
     * @param pParty
     */
    void setParty(Party pParty);

    /**
     * Return the fraction of the speaker
     * @return
     */
    Fraction getFraction();

    /**
     * Set the fraction of the speaker
     * @param pFraction
     */
    void setFraction(Fraction pFraction);

    /**
     * Return the role of the speaker
     * @return
     */
    String getRole();

    /**
     * Set the role of the speaker
     * @param sValue
     */
    void setRole(String sValue);

    /**
     * Return the title of the speaker
     * @return
     */
    String getTitle();

    /**
     * Set the title of the speaker
     * @param sValue
     */
    void setTitle(String sValue);

    /**
     * Return the surname of the speaker
     * @return
     */
    String getName();

    /**
     * Set the name of the speaker
     * @param sValue
     */
    void setName(String sValue);

    /**
     * Return the firstname of the speaker
     * @return
     */
    String getFirstName();

    /**
     * Set the firstname of the speaker
     * @param sValue
     */
    void setFirstName(String sValue);

    /**
     * Return all speeches of the speaker
     * @return
     */
    Set<Speech> getSpeeches();

    /**
     * Add a speech to the speaker
     * @param pSpeech
     */
    void addSpeech(Speech pSpeech);

    /**
     * Add multiple speeches to the speaker
     * @param pSet
     */
    void addSpeeches(Set<Speech> pSet);

    /**
     * Return all comments the speaker received
     * @return
     */
    Set<Comment> getComments();

    /**
     * Return the avg length of all speeches
     * @return
     */
    float getAvgLength();

    /**
     * Return true if the speaker is a leader of a Session
     * @return
     */
    boolean isLeader();

    /**
     * Return true if the speaker is part of the Government
     * @return
     */
    boolean isGovernment();

    /**
     * Return the Sessions where a speaker was absence
     * @return
     */
    Set<PlenaryProtocol> getAbsences();

    /**
     * Get the Quote of Absence by Wahlperiode
     * @param iWP
     * @return
     */
    float getAbsences(int iWP);


    /**
     * Add an absence to a speaker
     * @param pProtocol
     */
    void addAbsense(PlenaryProtocol pProtocol);

    String getAkademischerTitel();
    void setAkademischerTitel(String sValue);
    Date getGeburtsdatum();
    void setGeburtsdatum(Date pDate);

    String getGeburtsort();
    void setGeburtsort(String sValue);

    String getFamilienstand();
    void setFamilienstand(String sValue);

    String getReligion();
    void setReligion(String sValue);

    String getBeruf();
    void setBeruf(String sValue);

    void setGeschlecht(String sValue);
    String getGeschlecht();

    void setSterbedatum(Date pValue);
    Date getSterbedatum();
}
