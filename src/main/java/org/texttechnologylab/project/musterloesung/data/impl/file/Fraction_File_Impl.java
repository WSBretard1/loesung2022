package org.texttechnologylab.project.musterloesung.data.impl.file;

import org.texttechnologylab.project.musterloesung.data.Fraction;
import org.texttechnologylab.project.musterloesung.data.Speaker;
import org.w3c.dom.Node;

import java.util.HashSet;
import java.util.Set;

/**
 * Implementation of a fraction
 * @author Giuseppe Abrami
 */
public class Fraction_File_Impl implements Fraction {

    // Variable declaration
    private String sName = "";
    private Set<Speaker> pMembers = new HashSet<>(0);

    public Fraction_File_Impl(String sName){
        this.sName = sName;
    }

    /**
     * Constructed based on a node.
     * @param pNode
     */
    public Fraction_File_Impl(Node pNode){
        init(pNode);
    }

    /**
     * Internal init method by use of the xml-node
     * @param pNode
     */
    private void init(Node pNode){
        this.sName = pNode.getTextContent().trim();
    }

    @Override
    public String getName() {
        return this.sName;
    }

    @Override
    public void addMember(Speaker pSpeaker) {
        this.pMembers.add(pSpeaker);
    }

    @Override
    public Set<Speaker> getMembers() {
        return this.pMembers;
    }

    /**
     * Override default compareTo method
     * @param fraction
     * @return
     */
    @Override
    public int compareTo(Fraction fraction) {
        return this.getName().toLowerCase().compareTo(fraction.getName().toLowerCase());
    }

    @Override
    public boolean equals(Object o) {
        return this.hashCode()==o.hashCode();
    }

    @Override
    public int hashCode() {
        return this.getName().toLowerCase().hashCode();
    }

    @Override
    public String toString() {
        return this.getName();
    }
}
