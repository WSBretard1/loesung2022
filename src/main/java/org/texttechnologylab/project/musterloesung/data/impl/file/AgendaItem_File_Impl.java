package org.texttechnologylab.project.musterloesung.data.impl.file;

import org.texttechnologylab.project.musterloesung.data.AgendaItem;
import org.texttechnologylab.project.musterloesung.data.ParliamentFactory;
import org.texttechnologylab.project.musterloesung.data.PlenaryProtocol;
import org.texttechnologylab.project.musterloesung.data.Speech;
import org.texttechnologylab.project.musterloesung.helper.XMLHelper;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Implementation of AgendaItem based on Files
 * @author Giuseppe Abrami
 */
public class AgendaItem_File_Impl extends PlenaryObject_File_Impl implements AgendaItem {

    // Declaration of variables for a protocol
    private PlenaryProtocol pProtocol = null;
    private String sIndex = "";
    private String sTitle = "";
    private List<Speech> pSpeeches = new ArrayList<>(0);

    /**
     * Constructor
     * @param pFactory
     */
    public AgendaItem_File_Impl(ParliamentFactory pFactory){
        super(pFactory);
    }


    /**
     * Constructor
     * @param pProtocol
     * @param n
     */
    public AgendaItem_File_Impl(PlenaryProtocol pProtocol, Node n){
        super(pProtocol.getFactory());
        this.pProtocol = pProtocol;
        init(n);
    }

    /**
     * Initialization based on a node
     * @param n
     */
    private void init(Node n){

        // Reading the table of contents
        List<Node> pNodes = XMLHelper.getNodesFromXML(n, "ivz-block-titel");

        if(pNodes.size()==1){
            Node current = pNodes.stream().findFirst().get();

            this.setIndex(current.getTextContent().replace(":", ""));

            List<Node> contentNodes = XMLHelper.getNodesFromXML(current.getParentNode(), "ivz-eintrag-inhalt");
            if(contentNodes.size()>0) {

                StringBuilder sb = new StringBuilder();

                for (Node contentNode : contentNodes) {
                    if(current.getNodeType()==Node.TEXT_NODE) {
                        if (sb.length() > 0) {
                            sb.append("\n");
                        }
                        sb.append(contentNode.getTextContent());
                    }
                }

                this.setTitle(sb.toString());
            }

        }

        // Query of the speeches in the agenda item
        NodeList nl = ((PlenaryProtocol_File_Impl)getProtocol()).getFile().getElementsByTagName("tagesordnungspunkt");

        for(int a=0; a<nl.getLength(); a++){
            Node top = nl.item(a);
            if(top.getAttributes().getNamedItem("top-id").getTextContent().equals(this.getIndex())){
                List<Node> pRede = XMLHelper.getNodesFromXML(top, "rede");

                // Create a speech for each speech in the agenda
                pRede.forEach(r->{
                    Speech pSpeech = new Speech_File_Impl(this, r);
                });

            }
        }

    }

    @Override
    public List<Speech> getSpeeches() {
        return this.pSpeeches;
    }

    @Override
    public void addSpeech(Speech pValue) {
        this.pSpeeches.add(pValue);
    }

    @Override
    public void addSpeeches(Set<Speech> pSet) {
        pSet.forEach(s->{
            this.pSpeeches.add(s);
        });
    }

    @Override
    public String getIndex() {
        return this.sIndex;
    }

    @Override
    public void setIndex(String sValue) {
        this.sIndex = sValue;
    }

    @Override
    public String getTitle() {
        return this.sTitle;
    }

    @Override
    public void setTitle(String sValue) {
        this.sTitle = sValue;
    }

    @Override
    public PlenaryProtocol getProtocol() {
        return this.pProtocol;
    }

    @Override
    public String toString() {
        return getProtocol().getIndex()+"\t"+getIndex()+"\t"+getTitle();
    }

    @Override
    public boolean equals(Object o) {
        return hashCode()==o.hashCode();
    }

    @Override
    public int hashCode() {
        return this.getIndex().hashCode();
    }
}
