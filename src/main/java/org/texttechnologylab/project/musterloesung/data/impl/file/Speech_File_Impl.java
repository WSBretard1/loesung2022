package org.texttechnologylab.project.musterloesung.data.impl.file;

import org.apache.uima.UIMAException;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.project.musterloesung.data.*;
import org.texttechnologylab.project.musterloesung.helper.XMLHelper;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of speech mapping
 * @author Giuseppe Abrami
 */
public class Speech_File_Impl extends PlenaryObject_File_Impl implements Speech {

    private AgendaItem pAgenda = null;
    private List<Text> textContent = new ArrayList<>();

    private List<Speech> pInsertions = new ArrayList<>();

    private Speaker pSpeaker = null;

    /**
     * Construktor
     * @param pFactory
     */
    public Speech_File_Impl(ParliamentFactory pFactory) {
        super(pFactory);
    }

    /**
     * Construktor
     * @param pAgenda
     * @param pNode
     */
    public Speech_File_Impl(AgendaItem pAgenda, Node pNode) {
        super(pAgenda.getProtocol().getFactory());
        this.pAgenda = pAgenda;
        this.pAgenda.addSpeech(this);
        init(pNode);

    }

    /**
     * Constructor
     * @param pAgenda
     * @param sID
     */
    public Speech_File_Impl(AgendaItem pAgenda, String sID) {
        super(pAgenda.getProtocol().getFactory());
        this.pAgenda = pAgenda;
        this.setID(sID);
    }

    /**
     * Initialization based on an XML-node
     * @param pNode
     */
    private void init(Node pNode) {
        this.setID(pNode.getAttributes().getNamedItem("id").getTextContent());

        // get Redner

        NodeList nL = pNode.getChildNodes();

        Speaker currentSpeaker = null;
        Speech currentSpeech = this;

        int optionalSpeech = 1;

        for (int a = 0; a < nL.getLength(); a++) {
            Node n = nL.item(a);

            switch (n.getNodeName()) {

                case "p":

                    String sKlasse = "";
                    if (n.hasAttributes()) {
                        sKlasse = n.getAttributes().getNamedItem("klasse").getTextContent();
                    }

                    switch (sKlasse) {
                        case "redner":
                            Speaker nSpeaker = this.getFactory().getSpeaker(XMLHelper.getSingleNodesFromXML(n, "redner"));
                            currentSpeaker = nSpeaker;
                            currentSpeech = this;
                            nSpeaker.addSpeech(currentSpeech);
                            this.pSpeaker = nSpeaker;

                            break;
                        case "n":

                            Speaker tSpeaker = this.getFactory().getSpeaker(n);
                            currentSpeaker = tSpeaker;
                            tSpeaker.addSpeech(currentSpeech);

                            break;

                        default:
                            currentSpeech.addText(new Text_File_Impl(currentSpeaker, currentSpeech, n.getTextContent()));
                    }

                    break;

                case "name":

                    Speaker nSpeaker = this.getFactory().getSpeaker(n);
                    if (nSpeaker == this.getSpeaker()) {
                        currentSpeech = this;
                    } else {
                        if (currentSpeaker != nSpeaker && nSpeaker != null) {
                            currentSpeaker = nSpeaker;
                            currentSpeech = new Speech_File_Impl(getAgendaItem(), getID() + "-" + optionalSpeech);
                            currentSpeaker.addSpeech(currentSpeech);
                            currentSpeech.setSpeaker(currentSpeaker);
                            pInsertions.add(currentSpeech);
                            optionalSpeech++;
                        }
                    }

                    break;

                case "kommentar":
                    Comment_File_Impl pComment = new Comment_File_Impl(n);
                    pComment.setSpeech(this);
                    pComment.setSpeaker(this.getSpeaker());
                    textContent.add(pComment);
                    break;

            }

        }


    }

    @Override
    public AgendaItem getAgendaItem() {
        return this.pAgenda;
    }

    @Override
    public List<Comment> getComments() {
        List<Comment> rList = new ArrayList<>();
        this.textContent.stream().filter(c -> c instanceof Comment).forEach(c -> {
            rList.add((Comment) c);
        });
        return rList;
    }

    @Override
    public String getText() {
        StringBuilder sb = new StringBuilder();
        textContent.forEach(t -> {
            if (sb.length() > 0) {
                sb.append(" ");
            }
            if (t instanceof Comment) {
                sb.append("\n\n");
                sb.append("\t" + t.getContent());
                sb.append("\n\n");
            } else {
                sb.append(t.getContent());
            }


        });
        return sb.toString();
    }

    @Override
    public String getPlainText(){
        StringBuilder sb = new StringBuilder();

        textContent.forEach(t->{
            if (sb.length() > 0) {
                sb.append(" ");
            }
            if(!(t instanceof Comment)){
                sb.append(t.getContent());
            }
        });

        return sb.toString();
    }

    @Override
    public PlenaryProtocol getProtocol() {
        return this.getAgendaItem().getProtocol();
    }

    @Override
    public Speaker getSpeaker() {
        return this.pSpeaker;
    }

    @Override
    public void setSpeaker(Speaker pSpeaker) {
        this.pSpeaker = pSpeaker;
    }

    @Override
    public int getLength() {
        return getText().length();
    }

    @Override
    public List<Speech> getInsertions() {
        return pInsertions;
    }

    @Override
    public void addText(Text pText) {
        this.textContent.add(pText);
    }

    @Override
    public JCas toCas() throws UIMAException {
        return null;
    }

    @Override
    public void setCas(JCas pCas) {

    }

    @Override
    public String toString() {
        return pAgenda.toString();
    }


}
