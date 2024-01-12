package org.texttechnologylab.project.musterloesung;


import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.commons.cli.*;
import org.apache.uima.UIMAException;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.project.musterloesung.data.*;
import org.texttechnologylab.project.musterloesung.data.impl.file.ParliamentFactory_Impl;
import org.texttechnologylab.project.musterloesung.data.impl.file.PlenaryProtocol_File_Impl;
import org.texttechnologylab.project.musterloesung.data.impl.file.Speaker_Plain_File_Impl;
import org.texttechnologylab.project.musterloesung.data.impl.mongodb.Comment_MongoDB_Impl;
import org.texttechnologylab.project.musterloesung.data.impl.mongodb.Speaker_MongoDB_Impl;
import org.texttechnologylab.project.musterloesung.data.impl.mongodb.Speech_MongoDB_Impl;
import org.texttechnologylab.project.musterloesung.database.MongoDBConfig;
import org.texttechnologylab.project.musterloesung.database.MongoDBConnectionHandler;
import org.texttechnologylab.project.musterloesung.exceptions.InputException;
import org.texttechnologylab.project.musterloesung.exceptions.ParamException;
import org.texttechnologylab.project.musterloesung.helper.FileReader;
import org.texttechnologylab.project.musterloesung.helper.NLPHelper;
import org.texttechnologylab.project.musterloesung.helper.StringHelper;
import org.texttechnologylab.project.musterloesung.helper.XMLHelper;
import org.texttechnologylab.utilities.helper.FileUtils;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Giuseppe Abrami
 * @date 10.12.2022
 * <p>
 * This is the Main class for performing exercise task 1.
 */
public class ParAnSy {

    // Define a factory for centralized access to resources.
    private ParliamentFactory pFactory = null;

    /**
     * Main method
     *
     * @param args
     * @throws ParseException
     */
    public static void main(String[] args) throws ParseException, ParamException, IOException {

        System.out.println("Welcome to");
        System.out.println(getWelcome());
        System.out.println("======================================================================================================");

        // Init
        ParAnSy pManager = new ParAnSy();

        String ddcPath = ParAnSy.class.getClassLoader().getResource("ddc3-names-de.csv").getPath();
        FileReader.initDDC(new File(ddcPath));

        // create Options object
        Options options = new Options();

        // add f option for an Input File
        options.addOption("f", true, "Path to Parliament-Data");
        options.addOption("s", true, "Path to Stammdaten");
        options.addOption("db", true, "Path to DB-Config");

        // creating a parser and parse
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if(cmd.hasOption("db")){

            /*
                Loading database-path and connect to database
             */
            String sDatabaseConfigPath = cmd.getOptionValue("db");
            try {
                MongoDBConfig dbConfig = new MongoDBConfig(sDatabaseConfigPath);
                pManager.createDatabaseConnection(dbConfig);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        else{
            String dbDefaultPath = ParAnSy.class.getClassLoader().getResource("database_read.txt").getPath();
            MongoDBConfig dbConfig = new MongoDBConfig(dbDefaultPath);
            pManager.createDatabaseConnection(dbConfig);
        }

        if (cmd.hasOption("f")) {
            // if option is set, then use the specific path for the data
            String sInputPath = cmd.getOptionValue("f");
            Set<PlenaryProtocol> pProtocols = pManager.startImportFromFile(sInputPath);


        if (cmd.hasOption("s")) {
            // if option is set, then use the specific path for the data
            sInputPath = cmd.getOptionValue("s");
            try {
                pManager.startMDBStammdaten(sInputPath);
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SAXException e) {
                e.printStackTrace();
            }
        }
        else if(cmd.hasOption("f")){
            try {
                /*
                If no master data was given, no matter: let's pull dynamically from the Bundestag.
                 */
                File pFile = FileUtils.downloadFile("https://www.bundestag.de/resource/blob/472878/4d360eba29319547ed7fce385335a326/MdB-Stammdaten-data.zip");

                Set<File> extracted = FileReader.unzipFile(pFile);
                extracted.stream().filter(f->f.getName().toLowerCase().endsWith(".xml")).forEach(f->{
                    try {
                        pManager.startMDBStammdaten(f.getPath());
                    } catch (ParserConfigurationException e) {
                        throw new RuntimeException(e);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } catch (SAXException e) {
                        throw new RuntimeException(e);
                    }
                });

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        }

        // Start the menu after importing the data
        pManager.startMenue();

    }

    /**
     * Method to create a Database connection
     * @param pConfig
     */
    public void createDatabaseConnection(MongoDBConfig pConfig){
        this.pFactory.createDatabaseConnection(pConfig);
    }

    /**
     * Menu selection
     */
    public void startMenue() {

        startMenuUeb3();

    }

    /**
     * Old Menü for Exercise 1
     * @deprecated
     */
    public void startMenueUeb1(){

        String sCmd = "";

        // Scanner method to query console inputs.
        Scanner scanner = new Scanner(System.in);

        // Query the individual input methods
        while (!sCmd.equalsIgnoreCase("exit")) {

            try {



                System.out.println(getMenu());
                System.out.println("========================");
                System.out.println("Please make a selection:");
                System.out.println("\t search");
                System.out.println("\t list");
                System.out.println("\t length");
                System.out.println("\t sessions");
                System.out.println("\t speeches");
                System.out.println("\t leaders");
                System.out.println("\t comments");
                System.out.println("\t absences");
                System.out.println("\t exit");

                sCmd = scanner.nextLine();

                switch (sCmd) {

                    case "exit": {
                        System.out.println(getExit());
                        break;
                    }

                    // submenue of search

                    case "search": {
                        System.out.println("\t insert search string:");
                        String sValue = scanner.nextLine();

                        System.out.println("\t insert limit:");
                        int iLimit = Integer.valueOf(scanner.nextLine());

                        pFactory.fullTextSearch(sValue).stream().limit(iLimit).forEach(s->{
                            System.out.println(s.getID());
                            System.out.println(s.getText().substring(0, 50));
                            System.out.println(s.getSpeaker());
                            System.out.println("================================================");
                        });


                    }
                    break;

                    // Submenue of "list"
                    case "list":

                        System.out.println("\t all");
                        System.out.println("\t filter");
                        System.out.println("\t fraction");
                        System.out.println("\t party");
                        System.out.println("\t top");
                        System.out.println("> ");

                        String sType = scanner.nextLine();

                        switch (sType) {
                            case "top":

                                this.getFactory().getProtocols().stream().sorted(Comparator.comparing(PlenaryProtocol::getDate)).forEach(p -> {
                                    System.out.println("\t" + p.getDateFormated() + "\t" + p.getTitle());
                                });

                                System.out.println("Session?");

                                System.out.println("> ");

                                String sSelect = scanner.nextLine();

                                sSelect = sSelect.replaceAll(" ", "");
                                String[] sSplit = sSelect.split("/");

                                PlenaryProtocol pProtocol = null;

                                if (sSplit.length == 2) {

                                    AtomicInteger iTop = new AtomicInteger(1);
                                    pProtocol = this.getFactory().getProtocols().stream()
                                            .filter(p -> p.getWahlperiode() == Integer.valueOf(sSplit[0]))
                                            .filter(p -> p.getIndex() == Integer.valueOf(sSplit[1])).findFirst().get();

                                    if (pProtocol != null) {

                                        pProtocol.getAgendaItems().stream().sorted().forEach(aI -> {
                                            System.out.println("(" + iTop + ")\t" + aI.getIndex() + "\t" + aI.getTitle());
                                            iTop.getAndIncrement();
                                        });

                                        System.out.println("TOP?");
                                        System.out.println("> ");
                                        String sSelectTop = scanner.nextLine();

                                        int iSelection = Integer.valueOf(sSelectTop);

                                        AgendaItem pItem = pProtocol.getAgendaItems().get((iSelection - 1));

                                        System.out.println(pItem.getID() + "\t" + pItem.getIndex());
                                        pItem.getSpeeches().forEach(speech -> {
                                            System.out.println("---------------------------------------------------------------------");
                                            System.out.println(speech.getID() + "\t" + speech.getSpeaker());
                                            System.out.println(speech.getText());

                                        });

                                    }

                                }


                                break;
                            case "all":

                                // Return all users and list them sorted
                                this.getFactory().getSpeakers().stream().filter(s -> {
                                    return !(s instanceof Speaker_Plain_File_Impl);
                                }).sorted(Comparator.comparing(s -> s.getName().toLowerCase())).forEach(speaker -> {
                                    System.out.println(speaker);
                                });
                                break;

                            case "filter":

                                // Filter: How often should the hashtags to be listed occur in total?
                                System.out.println("\t search?");
                                String sValue = scanner.nextLine();

                                this.getFactory().getSpeakers().stream()
                                        .filter(s -> {
                                            return s.toString().toLowerCase().contains(sValue.toLowerCase());
                                        }).sorted().forEach(s -> {
                                            System.out.println(s);
                                        });

                                break;

                            case "fraction":

                                AtomicInteger iFraction = new AtomicInteger(1);
                                getFactory().getFractions().stream().forEach(pFraction -> {
                                    System.out.println("\t (" + (iFraction.getAndIncrement()) + ") " + pFraction.getName());
                                });

                                int iArg = Integer.valueOf(scanner.nextLine());

                                Fraction pFraction = getFactory().getFractions().stream().collect(Collectors.toList()).get(iArg - 1);
                                System.out.println("Fraction: " + pFraction.getName());
                                pFraction.getMembers().stream().sorted().forEach(pMember -> {
                                    System.out.println("\t" + pMember);
                                });


                                break;

                            case "party":

                                AtomicInteger iParty = new AtomicInteger(1);
                                getFactory().getParties().stream().sorted(Comparator.comparingInt(p -> p.getMembers().size())).forEach(pParty-> {
                                    System.out.println("\t (" + (iParty.getAndIncrement()) + ") " + pParty.getName());
                                });

                                int iArgParty = Integer.valueOf(scanner.nextLine());

                                Party pParty = getFactory().getParties().stream().sorted(Comparator.comparingInt(p -> p.getMembers().size())).collect(Collectors.toList()).get(iArgParty - 1);
                                System.out.println("Party: " + pParty.getName());
                                pParty.getMembers().stream().sorted(Comparator.comparing(Speaker::getName)).forEach(pMember -> {
                                    System.out.println("\t" + pMember.getTitle()+"\t"+pMember.getFirstName()+"\t"+pMember.getName());
                                });


                                break;

                            default:
                                throw new InputException("Unknown cmd " + sType);
                        }
                        break;

                    case "length": {

                        System.out.println("\t speeches");
                        System.out.println("\t speaker");
                        System.out.println("\t fraction");
                        System.out.println("\t short_long");
                        String sValue = scanner.nextLine();

                        switch (sValue) {

                            case "short_long":
                            {
                                List<Speech> sList = new ArrayList<>();
                                this.getFactory().getSpeakers().forEach(speaker -> {
                                    sList.addAll(speaker.getSpeeches());
                                });

                                Speech minRede = sList.stream().filter(s1->s1.getLength()>0).min((s1, s2) -> Integer.valueOf(s1.getLength()).compareTo(s2.getLength())).get();

                                Speech maxRede = sList.stream().max((s1, s2) -> Integer.valueOf(s1.getLength()).compareTo(s2.getLength())).get();

                                System.out.println("Kürzeste Rede "+minRede.getProtocol().getDateFormated()+"\t"+minRede.getAgendaItem().toString()+" \t "+minRede.getLength());
                                System.out.println("Längste Rede "+maxRede.getProtocol().getDateFormated()+"\t"+maxRede.getAgendaItem().toString()+" \t "+maxRede.getLength());
                            }
                            break;

                            case "speeches":
                                List<Speech> sList = new ArrayList<>();
                                this.getFactory().getSpeakers().forEach(speaker -> {
                                    sList.addAll(speaker.getSpeeches());
                                });
                                float avgLength = sList.stream().mapToInt(Speech::getLength).sum() / sList.size();

                                System.out.println(avgLength);
                                break;

                            case "speaker":

                                this.getFactory().getSpeakers().stream().sorted((a1, a2) -> Float.compare(a1.getAvgLength(), a2.getAvgLength()) * -1).forEach(speaker -> {
                                    System.out.println(speaker.getAvgLength() + "\t" + speaker);
                                });

                                break;

                            case "fraction":

                                this.getFactory().getFractions().stream().sorted().forEach(fraction -> {
                                    int iSum = fraction.getMembers().stream().mapToInt(m -> {
                                        int iLength = m.getSpeeches().stream().mapToInt(Speech::getLength).sum();
                                        return iLength;
                                    }).sum();
                                    System.out.println("Fraction " + fraction.getName() + "\t" + iSum / fraction.getMembers().stream().mapToInt(f -> f.getSpeeches().size()).sum());

                                });

                                break;

                            default:

                                throw new InputException("Unknown cmd " + sValue);

                        }
                    }
                    break;

                    case "speeches":

                        System.out.println("\t amount");
                        System.out.println("> ");

                        int iValue = Integer.valueOf(scanner.nextLine());

                        Set<Speech> speechList = new HashSet<>(0);
                        this.getFactory().getSpeakers().stream().map(Speaker::getSpeeches).forEach(speechList::addAll);

                        speechList.stream().sorted((s1, s2) -> {
                                    return Integer.valueOf(s1.getComments().size()).compareTo(s2.getComments().size()) * -1;
                                }).collect(Collectors.toList()).subList(0, (iValue > 0) ? iValue : 1)
                                .forEach(s -> {
                                    System.out.println("Session: " + s.getProtocol().getDateFormated() + "\t" + s.getAgendaItem().getIndex() + "\t" + s.getID() + "\t" + s.getSpeaker().getName() + "\t " + (s.getSpeaker().getFraction() != null ? "(" + s.getSpeaker().getFraction() + ") " : "") + " \t Comments: " + s.getComments().size());
                                });

                        break;

                    case "leaders":

                        Map<Speaker, Integer> leadingMap = new HashMap<>(0);

                        this.getFactory().getProtocols().stream().forEach(p -> {
                            p.getLeaders().forEach(l -> {
                                if (leadingMap.containsKey(l)) {
                                    leadingMap.put(l, leadingMap.get(l) + 1);
                                } else {
                                    leadingMap.put(l, 1);
                                }
                            });
                        });

                        leadingMap.entrySet().stream().sorted((e1, e2) -> {
                            return e1.getValue().compareTo(e2.getValue()) * -1;
                        }).forEach(s -> {
                            System.out.println(s.getValue() + "\t" + s.getKey());
                        });


                        break;

                    case "sessions":

                        try {
                            this.getFactory().getProtocols().stream().sorted((p1, p2) -> {
                                return Long.valueOf(p1.getDuration()).compareTo(p2.getDuration())*-1;
                            }).forEach(p -> {

                                try {
                                    System.out.println("\nDauer: "+p.getDurationFormated()+"\t Sitzung Nr. "+p.getIndex()+" vom "+p.getDateFormated()+" von \t"+p.getStartTimeFormated()+" bis "+p.getEndTimeFormated()+"\n=======================");
                                    pFactory.getFractions().stream().sorted((f1, f2)->{
                                        return Integer.valueOf(p.getSpeakers(f1).size()).compareTo(p.getSpeakers(f2).size())*-1;
                                    }).forEach(f -> {
                                        System.out.println("\t ("+p.getSpeakers(f).size()+") "+f.getName());
                                    });
                                } catch (InputException e) {
                                    System.out.println(p.getIndex()+": "+e.getMessage());
                                }



                            });
                        }
                        catch (Exception e){
                            e.printStackTrace();
                        }

                        break;

                    case "absences":

                        System.out.println("\t WP");
                        System.out.println("> ");

                        int iWP = Integer.valueOf(scanner.nextLine());

                        System.out.println("\t mdb");
                        System.out.println("\t fraction");
                        System.out.println("\t government");
                        System.out.println("\t avg");
                        System.out.println("> ");

                        String sSelect = scanner.nextLine();

                        switch (sSelect){

                            case "mdb":
                                System.out.println("Parlamentaries:");
                                this.getFactory().getSpeakers().stream().filter(s->{
                                    return s.getAbsences(iWP)>0;
                                }).sorted((s1, s2)->{
                                    return Float.valueOf(s1.getAbsences(iWP)).compareTo(s2.getAbsences(iWP));
                                }).forEach(s->{
                                    System.out.println(s.getFirstName()+" "+s.getName()+"\t ("+s.getFraction()+") \t"+s.getAbsences(iWP));
                                });
                                break;

                            case "fraction":

                                List<Fraction> fList = this.getFactory().getFractions().stream().collect(Collectors.toList());

                                for(int a=0; a<fList.size(); a++){
                                    System.out.println("("+a+") \t"+fList.get(a).getName());
                                }
                                System.out.println("> ");

                                int iFractionSelection = Integer.valueOf(scanner.nextLine());

                                Fraction selectedFraction = fList.get(iFractionSelection);
                                System.out.println("\nFraction: "+selectedFraction.getName()+"\n=========================");
                                this.getFactory().getSpeakers(selectedFraction).stream().filter(s->{
                                    return s.getAbsences(iWP)>0;
                                }).sorted((s1, s2)->{
                                    return Float.valueOf(s1.getAbsences(iWP)).compareTo(s2.getAbsences(iWP));
                                }).forEach(s->{
                                    System.out.println(s.getFirstName()+" "+s.getName()+"\t ("+s.getFraction()+") \t"+s.getAbsences(iWP));
                                });


                                break;

                            case "government":

                                this.getFactory().getSpeakers().stream().filter(s->{
                                            return s.getAbsences(iWP)>0;
                                        })
                                        .filter(s->s.isGovernment())
                                        .sorted((s1, s2)->{
                                            return Float.valueOf(s1.getAbsences(iWP)).compareTo(s2.getAbsences(iWP));
                                        }).forEach(s->{
                                            System.out.println(s.getFirstName()+" "+s.getName()+"\t"+s.getAbsences(iWP));
                                        });

                                break;


                            case "avg":

                                this.getFactory().getFractions().stream().forEach(f->{
                                    double avgCount = this.getFactory().getSpeakers(f).stream().filter(s->{
                                        return s.getAbsences(iWP)>0;
                                    }).mapToDouble(s->{
                                        return s.getAbsences(iWP);
                                    }).average().getAsDouble();
                                    System.out.println("\nFraction: "+f.getName()+"\t"+avgCount+"\n=========================");
                                });

                                double avgGov = this.getFactory().getSpeakers().stream().filter(s->{
                                            return s.getAbsences(iWP)>0;
                                        })
                                        .filter(s->s.isGovernment())
                                        .sorted((s1, s2)->{
                                            return Float.valueOf(s1.getAbsences(iWP)).compareTo(s2.getAbsences(iWP));
                                        }).mapToDouble(s->{
                                            return s.getAbsences(iWP);
                                        }).average().getAsDouble();

                                System.out.println("AVG Government:\t"+avgGov);


                                break;


                        }


                        break;

                    case "comments":

                        System.out.println("\t speaker");
                        System.out.println("\t party");
                        System.out.println("\t fraction");
                        System.out.println("> ");

                        String cValue = scanner.nextLine();

                        switch (cValue) {

                            case "party":

                                this.getFactory().getParties().stream().sorted((p1, p2)->{
                                    int p1Sum = p1.getMembers().stream().mapToInt(m -> m.getComments().size()).sum();
                                    int p2Sum = p2.getMembers().stream().mapToInt(m -> m.getComments().size()).sum();

                                    return Integer.compare(p1Sum, p2Sum);
                                }).forEach(pParty->{
                                    Set<Comment> pComments = new HashSet<>(0);
                                    Set<Speech> pSpeeches = new HashSet<>(0);
                                    pParty.getMembers().stream().forEach(m -> {
                                        pComments.addAll(m.getComments());
                                        pSpeeches.addAll(m.getSpeeches());
                                    });

                                    System.out.println(pParty + "\t (" + pComments.size() + ")\t AVG:\t" + ((float) pComments.size() / (float) pSpeeches.size()));

                                    Map<Party, Integer> commentMap = new HashMap<>(0);

                                    pComments.stream().forEach(c -> {

                                        this.getFactory().getParties().forEach(f -> {

                                            if (c.getContent().toLowerCase().contains(f.getName().toLowerCase())) {

                                                if (commentMap.containsKey(f)) {
                                                    commentMap.put(f, commentMap.get(f) + 1);
                                                } else {
                                                    commentMap.put(f, 1);
                                                }

                                            }

                                        });

                                    });

                                    commentMap.entrySet().stream().sorted((f1, f2) -> {
                                        return f1.getValue().compareTo(f2.getValue()) * -1;
                                    }).forEach(fS -> {
                                        System.out.println("\t\t" + fS.getKey() + "\t (" + fS.getValue() + ")\t AVG:\t" + ((float) fS.getValue() / (float) pComments.size()));
                                    });
                                });

                                break;

                            case "fraction":

                                this.getFactory().getFractions().stream().sorted((p1, p2) -> {
                                    int p1Sum = p1.getMembers().stream().mapToInt(m -> m.getComments().size()).sum();
                                    int p2Sum = p2.getMembers().stream().mapToInt(m -> m.getComments().size()).sum();

                                    return Integer.compare(p1Sum, p2Sum);

                                }).forEach(pFraction -> {
                                    Set<Comment> pComments = new HashSet<>(0);
                                    Set<Speech> pSpeeches = new HashSet<>(0);
                                    pFraction.getMembers().stream().forEach(m -> {
                                        pComments.addAll(m.getComments());
                                        pSpeeches.addAll(m.getSpeeches());
                                    });

                                    System.out.println(pFraction + "\t (" + pComments.size() + ")\t AVG:\t" + ((float) pComments.size() / (float) pSpeeches.size()));

                                    Map<Fraction, Integer> commentMap = new HashMap<>(0);

                                    pComments.stream().forEach(c -> {

                                        this.getFactory().getFractions().forEach(f -> {

                                            if (c.getContent().toLowerCase().contains(f.getName().toLowerCase())) {

                                                if (commentMap.containsKey(f)) {
                                                    commentMap.put(f, commentMap.get(f) + 1);
                                                } else {
                                                    commentMap.put(f, 1);
                                                }

                                            }

                                        });

                                    });

                                    commentMap.entrySet().stream().sorted((f1, f2) -> {
                                        return f1.getValue().compareTo(f2.getValue()) * -1;
                                    }).forEach(fS -> {
                                        System.out.println("\t\t" + fS.getKey() + "\t (" + fS.getValue() + ")\t AVG:\t" + ((float) fS.getValue() / (float) pComments.size()));
                                    });

                                });


                                break;

                            case "speaker":

                                this.getFactory().getSpeakers().stream().sorted((s1, s2) -> {
                                    return Integer.compare(s1.getComments().size(), s2.getComments().size()) * -1;
                                }).forEach(s -> {

                                    System.out.println("=========================================================");
                                    System.out.println(s + "\t Comments:" + s.getComments().size());

                                    Map<Fraction, Integer> commentMap = new HashMap<>(0);

                                    s.getComments().forEach(c -> {

                                        this.getFactory().getFractions().forEach(f -> {

                                            if (c.getContent().toLowerCase().contains(f.getName().toLowerCase())) {

                                                if (commentMap.containsKey(f)) {
                                                    commentMap.put(f, commentMap.get(f) + 1);
                                                } else {
                                                    commentMap.put(f, 1);
                                                }

                                            }

                                        });

                                    });

                                    commentMap.entrySet().stream().sorted((f1, f2) -> {
                                        return f1.getValue().compareTo(f2.getValue()) * -1;
                                    }).forEach(fS -> {
                                        System.out.println("\t\t" + fS.getKey() + "\t (" + fS.getValue() + ")");
                                    });

                                });


                                break;

                            default:

                                throw new InputException("Unknown cmd " + cValue);
                        }


                        break;

                    default:

                        throw new InputException("Unknown cmd " + sCmd);

                }
            } catch (Exception ie) {
                System.err.println(ie.getMessage());
            }
        }


    }

    /**
     * Menü only for Exercise 2 - No Bonus-Implementation was performed
     * @deprecated
     */
    public void startMenuUeb2(){

        String sCmd = "";

        // Scanner method to query console inputs.
        Scanner scanner = new Scanner(System.in);

        Date fromDate = null;
        Date toDate = null;

        SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy");

        // Query the individual input methods
        while (!sCmd.equalsIgnoreCase("exit")) {

            try {
                System.out.println(getMenu());
                System.out.println("========================");
                System.out.println("Please make a selection:");
                System.out.println("\t speakers");
                System.out.println("\t search");
                System.out.println("\t avg");
                System.out.println("\t duration");
                System.out.println("\t filter");
                System.out.println("\t exit");

                sCmd = scanner.nextLine();

                switch (sCmd){

                    case "filter":
                    {
                        String sFromDate = "";
                        String sToDate = "";

                        System.out.println("\t from Date?");
                        sFromDate = scanner.nextLine();

                        System.out.println("\t to Date?");
                        sToDate = scanner.nextLine();

                        fromDate = sFromDate.length()>0 ? sdf.parse(sFromDate) : null;
                        toDate = sToDate.length()>0 ? sdf.parse(sToDate) :  null;

                        System.out.println("Filter set!");
                    }
                    break;

                    case "speakers":
                    {

                        String sParty = "";
                        String sFraction = "";

                        System.out.println("\t party?");
                        sParty = scanner.nextLine();

                        System.out.println("\t fraction?");
                        sFraction = scanner.nextLine();

                        Party pParty = sParty.length()>0 ? getFactory().getParty(sParty) : null;
                        Fraction pFraction = sFraction.length()>0 ? getFactory().getFraction(sFraction) :  null;


                        pFactory.getSpeakerBySpeeches(fromDate, toDate, pParty, pFraction).stream().forEach(s->{
                            Speaker pSpeaker = new Speaker_MongoDB_Impl(pFactory, s.get("speaker", org.bson.Document.class));
                            System.out.println(s.getInteger("count")+"\t"+pSpeaker.toString());
                        });


                    }

                    break;

                    case "search":
                    {

                        System.out.println("\t Insert search term(s):");

                        String sInput = scanner.nextLine();

                        System.out.println("\t limit?");

                        int iLimit = -1;

                        try {
                            iLimit = Integer.valueOf(scanner.nextLine());
                        }
                        catch (Exception e){
                            System.out.println(e.getMessage());
                        }

                        List<Speech> resultSet = pFactory.fullTextSearch(sInput);
                        System.out.println("Results: "+resultSet.size());

                        System.out.println("\t more? (y/n):");

                        sInput = scanner.nextLine();

                        if(sInput.equalsIgnoreCase("y")) {

                            resultSet.stream().limit(iLimit).forEach(s -> {
                                System.out.println(s.getSpeaker());
                                System.out.println(s.getID());
                                System.out.println(s.getText());
                                System.out.println("=======================================");
                            });
                        }

                    }
                    break;

                    case "avg":
                    {

                        String sParty = "";
                        String sFraction = "";

                        System.out.println("\t party?");
                        sParty = scanner.nextLine();

                        System.out.println("\t fraction?");
                        sFraction = scanner.nextLine();

                        Party pParty = sParty.length()>0 ? getFactory().getParty(sParty) : null;
                        Fraction pFraction = sFraction.length()>0 ? getFactory().getFraction(sFraction) :  null;

                        pFactory.getAvgSpeeches(fromDate, toDate, pParty, pFraction).stream().forEach(s->{
                            List<String> sList = s.getList("_id", String.class);
                            if(sList.size()>0){
                                System.out.println("NONE"+"\t"+s.getDouble("avg"));
                            }
                            else{
                                System.out.println(sList.get(0)+"\t"+s.getDouble("avg"));
                            }

                        });

                    }
                    break;

                    case "duration":

                        pFactory.listByDate(true);

                    break;
                }

            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    /**
     * Menü only for Exercise 3 - No Bonus-Implementation was performed
     */
    public void startMenuUeb3(){

        String sCmd = "";

        // Scanner method to query console inputs.
        Scanner scanner = new Scanner(System.in);


        // Query the individual input methods
        while (!sCmd.equalsIgnoreCase("exit")) {

            /*
             *   Since the filter function is analogous to exercise 2, I have skipped it here, since the functionality operates using the same filters as in exercise 2.
             */
            try {
                System.out.println("========================");
                System.out.println("Please make a selection:");
                System.out.println("\t nlp");
                System.out.println("\t lemmata");
                // this is not part of the exercise. I implemented it, to show a differnz between the "lemmata" query
                System.out.println("\t lemmata_comment");
                System.out.println("\t NE");
                System.out.println("\t sentiment");
                System.out.println("\t ddc");
                System.out.println("\t comments");
                System.out.println("\t exit");

                sCmd = scanner.nextLine();

                switch (sCmd){

                    case "nlp":
                    {
                        String sInput = "";
                        System.out.println("\t comments");
                        System.out.println("\t speeches");
                        sInput = scanner.nextLine();

                        switch (sInput){

                            case "comments":
                                NLPHelper.processNLP(this.getFactory(), "comment");
                            break;

                            case "speeches":
                                NLPHelper.processNLP(this.getFactory(), "speech");
                            break;

                        }

                    }
                    break;

                    case "lemmata":
                    {

                        String sInput = "";
                        System.out.println("\t noun");
                        System.out.println("\t verb");
                        sInput = scanner.nextLine();

                        List<Document> result = new ArrayList<>(0);

                        switch (sInput){

                            case "noun":
                                result = this.getFactory().getSpeechLemmaByPos("POS_NOUN");
                            break;

                            case "verb":
                                result = this.getFactory().getSpeechLemmaByPos("POS_VERB");
                            break;

                        }
                        result.stream().forEach(d->{
                            System.out.println(d.get("_id")+"\t ("+d.get("count")+")");
                        });
                    }

                    case "lemmata_comment":
                    {

                        String sInput = "";
                        System.out.println("\t noun");
                        System.out.println("\t verb");
                        sInput = scanner.nextLine();

                        List<Document> result = new ArrayList<>(0);
                        switch (sInput){

                            case "noun":
                                result = this.getFactory().getCommentLemmaByPos("POS_NOUN");
                            break;

                            case "verb":
                                result =this.getFactory().getCommentLemmaByPos("POS_VERB");
                            break;

                        }
                        result.stream().forEach(d->{
                            System.out.println(d.get("_id")+"\t ("+d.get("count")+")");
                        });

                    }

                    break;

                    case "NE":
                    {
                        Map<String, Map<String, Integer>> mapResult = this.getFactory().getNamedEntities();

                        mapResult.entrySet().forEach(e->{
                            System.out.println(e.getKey());
                            System.out.println("======================");
                            e.getValue().entrySet().forEach(e1->{
                                System.out.println("\t"+e1.getKey()+"\t"+e1.getValue());
                            });
                        });
                        break;
                    }


                    case "sentiment":{

                        pFactory.getSentimentCorpus("speech").forEach(d->{
                            System.out.println(d.get("_id")+"\t"+d.get("count"));
                        });

                        break;
                    }

                    case "ddc": {

                        String sInput = "";
                        System.out.println("\t top?");

                        sInput = scanner.nextLine();

                        pFactory.getDDC(Integer.valueOf(sInput)).forEach(d->{
                            if(d.get("_id")!=null) {
                                System.out.println(FileReader.getDDC(d.getString("_id")) + "\t (" + d.getInteger("count")+")");
                            }
                        });
                        break;
                    }

                    case "comments":
                    {

                        System.out.println("\t limit: ");

                        String sInput = scanner.nextLine();

                        int iLimit = Integer.valueOf(sInput);

                        System.out.println("Positive");
                        System.out.println("===============");
                        this.getFactory().getSentimentComment(BsonDocument.parse("{ \"sentiment\": { $lte: 1}}, { \"sentiment\":  { $gt: 0 } }"), iLimit).forEach(c->{
                            System.out.println(c.get("_id")+"\t ("+c.get("count")+")");
                        });

                        System.out.println("Neutral");
                        System.out.println("===============");
                        this.getFactory().getSentimentComment(BsonDocument.parse("{ \"sentiment\": { $eq: 0} }"), iLimit).forEach(c->{
                            System.out.println(c.get("_id")+"\t ("+c.get("count")+")");
                        });

                        System.out.println("Negative");
                        System.out.println("===============");
                        this.getFactory().getSentimentComment(BsonDocument.parse("{ \"sentiment\": { $lt: 0} }"), iLimit).forEach(c->{
                            System.out.println(c.get("_id")+"\t ("+c.get("count")+")");
                        });


                        break;
                    }


                }

            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    /**
     * Get Parliament Factory
     * @return
     */
    public ParliamentFactory getFactory() {
        return this.pFactory;
    }

    /**
     * Constructor
     */
    public ParAnSy() {
        this.pFactory = new ParliamentFactory_Impl();
    }

    /**
     * Import parliamentary files
     * @param sPath
     */
    public Set<PlenaryProtocol> startImportFromFile(String sPath) {

        Set<PlenaryProtocol> rSet = new HashSet<>(0);

        System.out.println("Start loading Plenary Protocols...");

        FileReader.getFiles(sPath, "xml").stream().sorted().forEach(f -> {

            int iIndex = Integer.valueOf(f.getName().replace(".xml", ""));
            System.out.println("Checking "+f.getName());

            if(pFactory.getProtocol(iIndex)==null || true) {

            System.out.println("Insert "+f.getName());

                PlenaryProtocol pProtocol = new PlenaryProtocol_File_Impl(this.pFactory, f);
//                getFactory().addProtocol(pProtocol);
                rSet.add(pProtocol);
                pProtocol.getAgendaItems().forEach(item -> {
                    item.getSpeeches().forEach(speech -> {

                        List<String> ids = new ArrayList<>(0);

                        speech.getComments().forEach(c -> {
                            try {
                                getFactory().addComment(c);
                            } catch (UIMAException e) {
                                throw new RuntimeException(e);
                            }
                            ids.add(c.getID());
                        });

                        try {
                            this.getFactory().addSpeech(speech);
                        } catch (UIMAException e) {
                            throw new RuntimeException(e);
                        }

//                    Speech_MongoDB_Impl newSpeech = (Speech_MongoDB_Impl) this.getFactory().getSpeech(speech.getID());
//
//                    if(!newSpeech.getDocument().containsKey("comments")) {
//                        newSpeech.getDocument().put("comments", ids);
//                        newSpeech.update();
//                    }

                    });
                });
                System.out.println("Finish " + f.getName());
            }
        });



        System.out.println(getFactory().getProtocols().size() + " Plenary Protocols loaded!");

        return rSet;
    }

    /**
     * Method for reading parliament master data
     * This was not part of exercise 1 but was implemented for completeness.
     * @param sPath
     * @throws ParserConfigurationException
     * @throws IOException
     * @throws SAXException
     */
    public void startMDBStammdaten(String sPath) throws ParserConfigurationException, IOException, SAXException {

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

        // parse XML file
        DocumentBuilder db = dbf.newDocumentBuilder();

        org.w3c.dom.Document pDocument = db.parse(new File(sPath));

        NodeList nl = pDocument.getElementsByTagName("MDB");

        for (int a = 0; a < nl.getLength(); a++) {

            Node n = nl.item(a);

            Node nID = XMLHelper.getSingleNodesFromXML(n, "ID");

            Node finalNID = nID;
            List<Speaker> sList = pFactory.getSpeakers().stream().filter(s -> s.getID().equalsIgnoreCase(finalNID.getTextContent())).collect(Collectors.toList());
            if (sList.size() == 1) {
                Node pPartei = XMLHelper.getSingleNodesFromXML(n, "PARTEI_KURZ");
                Node pAKAD_TITEL = XMLHelper.getSingleNodesFromXML(n, "AKAD_TITEL");
                Node pGEBURTSORT = XMLHelper.getSingleNodesFromXML(n, "GEBURTSORT");
                Node pGESCHLECHT = XMLHelper.getSingleNodesFromXML(n, "GESCHLECHT");
                Node pRELIGION = XMLHelper.getSingleNodesFromXML(n, "RELIGION");
                Node pBERUF = XMLHelper.getSingleNodesFromXML(n, "BERUF");
                Node pGEBURTSDATUM = XMLHelper.getSingleNodesFromXML(n, "GEBURTSDATUM");
                Node pSTERBEDATUM = XMLHelper.getSingleNodesFromXML(n, "STERBEDATUM");
                Node pFAMILIENSTAND = XMLHelper.getSingleNodesFromXML(n, "FAMILIENSTAND");
                Speaker pSpeaker = sList.get(0);

                if(pPartei!=null) {
                    Party pParty = pFactory.getParty(pPartei.getTextContent());
                    pSpeaker.setParty(pParty);
                    pFactory.updateSpeaker(pSpeaker);
                }

                if(pAKAD_TITEL!=null) {
                    if(pAKAD_TITEL.getTextContent().length()>0){
                        pSpeaker.setAkademischerTitel(pAKAD_TITEL.getTextContent());
                    }
                }
                if(pGEBURTSORT!=null) {
                    if(pGEBURTSORT.getTextContent().length()>0){
                        pSpeaker.setGeburtsort(pGEBURTSORT.getTextContent());
                    }
                }
                if(pGESCHLECHT!=null) {
                    if(pGESCHLECHT.getTextContent().length()>0){
                        pSpeaker.setGeschlecht(pGESCHLECHT.getTextContent());
                    }
                }
                if(pRELIGION!=null) {
                    if(pRELIGION.getTextContent().length()>0){
                        pSpeaker.setReligion(pRELIGION.getTextContent());
                    }
                }
                if(pBERUF!=null) {
                    if(pBERUF.getTextContent().length()>0){
                        pSpeaker.setBeruf(pBERUF.getTextContent());
                    }
                }
                if(pFAMILIENSTAND!=null) {
                    if(pFAMILIENSTAND.getTextContent().length()>0){
                        pSpeaker.setFamilienstand(pFAMILIENSTAND.getTextContent());
                    }
                }
                if(pGEBURTSDATUM!=null) {
                    if(pGEBURTSDATUM.getTextContent().length()>0){
                        try {
                            pSpeaker.setGeburtsdatum(StringHelper.DATEOFRMAT.parse(pGEBURTSDATUM.getTextContent()));
                        } catch (java.text.ParseException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                if(pSTERBEDATUM!=null) {
                    if(pSTERBEDATUM.getTextContent().length()>0){
                        try {
                            pSpeaker.setSterbedatum(StringHelper.DATEOFRMAT.parse(pSTERBEDATUM.getTextContent()));
                        } catch (java.text.ParseException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

                pFactory.updateSpeaker(pSpeaker);

            }
//            else{
//                Speaker pSpeaker = new Speaker_File_Impl(this.getFactory());
//
//                Node pName = XMLHelper.getSingleNodesFromXML(n, "NACHNAME");
//                Node pVorname = XMLHelper.getSingleNodesFromXML(n, "VORNAME");
//                Node pTitel = XMLHelper.getSingleNodesFromXML(n, "ANREDE_TITEL");
//                Node pPartei = XMLHelper.getSingleNodesFromXML(n, "PARTEI_KURZ");
//
//                pSpeaker.setID(nID.getTextContent());
//                pSpeaker.setFirstName(pVorname.getTextContent());
//                pSpeaker.setName(pName.getTextContent());
//                pSpeaker.setTitle(pTitel.getTextContent());
//                Party pParty = pFactory.getParty(pPartei.getTextContent());
//                pSpeaker.setParty(pParty);
//
//                this.getFactory().addSpeaker(pSpeaker);
//
//
//            }

        }


    }

    /**
     * Return software name
     * @return
     */
    public static String getWelcome() {

        String rString ="  _____                         _____       \n" +
                " |  __ \\           /\\          / ____|      \n" +
                " | |__) |_ _ _ __ /  \\   _ __ | (___  _   _ \n" +
                " |  ___/ _` | '__/ /\\ \\ | '_ \\ \\___ \\| | | |\n" +
                " | |  | (_| | | / ____ \\| | | |____) | |_| |\n" +
                " |_|   \\__,_|_|/_/    \\_\\_| |_|_____/ \\__, |\n" +
                "                                       __/ |\n" +
                "                                      |___/ ";
        return rString;

    }

    /**
     * Return exit message
     * @return
     */
    private String getExit() {

        String rString = "  ______           _          __   _      _            \n" +
                " |  ____|         | |        / _| | |    (_)           \n" +
                " | |__   _ __   __| |   ___ | |_  | |     _ _ __   ___ \n" +
                " |  __| | '_ \\ / _` |  / _ \\|  _| | |    | | '_ \\ / _ \\\n" +
                " | |____| | | | (_| | | (_) | |   | |____| | | | |  __/\n" +
                " |______|_| |_|\\__,_|  \\___/|_|   |______|_|_| |_|\\___|\n" +
                "                                                       \n" +
                "                                                       \n";

        return rString;

    }

    /**
     * Return menue message
     * @return
     */
    private String getMenu() {

        String rString = "___  ___                 \n" +
                "|  \\/  |                 \n" +
                "| .  . | ___ _ __  _   _ \n" +
                "| |\\/| |/ _ | '_ \\| | | |\n" +
                "| |  | |  __| | | | |_| |\n" +
                "\\_|  |_/\\___|_| |_|\\__,_|\n";

        return rString;

    }


    /**
     * Method for updating speeches in a separate database.
     * @throws IOException
     */
    @Test
    public void updateSpeeches() throws IOException {
        String pTarget = ParAnSy.class.getClassLoader().getResource("dbconnection_rw_target.txt").getPath();
        MongoDBConfig dbConfigTarget = new MongoDBConfig(pTarget);
        MongoDBConnectionHandler targetDB = new MongoDBConnectionHandler(dbConfigTarget);
        ParliamentFactory targetFactory = new ParliamentFactory_Impl(targetDB);

        MongoCollection pCollection = targetFactory.getMongoConnection().getCollection("speech");

        int iCount=0;
        int iSkip=500;

        boolean isFinish = true;

        while(isFinish){
            try {
                MongoCursor<Document> pDocuments = pCollection.find(BsonDocument.parse("{\"lemma.0.pos\": {$exists:0}}")).limit(iSkip).skip(iSkip*iCount).sort(BsonDocument.parse("{_id : 1}")).cursor();

                isFinish = pDocuments.hasNext();

                pDocuments.forEachRemaining(d->{
                    try {
                        List<Document> lemmas = d.getList("lemma", Document.class);
                        List<Document> pos = d.getList("pos", Document.class);

                        for (int a = 0; a < pos.size(); a++) {
                            lemmas.get(a).put("pos", pos.get(a).getString("type"));
                        }
                        d.put("lemma", lemmas);

                        BasicDBObject whereQuery = new BasicDBObject();
                        whereQuery.put("_id", d.getString("_id"));
                        pCollection.replaceOne(whereQuery, d);
                    }
                    catch (Exception e){
                        e.printStackTrace();

                        Speech pSpeech = new Speech_MongoDB_Impl(targetFactory, d);
                        targetFactory.updateSpeech(pSpeech);

                    }
                });

            }
            catch (Exception e){
                e.printStackTrace();
                System.out.println(iCount);
            }
            iCount++;
        }



    }

    /**
     * Method for updating the protocols in a separate database.
     * @throws IOException
     */
    @Test
    public void updateProtocols() throws IOException {

        String pTarget = ParAnSy.class.getClassLoader().getResource("dbconnection_rw_target.txt").getPath();
        MongoDBConfig dbConfigTarget = new MongoDBConfig(pTarget);
        MongoDBConnectionHandler targetDB = new MongoDBConnectionHandler(dbConfigTarget);
        ParliamentFactory targetFactory = new ParliamentFactory_Impl(targetDB);

        MongoCollection pCollection = targetFactory.getMongoConnection().getCollection("comment");
        int iCount=0;
        int iSkip=500;
        boolean isFinish = true;

        while(isFinish){
            try {
                MongoCursor<Document> pDocuments = pCollection.find(BsonDocument.parse("{\"lemma\": {$exists:0}}")).skip(iSkip*iCount).limit(iSkip).sort(BsonDocument.parse("{_id : 1}")).cursor();

                isFinish = pDocuments.hasNext();

                pDocuments.forEachRemaining(d->{
                    Comment pComment = new Comment_MongoDB_Impl(targetFactory, d);
                    try {
                        targetFactory.updateComment(pComment);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
            catch (Exception e){
                e.printStackTrace();
                System.out.println(iCount);
            }
            iCount++;
        }


    }

    /**
     * Test function for manual transfer of protocols from a source to a target database.
     * @throws IOException
     */
    @Test
    public void transportProtocols() throws IOException {

        String pSource = ParAnSy.class.getClassLoader().getResource("dbconnection_rw_source.txt").getPath();
        String pTarget = ParAnSy.class.getClassLoader().getResource("dbconnection_rw_target.txt").getPath();


        MongoDBConfig dbConfigSource = new MongoDBConfig(pSource);
        MongoDBConfig dbConfigTarget = new MongoDBConfig(pTarget);

        MongoDBConnectionHandler targetDB = new MongoDBConnectionHandler(dbConfigTarget);
        MongoDBConnectionHandler sourceDB = new MongoDBConnectionHandler(dbConfigSource);

        ParliamentFactory targetFactory = new ParliamentFactory_Impl(targetDB);
        ParliamentFactory sourceFactory = new ParliamentFactory_Impl(sourceDB);

//        sourceFactory.getSpeakers().stream().forEach(s->{
//            targetFactory.addSpeaker(s);
//        });

        Set<Comment> newComments = new HashSet<>(0);

        int iCount=0;
        int iSkip=500;

        boolean isFinish = true;

        MongoCollection pCollection = targetFactory.getMongoConnection().getCollection("comment");

        while(isFinish){
            try {
                MongoCursor<Document> pDocuments = pCollection.find(BsonDocument.parse("{\"pos.0.type\": {$exists: 0}}")).skip(iSkip*iCount).limit(iSkip).sort(BsonDocument.parse("{_id : 1}")).cursor();

                isFinish = pDocuments.hasNext();

                pDocuments.forEachRemaining(d->{
                    Comment pComment = new Comment_MongoDB_Impl(targetFactory, d);
                    try {
                        targetFactory.updateComment(pComment);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
            catch (Exception e){
                e.printStackTrace();
                System.out.println(iCount);
            }
            iCount++;
        }

        pCollection = targetFactory.getMongoConnection().getCollection("speech");

        iCount=0;
        iSkip=500;

        isFinish = true;

        while(isFinish){
            try {
                MongoCursor<Document> pDocuments = pCollection.find(BsonDocument.parse("{\"pos.0.type\": {$exists: 0}}")).skip(iSkip*iCount).limit(iSkip).sort(BsonDocument.parse("{_id : 1}")).cursor();

                isFinish = pDocuments.hasNext();

                pDocuments.forEachRemaining(d->{
                    Speech pSpeech = new Speech_MongoDB_Impl(targetFactory, d);
                    targetFactory.updateSpeech(pSpeech);
                });
            }
            catch (Exception e){
                System.out.println(iCount);
            }
            iCount++;
        }



    }

}
