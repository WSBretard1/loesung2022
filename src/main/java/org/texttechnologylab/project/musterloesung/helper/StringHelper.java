package org.texttechnologylab.project.musterloesung.helper;

import java.text.SimpleDateFormat;

/**
 * @author Giuseppe Abrami
 * @date 10.11.2022
 */
public class StringHelper {

    public static final SimpleDateFormat DATEOFRMAT = new SimpleDateFormat("dd.MM.YYYY");

    public static String[] FIRSTNAME = {"Dr. h. c.", "Dr."};
    public static String[] REGEXCLEAN = {"\\(.*\\)"};

    public static String replaceList(String sInput, String[] replaceList){

        String rString = sInput;

        rString = rString.replaceAll("[\\p{Cc}\\p{Cf}\\p{Co}\\p{Cn}\\p{Z}]", " ");

        for (String s : replaceList) {
            rString = rString.replace(s, "");
        }
        rString = clean(rString);
        return rString;

    }

    public static String clean(String sInput){

        return sInput.trim();

    }

}
