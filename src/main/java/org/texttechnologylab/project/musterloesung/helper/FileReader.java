package org.texttechnologylab.project.musterloesung.helper;

import org.texttechnologylab.utilities.helper.FileUtils;
import org.texttechnologylab.utilities.helper.TempFileHandler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Auxiliary class for reading files
 * @author Giuseppe Abrami
 */
public class FileReader {

    /**
     * DDC-Mapping
     */
    private static Map<String, String> ddcMap = new HashMap<>();

    /**
     * Get all Files, recursive, based on sPath with given suffixes
     * @param sPath
     * @param sSuffix
     * @return
     */
    public static Set<File> getFiles(String sPath, String sSuffix){

        Set<File> fSet = new HashSet<>(0);

            File sDir = new File(sPath);

            if(sDir.isDirectory()){
                for (File f : sDir.listFiles()) {
                    if(f.getName().endsWith(sSuffix)){
                        fSet.add(f);
                    }

                }
            }
            else if(sDir.isFile()){
                if(sDir.getName().endsWith(sSuffix)) {
                    fSet.add(sDir);
                }
            }

        return fSet;

    }

    /**
     * Method for Unzipping Zip-Archives
     * Always nice to have!
     * @param pFile
     * @return
     * @throws IOException
     */
    public static Set<File> unzipFile(File pFile) throws IOException {

        ZipInputStream zis = new ZipInputStream(new FileInputStream(pFile));
        ZipEntry zipEntry = zis.getNextEntry();
        byte[] buffer = new byte[1024];

        Set<File> rSet = new HashSet<>(0);

        while (zipEntry != null){
            File tFile = TempFileHandler.getTempFileName(zipEntry.getName());
            tFile.deleteOnExit();

            FileOutputStream fos = new FileOutputStream(tFile);
            int len;
            while ((len = zis.read(buffer)) > 0) {
                fos.write(buffer, 0, len);
            }
            fos.close();
            rSet.add(tFile);

            zipEntry = zis.getNextEntry();
        }
        zis.closeEntry();
        zis.close();

        return rSet;
    }

    /**
     * Init DDC-Mapping
     * @param pFile
     * @throws IOException
     */
    public static void initDDC(File pFile) throws IOException {

        String sContent = FileUtils.getContentFromFile(pFile);

        for (String s1 : sContent.split("\n")) {
            if(s1.length()>0){
                String sKey = s1.substring(0, 3);
                String sValue = s1.substring(4);
                ddcMap.put(sKey, sValue);
            }
        }
    }

    /**
     * Get DDC-Value
     * @param sKey
     * @return
     */
    public static String getDDC(String sKey){
        String tKey = sKey.replace("__label_ddc__", "");
        return ddcMap.get(tKey);
    }

}
