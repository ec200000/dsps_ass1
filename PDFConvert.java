import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.List;

import org.apache.pdfbox.pdfparser.*;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.PDFTextStripperByArea;
import org.apache.pdfbox.util.*;

import javax.imageio.ImageIO;

public class PDFConvert {

    public static void parseImage(String path) throws IOException {
        File f = new File(path);
        //("C:\\Users\\Lenovo\\IdeaProjects\\dsps_ass1\\src\\main\\java\\Assignment 1.pdf");
        PDDocument pd = PDDocument.load(f);
        PDFRenderer pr = new PDFRenderer (pd);
        BufferedImage bi = pr.renderImageWithDPI (0, 300);
        ImageIO.write(bi, "PNG", new File ("ass1.png"));
        pd.close();
    }

    public static void parseHTML(String path) throws IOException {
        File f = new File(path);
        //("C:\\Users\\Lenovo\\IdeaProjects\\dsps_ass1\\src\\main\\java\\Assignment 1.pdf");
        PDDocument pd = PDDocument.load(f);
        PDFText2HTML pdfText2HTML = new PDFText2HTML("UTF-8");
        pdfText2HTML.setStartPage(1);
        pdfText2HTML.setEndPage(1);
        FileWriter fWriter = new FileWriter("ass1.html");
        BufferedWriter bufferedWriter = new BufferedWriter(fWriter);
        pdfText2HTML.writeText(pd, bufferedWriter);
        bufferedWriter.close();
        pd.close();
    }

    public static void parseText(String path) throws IOException {
        File f = new File(path);
        //"C:\\Users\\Lenovo\\IdeaProjects\\dsps_ass1\\src\\main\\java\\Assignment 1.pdf");
        PDDocument pd = PDDocument.load(f);
        PDFTextStripperByArea stripper = new PDFTextStripperByArea();
        stripper.setSortByPosition( true );
        Rectangle rect = new Rectangle( 0, 0, 2480, 3508 );
        stripper.addRegion( "class1", rect );
        PDPage firstPage = pd.getPage(0);
        stripper.extractRegions( firstPage );
        //System.out.println( "Text in the area:" + rect );
        String s = stripper.getTextForRegion( "class1" );
        try (PrintWriter out = new PrintWriter("ass1.txt")) {
            out.println(s);
        }
        pd.close();
    }

    public static void main(String[] args) throws IOException {
        String path = "C:\\Users\\Lenovo\\IdeaProjects\\dsps_ass1\\src\\main\\java\\Assignment 1.pdf";
        parseImage(path);
    }
}
