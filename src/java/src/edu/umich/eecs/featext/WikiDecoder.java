package edu.umich.eecs.featext;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.lang3.StringUtils;

import edu.umich.eecs.featext.DataSources.WikiPage;

public class WikiDecoder {
	public static WikiPage decode(String input) {
		String[] lineParts = StringUtils.split(input, "\t");
		int pageId = new Integer(lineParts[0]);
		String encodedContent = lineParts[1];	  
		byte[] decodedBytes = DatatypeConverter.parseBase64Binary(encodedContent);
		String content = WikiDecoder.decompressToString(decodedBytes);
		String[] lines = StringUtils.split(content, "\n");
		String title = lines[0];
		
		content = StringUtils.join(Arrays.asList(lines).subList(1, lines.length), "\n");

		return new WikiPage(pageId, title, content);
	}
	
	public static byte[] decompress(byte[] bytesToDecompress) {
        Inflater inflater = new Inflater();

        int numberOfBytesToDecompress = bytesToDecompress.length;
        inflater.setInput (bytesToDecompress, 0, numberOfBytesToDecompress);

        int compressionFactorMaxLikely = 3;

        int bufferSizeInBytes = numberOfBytesToDecompress * compressionFactorMaxLikely;

        byte[] bytesDecompressed = new byte[bufferSizeInBytes];

        byte[] returnValues = null;

        try         {
            int numberOfBytesAfterDecompression = inflater.inflate(bytesDecompressed);
            returnValues = new byte[numberOfBytesAfterDecompression];

            System.arraycopy(bytesDecompressed, 0, returnValues, 0, numberOfBytesAfterDecompression);           
        }
        catch (DataFormatException dfe) {
            dfe.printStackTrace();
        }

        inflater.end();
        return returnValues;
    }

    public static String decompressToString(byte[] bytesToDecompress) {    
        byte[] bytesDecompressed = WikiDecoder.decompress(bytesToDecompress);
        String returnValue = null;

        try {
            returnValue = new String(bytesDecompressed, 0, bytesDecompressed.length, "UTF-8");    
        }
        catch (UnsupportedEncodingException uee) {
            uee.printStackTrace();
        }

        return returnValue;
    }
}
