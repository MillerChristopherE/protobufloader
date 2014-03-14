/*
 * Copyright 2001-2004 The Apache Software Foundation.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */ 

// From commons, updated for URL-safe base64 and omitting trailing padding.

/*
package org.apache.commons.codec.binary;

import org.apache.commons.codec.BinaryDecoder;
import org.apache.commons.codec.BinaryEncoder;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.EncoderException;
*/

package org.protobufloader.util;

/**
 * Provides Base64 encoding and decoding as defined by RFC 2045.
 * 
 * <p>This class implements section <cite>6.8. Base64 Content-Transfer-Encoding</cite> 
 * from RFC 2045 <cite>Multipurpose Internet Mail Extensions (MIME) Part One: 
 * Format of Internet Message Bodies</cite> by Freed and Borenstein.</p> 
 *
 * @see <a href="http://www.ietf.org/rfc/rfc2045.txt">RFC 2045</a>
 * @author Apache Software Foundation
 * @since 1.0-dev
 * @version $Id: Base64.java,v 1.20 2004/05/24 00:21:24 ggregory Exp $
 */
public class Base64 { // implements BinaryEncoder, BinaryDecoder {
    public static class DecoderException extends Exception { public DecoderException(String msg) { super(msg); } }
    public static class EncoderException extends Exception { public EncoderException(String msg) { super(msg); } }

    /**
     * Chunk size per RFC 2045 section 6.8.
     * 
     * <p>The {@value} character limit does not count the trailing CRLF, but counts 
     * all other characters, including any equal signs.</p>
     * 
     * @see <a href="http://www.ietf.org/rfc/rfc2045.txt">RFC 2045 section 6.8</a>
     */
    static final int CHUNK_SIZE = 76;

    /**
     * Chunk separator per RFC 2045 section 2.1.
     * 
     * @see <a href="http://www.ietf.org/rfc/rfc2045.txt">RFC 2045 section 2.1</a>
     */
    static final byte[] CHUNK_SEPARATOR = "\r\n".getBytes();

    /**
     * The base length.
     */
    static final int BASELENGTH = 255;

    /**
     * Lookup length.
     */
    static final int LOOKUPLENGTH = 64;

    /**
     * Used to calculate the number of bits in a byte.
     */
    static final int EIGHTBIT = 8;

    /**
     * Used when encoding something which has fewer than 24 bits.
     */
    static final int SIXTEENBIT = 16;

    /**
     * Used to determine how many bits data contains.
     */
    static final int TWENTYFOURBITGROUP = 24;

    /**
     * Used to get the number of Quadruples.
     */
    static final int FOURBYTE = 4;

    /**
     * Used to test the sign of a byte.
     */
    static final int SIGN = -128;
    
    /**
     * Byte used to pad output.
     */
    static final byte PAD = (byte) '=';

    // Create arrays to hold the base64 characters and a 
    // lookup for base64 chars
    private static byte[] base64Alphabet = new byte[BASELENGTH];
    private static byte[] lookUpBase64Alphabet = new byte[LOOKUPLENGTH];

    // Populating the lookup and character arrays
    static {
        for (int i = 0; i < BASELENGTH; i++) {
            base64Alphabet[i] = (byte) -1;
        }
        for (int i = 'Z'; i >= 'A'; i--) {
            base64Alphabet[i] = (byte) (i - 'A');
        }
        for (int i = 'z'; i >= 'a'; i--) {
            base64Alphabet[i] = (byte) (i - 'a' + 26);
        }
        for (int i = '9'; i >= '0'; i--) {
            base64Alphabet[i] = (byte) (i - '0' + 52);
        }

        base64Alphabet['+'] = 62;
        base64Alphabet['/'] = 63;

        // URL-safe decoding:
        base64Alphabet['-'] = 62;
        base64Alphabet['_'] = 63;

        for (int i = 0; i <= 25; i++) {
            lookUpBase64Alphabet[i] = (byte) ('A' + i);
        }

        for (int i = 26, j = 0; i <= 51; i++, j++) {
            lookUpBase64Alphabet[i] = (byte) ('a' + j);
        }

        for (int i = 52, j = 0; i <= 61; i++, j++) {
            lookUpBase64Alphabet[i] = (byte) ('0' + j);
        }

        /*if (isUrlSafe) {
            // For URL-safe encoding:
            lookUpBase64Alphabet[62] = (byte) '-';
            lookUpBase64Alphabet[63] = (byte) '_';
        } else*/ {
            lookUpBase64Alphabet[62] = (byte) '+';
            lookUpBase64Alphabet[63] = (byte) '/';
        }

    }

    private static boolean isBase64(byte octect) {
        if (octect == PAD) {
            return true;
        } else if (base64Alphabet[octect] == -1) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Tests a given byte array to see if it contains
     * only valid characters within the Base64 alphabet.
     *
     * @param arrayOctect byte array to test
     * @return true if all bytes are valid characters in the Base64
     *         alphabet or if the byte array is empty; false, otherwise
     */
    public static boolean isArrayByteBase64(byte[] arrayOctect) {

        arrayOctect = discardWhitespace(arrayOctect);

        int length = arrayOctect.length;
        if (length == 0) {
            // shouldn't a 0 length array be valid base64 data?
            // return false;
            return true;
        }
        for (int i = 0; i < length; i++) {
            if (!isBase64(arrayOctect[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Encodes binary data using the base64 algorithm but
     * does not chunk the output.
     *
     * @param binaryData binary data to encode
     * @return Base64 characters
     */
    public static byte[] encodeBase64(byte[] binaryData) {
        return encodeBase64(binaryData, false);
    }

    /**
     * Encodes binary data using the base64 algorithm and chunks
     * the encoded output into 76 character blocks
     *
     * @param binaryData binary data to encode
     * @return Base64 characters chunked in 76 character blocks
     */
    public static byte[] encodeBase64Chunked(byte[] binaryData) {
        return encodeBase64(binaryData, true);
    }


    /**
     * Decodes an Object using the base64 algorithm.  This method
     * is provided in order to satisfy the requirements of the
     * Decoder interface, and will throw a DecoderException if the
     * supplied object is not of type byte[].
     *
     * @param pObject Object to decode
     * @return An object (of type byte[]) containing the 
     *         binary data which corresponds to the byte[] supplied.
     * @throws DecoderException if the parameter supplied is not
     *                          of type byte[]
     */
    public Object decode(Object pObject) throws DecoderException {
        if (!(pObject instanceof byte[])) {
            throw new DecoderException("Parameter supplied to Base64 decode is not a byte[]");
        }
        return decode((byte[]) pObject);
    }

    /**
     * Decodes a byte[] containing containing
     * characters in the Base64 alphabet.
     *
     * @param pArray A byte array containing Base64 character data
     * @return a byte array containing binary data
     */
    public byte[] decode(byte[] pArray) {
        return decodeBase64(pArray);
    }

    /**
     * Encodes binary data using the base64 algorithm, optionally
     * chunking the output into 76 character blocks.
     *
     * @param binaryData Array containing binary data to encode.
     * @param isChunked if isChunked is true this encoder will chunk
     *                  the base64 output into 76 character blocks
     * @return Base64-encoded data.
     */
    public static byte[] encodeBase64(byte[] binaryData, boolean isChunked) {
        int lengthDataBits = binaryData.length * EIGHTBIT;
        int fewerThan24bits = lengthDataBits % TWENTYFOURBITGROUP;
        int numberTriplets = lengthDataBits / TWENTYFOURBITGROUP;
        byte encodedData[] = null;
        int encodedDataLength = 0;
        int nbrChunks = 0;

        if (fewerThan24bits != 0) {
            //data not divisible by 24 bit
            encodedDataLength = (numberTriplets + 1) * 4;
        } else {
            // 16 or 8 bit
            encodedDataLength = numberTriplets * 4;
        }

        // If the output is to be "chunked" into 76 character sections, 
        // for compliance with RFC 2045 MIME, then it is important to 
        // allow for extra length to account for the separator(s)
        if (isChunked) {

            nbrChunks =
                (CHUNK_SEPARATOR.length == 0 ? 0 : (int) Math.ceil((float) encodedDataLength / CHUNK_SIZE));
            encodedDataLength += nbrChunks * CHUNK_SEPARATOR.length;
        }

        encodedData = new byte[encodedDataLength];

        byte k = 0, l = 0, b1 = 0, b2 = 0, b3 = 0;

        int encodedIndex = 0;
        int dataIndex = 0;
        int i = 0;
        int nextSeparatorIndex = CHUNK_SIZE;
        int chunksSoFar = 0;

        //log.debug("number of triplets = " + numberTriplets);
        for (i = 0; i < numberTriplets; i++) {
            dataIndex = i * 3;
            b1 = binaryData[dataIndex];
            b2 = binaryData[dataIndex + 1];
            b3 = binaryData[dataIndex + 2];

            //log.debug("b1= " + b1 +", b2= " + b2 + ", b3= " + b3);

            l = (byte) (b2 & 0x0f);
            k = (byte) (b1 & 0x03);

            byte val1 =
                ((b1 & SIGN) == 0) ? (byte) (b1 >> 2) : (byte) ((b1) >> 2 ^ 0xc0);
            byte val2 =
                ((b2 & SIGN) == 0) ? (byte) (b2 >> 4) : (byte) ((b2) >> 4 ^ 0xf0);
            byte val3 =
                ((b3 & SIGN) == 0) ? (byte) (b3 >> 6) : (byte) ((b3) >> 6 ^ 0xfc);

            encodedData[encodedIndex] = lookUpBase64Alphabet[val1];
            //log.debug( "val2 = " + val2 );
            //log.debug( "k4   = " + (k<<4) );
            //log.debug(  "vak  = " + (val2 | (k<<4)) );
            encodedData[encodedIndex + 1] =
                lookUpBase64Alphabet[val2 | (k << 4)];
            encodedData[encodedIndex + 2] =
                lookUpBase64Alphabet[(l << 2) | val3];
            encodedData[encodedIndex + 3] = lookUpBase64Alphabet[b3 & 0x3f];

            encodedIndex += 4;

            // If we are chunking, let's put a chunk separator down.
            if (isChunked) {
                // this assumes that CHUNK_SIZE % 4 == 0
                if (encodedIndex == nextSeparatorIndex) {
                    System.arraycopy(
                        CHUNK_SEPARATOR,
                        0,
                        encodedData,
                        encodedIndex,
                        CHUNK_SEPARATOR.length);
                    chunksSoFar++;
                    nextSeparatorIndex =
                        (CHUNK_SIZE * (chunksSoFar + 1)) + 
                        (chunksSoFar * CHUNK_SEPARATOR.length);
                    encodedIndex += CHUNK_SEPARATOR.length;
                }
            }
        }

        // form integral number of 6-bit groups
        dataIndex = i * 3;

        if (fewerThan24bits == EIGHTBIT) {
            b1 = binaryData[dataIndex];
            k = (byte) (b1 & 0x03);
            //log.debug("b1=" + b1);
            //log.debug("b1<<2 = " + (b1>>2) );
            byte val1 =
                ((b1 & SIGN) == 0) ? (byte) (b1 >> 2) : (byte) ((b1) >> 2 ^ 0xc0);
            encodedData[encodedIndex] = lookUpBase64Alphabet[val1];
            encodedData[encodedIndex + 1] = lookUpBase64Alphabet[k << 4];
            encodedData[encodedIndex + 2] = PAD;
            encodedData[encodedIndex + 3] = PAD;
        } else if (fewerThan24bits == SIXTEENBIT) {

            b1 = binaryData[dataIndex];
            b2 = binaryData[dataIndex + 1];
            l = (byte) (b2 & 0x0f);
            k = (byte) (b1 & 0x03);

            byte val1 =
                ((b1 & SIGN) == 0) ? (byte) (b1 >> 2) : (byte) ((b1) >> 2 ^ 0xc0);
            byte val2 =
                ((b2 & SIGN) == 0) ? (byte) (b2 >> 4) : (byte) ((b2) >> 4 ^ 0xf0);

            encodedData[encodedIndex] = lookUpBase64Alphabet[val1];
            encodedData[encodedIndex + 1] =
                lookUpBase64Alphabet[val2 | (k << 4)];
            encodedData[encodedIndex + 2] = lookUpBase64Alphabet[l << 2];
            encodedData[encodedIndex + 3] = PAD;
        }

        if (isChunked) {
            // we also add a separator to the end of the final chunk.
            if (chunksSoFar < nbrChunks) {
                System.arraycopy(
                    CHUNK_SEPARATOR,
                    0,
                    encodedData,
                    encodedDataLength - CHUNK_SEPARATOR.length,
                    CHUNK_SEPARATOR.length);
            }
        }

        return encodedData;
    }

    /**
     * Decodes Base64 data into octects
     *
     * @param base64Data Byte array containing Base64 data
     * @return Array containing decoded data.
     */
    public static byte[] decodeBase64(byte[] base64Data) {
        return decodeBase64(base64Data, base64Data.length);
    }
    public static byte[] decodeBase64(byte[] base64Data, int datalen) {
        
        {
        // RFC 2045 requires that we discard ALL non-Base64 characters
        // Also add padding as needed.
            if (datalen % FOURBYTE != 0) {
                base64Data = ensureBase64(base64Data, datalen);
                datalen = base64Data.length;
            } else {
                for (int i = 0; i < datalen; i++) {
                    if (!isBase64(base64Data[i])) {
                        base64Data = ensureBase64(base64Data, datalen);
                        datalen = base64Data.length;
                        break;
                    }
                }
            }
        }

        // handle the edge case, so we don't have to worry about it later
        if (datalen == 0) {
            return new byte[0];
        }

        int numberQuadruple = datalen / FOURBYTE;
        byte decodedData[] = null;
        byte b1 = 0, b2 = 0, b3 = 0, b4 = 0, marker0 = 0, marker1 = 0;

        // Throw away anything not in base64Data

        int encodedIndex = 0;
        int dataIndex = 0;
        {
            // this sizes the output array properly - rlw
            int lastData = datalen;
            // ignore the '=' padding
            while (base64Data[lastData - 1] == PAD) {
                if (--lastData == 0) {
                    return new byte[0];
                }
            }
            decodedData = new byte[lastData - numberQuadruple];
        }
        
        for (int i = 0; i < numberQuadruple; i++) {
            dataIndex = i * 4;
            marker0 = base64Data[dataIndex + 2];
            marker1 = base64Data[dataIndex + 3];
            
            b1 = base64Alphabet[base64Data[dataIndex]];
            b2 = base64Alphabet[base64Data[dataIndex + 1]];
            
            if (marker0 != PAD && marker1 != PAD) {
                //No PAD e.g 3cQl
                b3 = base64Alphabet[marker0];
                b4 = base64Alphabet[marker1];
                
                decodedData[encodedIndex] = (byte) (b1 << 2 | b2 >> 4);
                decodedData[encodedIndex + 1] =
                    (byte) (((b2 & 0xf) << 4) | ((b3 >> 2) & 0xf));
                decodedData[encodedIndex + 2] = (byte) (b3 << 6 | b4);
            } else if (marker0 == PAD) {
                //Two PAD e.g. 3c[Pad][Pad]
                decodedData[encodedIndex] = (byte) (b1 << 2 | b2 >> 4);
            } else if (marker1 == PAD) {
                //One PAD e.g. 3cQ[Pad]
                b3 = base64Alphabet[marker0];
                
                decodedData[encodedIndex] = (byte) (b1 << 2 | b2 >> 4);
                decodedData[encodedIndex + 1] =
                    (byte) (((b2 & 0xf) << 4) | ((b3 >> 2) & 0xf));
            }
            encodedIndex += 3;
        }
        return decodedData;
    }
    
    /**
     * Discards any whitespace from a base-64 encoded block.
     *
     * @param data The base-64 encoded data to discard the whitespace
     * from.
     * @return The data, less whitespace (see RFC 2045).
     */
    static byte[] discardWhitespace(byte[] data) {
        byte groomedData[] = new byte[data.length];
        int bytesCopied = 0;
        
        for (int i = 0; i < data.length; i++) {
            switch (data[i]) {
            case (byte) ' ' :
            case (byte) '\n' :
            case (byte) '\r' :
            case (byte) '\t' :
                    break;
            default:
                    groomedData[bytesCopied++] = data[i];
            }
        }

        byte packedData[] = new byte[bytesCopied];

        System.arraycopy(groomedData, 0, packedData, 0, bytesCopied);

        return packedData;
    }

    /**
     * Discards any characters outside of the base64 alphabet, per
     * the requirements on page 25 of RFC 2045 - "Any characters
     * outside of the base64 alphabet are to be ignored in base64
     * encoded data."
     * We are also adding padding if not 4 char aligned.
     *
     * @param data The base-64 encoded data to groom
     * @return The data, less non-base64 characters (see RFC 2045).
     */
    static byte[] ensureBase64(byte[] data) {
        return ensureBase64(data, data.length);
    }
    
    static byte[] ensureBase64(byte[] data, int datalen) {
        byte groomedData[] = new byte[datalen + FOURBYTE];
        int bytesCopied = 0;

        for (int i = 0; i < datalen; i++) {
            if (isBase64(data[i])) {
                groomedData[bytesCopied++] = data[i];
            }
        }
        
        int addpadding = FOURBYTE - bytesCopied % FOURBYTE;
        for (int i = 0; i < addpadding; i++) {
            // Add the padding to make 4 char aligned.
            groomedData[bytesCopied++] = PAD;
        }

        byte packedData[] = new byte[bytesCopied];

        System.arraycopy(groomedData, 0, packedData, 0, bytesCopied);

        return packedData;
    }


    // Implementation of the Encoder Interface

    /**
     * Encodes an Object using the base64 algorithm.  This method
     * is provided in order to satisfy the requirements of the
     * Encoder interface, and will throw an EncoderException if the
     * supplied object is not of type byte[].
     *
     * @param pObject Object to encode
     * @return An object (of type byte[]) containing the 
     *         base64 encoded data which corresponds to the byte[] supplied.
     * @throws EncoderException if the parameter supplied is not
     *                          of type byte[]
     */
    public Object encode(Object pObject) throws EncoderException {
        if (!(pObject instanceof byte[])) {
            throw new EncoderException(
                "Parameter supplied to Base64 encode is not a byte[]");
        }
        return encode((byte[]) pObject);
    }

    /**
     * Encodes a byte[] containing binary data, into a byte[] containing
     * characters in the Base64 alphabet.
     *
     * @param pArray a byte array containing binary data
     * @return A byte array containing only Base64 character data
     */
    public byte[] encode(byte[] pArray) {
        return encodeBase64(pArray, false);
    }
    
    
    
    /*
    
    private static void _ensure(String s) throws java.io.UnsupportedEncodingException
    {
        byte[] x = s.getBytes("UTF-8");
        byte[] enc = encodeBase64(x);
        int enclen = 0;
        for(int i = 0; i < enc.length; i++)
        {
            if(enc[i] == PAD)
            {
                break;
            }
            enclen++;
        }
        byte[] y = decodeBase64(enc, enclen);
        System.out.println("Base64 '" + new String(x, "UTF-8") + "' enc-> '" + new String(enc, "UTF-8") + "' trim-> '"
            + new String(enc, 0, enclen, "UTF-8") + "' dec-> '" + new String(y, "UTF-8") + "'");
        if(x.length != y.length)
        {
            throw new RuntimeException("Base64 length mismatch for " + s);
        }
        for(int i = 0; i < x.length; i++)
        {
            if(x[i] != y[i])
            {
                throw new RuntimeException("Base64 content mismatch for " + s);
            }
        }
    }
    
    
    public static void main(String[] args) throws Exception
    {
        _ensure("a");
        _ensure("ab");
        _ensure("abc");
        _ensure("abcd");
        _ensure("abcde");
        _ensure("abcdef");
        _ensure("abcdefg");
        _ensure("abcdefgh");
        _ensure("abcdefghi");
        _ensure("abcdefghij");
        _ensure("abcdefghijk");
        _ensure("abcdefghijkl");
        _ensure("abcdefghijklm");
        _ensure("abcdefghijklmn");
        _ensure("abcdefghijklmno");
        _ensure("abcdefghijklmnop");
        _ensure("abcdefghijklmnopq");
        _ensure("abcdefghijklmnopqr");
        _ensure("abcdefghijklmnopqrs");
        _ensure("abcdefghijklmnopqrst");
        _ensure("abcdefghijklmnopqrstu");
        _ensure("abcdefghijklmnopqrstuv");
        _ensure("abcdefghijklmnopqrstuvw");
        _ensure("abcdefghijklmnopqrstuvwx");
        _ensure("abcdefghijklmnopqrstuvwxy");
        _ensure("abcdefghijklmnopqrstuvwxyz");
        System.out.println("- Base64 tests passed - ");
    }
    
    */
    
    
    
}



