package net.haibo.loganalyser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
    	String badline = "containing 405 bad lines";
    	Pattern badLinesPattern = Pattern.compile("\\b([1-9]\\d*)\\s+bad\\s+lines\\b");

    	Matcher matcher = badLinesPattern.matcher(badline);
    	assertTrue(matcher.find());

    	System.out.println(matcher.group(1));        	

    }
}
