package controllers;

import play.*;
import play.data.Form;
import play.mvc.*;
import models.*;
import play.data.FormFactory;

import java.io.File;
import java.util.Scanner;
import java.util.TimeZone;

import javax.inject.*;

/**
 * This controller contains an action to process the input file
 */
public class LogsController extends Controller {
	@Inject 
	FormFactory formFactory;
	
    public Result showLog(){
    	Form<Logs> logForm = formFactory.form(Logs.class);
    	Logs l1 = new Logs();
    	File lfile = Play.application().getFile("/logs/application.log");
    	String logtext = "";
    	try {
    	 StringBuilder fileContents = new StringBuilder((int)lfile.length());
    	    Scanner scanner = new Scanner(lfile);
    	    String lineSeparator = System.getProperty("line.separator");

    	    try {
    	        while(scanner.hasNextLine()) {
    	            fileContents.append(scanner.nextLine() + lineSeparator);
    	        }
    	        logtext =  fileContents.toString();
    	    } finally {
    	        scanner.close();
    	    }
    	}
    	catch (Exception e)
    	{e.printStackTrace();}
    	l1.setLogtext(logtext);        
    	return ok(views.html.logs.render(logForm.fill(l1)));    
    }
    
}
