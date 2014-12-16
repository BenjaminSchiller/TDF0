package de.tuda.p2p.tdf.cmd;

import java.io.FileNotFoundException;

import argo.jdom.JsonNode;
import de.tuda.p2p.tdf.common.InvalidDatabaseKey;
import de.tuda.p2p.tdf.common.NamespaceNotExistant;
import de.tuda.p2p.tdf.common.databaseObjects.Namespace;
import de.tuda.p2p.tdf.common.databaseObjects.Task;
import de.tuda.p2p.tdf.common.databaseObjects.TaskList;

/**
 * Unused at the moment!
 * @author jan
 *
 */
public class Show extends CMD {
	
	public static void main(String[] args){
		init();
		//getInput(args);
		if(args.length != 1) {
			System.err.println("Invalid number of parameters!");
			showHelp(args);
			System.exit(1);
		}
		
		try {
			System.out.println(dbFactory.showDatabaseEntry(args[0]));
		} catch (InvalidDatabaseKey e) {
			System.err.println("Wrong database key/not existant!");
			showHelp(args);
			System.exit(1);
		} catch (NamespaceNotExistant e) {
			System.err.println("Namespace not existant!");
			showHelp(args);
			System.exit(1);
		}
	}
	
	private static void showHelp(String[] args) {
		System.out.println("usage:");
		System.out.println("$ ./Show databaseKey");
	}
}
