package de.tuda.p2p.tdf.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

/**
 * Threaded class for capturing multiple streams at once, e.g. STDOUT and STDERR
 */
public class StreamGobbler extends Thread {

	private InputStream is;
	private File file;

	public StreamGobbler(InputStream is, File file) {
		this.is = is;
		this.file = file;
	}

	public void run() {
		OutputStream out = null;
		String line = null;

		try {
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);

			out = FileUtils.openOutputStream(file, true);
			while ((line = br.readLine()) != null) {
				IOUtils.write(line, out);
				IOUtils.write(IOUtils.LINE_SEPARATOR, out);
			}
			out.close();
		} catch (IOException e) {
			Client.logError("Error writing worker log/error to file: " + e.getMessage());
		} finally {
			IOUtils.closeQuietly(out);
		}
	}

}
