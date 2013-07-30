package de.tuda.p2p.tdf.worker.test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import de.tuda.p2p.tdf.worker.NumberFormatter;

public class NumberFormatterTest {

	@Test
	public void testNumberFormatter() {
		NumberFormatter numberFormatter = new NumberFormatter();
		assertThat(numberFormatter.toString(), is("NUMBERS=()"));

		numberFormatter.add(" 5\n");
		assertThat(numberFormatter.toString(), is("NUMBERS=(5)"));

		numberFormatter.clear();
		assertThat(numberFormatter.toString(), is("NUMBERS=()"));

		numberFormatter.add("42");
		numberFormatter.add(23L);
		assertThat(numberFormatter.toString(), is("NUMBERS=(42 23)"));
	}
}
