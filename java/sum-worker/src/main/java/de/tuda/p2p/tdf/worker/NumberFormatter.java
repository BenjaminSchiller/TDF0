package de.tuda.p2p.tdf.worker;

public class NumberFormatter {

	private StringBuilder sb = new StringBuilder("NUMBERS=(");

	public NumberFormatter() {
		clear();
	}

	/**
	 * Removes all added numbers
	 */
	public void clear() {
		if (sb.length() > 9) {
			sb = new StringBuilder("NUMBERS=(");
		}
	}

	/**
	 * Adds a number to the formatted string
	 * 
	 * @param number
	 *            The number to add
	 */
	public void add(Long number) {
		sb.append(number);
		sb.append(" ");
	}

	/**
	 * Adds a number to the formatted string. 
	 * The input is trimmed and parsed into a Long object.
	 * 
	 * @param number
	 *            The number to add
	 */
	public void add(String number) {
		this.add(Long.valueOf(number.trim()));
	}

	/**
	 * Returns the formatted number string
	 */
	public String toString() {
		return sb.toString().trim() + ")";
	}

}
