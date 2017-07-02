package mapreduce.pagerank;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;

public class Node {
	
	//PR值与数组的分隔符/t
		public static final char fieldSeparator = '\t';
	//
	private double pageRank = 1.0;
	
	//字符串中后面节点的列表数组
	private String[] adjacentNodeNames;
	
	public double getPageRank() {
		return pageRank;
	}

	public Node setPageRank(double pageRank) {
		this.pageRank = pageRank;
		return this;
	}

	public String[] getAdjacentNodeNames() {
		return adjacentNodeNames;
	}

	public Node setAdjacentNodeNames(String[] adjacentNodeNames) {
		this.adjacentNodeNames = adjacentNodeNames;
		return this;
	}
	
	public boolean containsAdjacentNodes() {
		return adjacentNodeNames != null && adjacentNodeNames.length > 0;
	}

	public static Node fromMR(String value ) throws IOException {
		String[] parts = StringUtils.splitPreserveAllTokens(value, fieldSeparator);
		if (parts.length < 1 ) {
			throw new IOException("Expected 1 or more parts, but received "+parts.length);
		}
		//
		Node node = new Node().setPageRank(Double.valueOf(parts[0]));
		if (parts.length >1) {
			node.setAdjacentNodeNames(Arrays.copyOfRange(parts, 1, parts.length));
		}
		return node;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(pageRank);
		
		if (getAdjacentNodeNames() != null) {
			sb.append(fieldSeparator).append(StringUtils.join(getAdjacentNodeNames(),fieldSeparator));
			
		}
		return sb.toString();
	}
	
	

}
