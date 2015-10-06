/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.tools.graphanalytics.semiclustering;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

/**
 * SemiClusterTextReader defines the way in which data is to be read from the
 * input file and store as a vertex with VertexId and Edges
 * 
 */
public class SemiClusterTextReader extends
AdjacencyListTextVertexInputFormat<IntWritable, SemiClusterMessage, DoubleWritable> {
	/** Separator for id and value */
	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

	@Override
	public AdjacencyListTextVertexReader createVertexReader(
		InputSplit split, TaskAttemptContext context) {
		return new LabeledGraphReader();
	}

	public class  LabeledGraphReader extends AdjacencyListTextVertexReader {

		protected String[] preprocessLine(Text line) throws IOException {
			String[] values = SEPARATOR.split(line.toString());
			return values;
		}

		@Override
		protected IntWritable getId(String[] values) throws IOException {
			return decodeId(values[0]);
		}

		@Override
		protected SemiClusterMessage getValue(String[] values) throws IOException {
			return null; //vertex label;
		}

		@Override
		protected Iterable<Edge<IntWritable, DoubleWritable>> getEdges(String[] values) throws
		IOException {
			int i = 2;
			List<Edge<IntWritable, DoubleWritable>> outEdges = Lists.newLinkedList();
			while (i < values.length) {
				int target = Integer.valueOf(values[i]);
				int elabel = Integer.valueOf(values[i+1]);
				outEdges.add(EdgeFactory.create(new IntWritable(target), new DoubleWritable(elabel)));
				i += 2;
			}
			return outEdges;
		}

		@Override
		public IntWritable decodeId(String s) {
			return new IntWritable(Integer.valueOf(s));
		}

		@Override
		public SemiClusterMessage decodeValue(String s) {
			return null;
		}

		@Override
		public Edge<IntWritable, DoubleWritable> decodeEdge(String id,
			String value) {
			return null;
		}
	}
}
