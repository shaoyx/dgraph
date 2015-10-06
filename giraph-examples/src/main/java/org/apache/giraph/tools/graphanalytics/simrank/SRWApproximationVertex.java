package org.apache.giraph.tools.graphanalytics.simrank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.examples.Algorithm;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import org.apache.giraph.tools.utils.Random;

import com.google.common.collect.Lists;

@Algorithm(
		name = "Sampling Random Walks for calculating approximated SimRank",
		description = "compute s(u,v) based on Sampled Random Walk without first-meeting gaurantee "+
		"(Refer to Scalable Similarity Search for SimRank, SIGMOD 2014)."
)
public class SRWApproximationVertex 
extends Vertex<IntWritable, DoubleWritable, NullWritable, DoubleWritable>{

		private static Logger LOG = Logger.getLogger(SRWApproximationVertex.class);
		private static double decayFactor = 0.8;
		private int MAX_ITERATION_NUM = 10000000;
		
		private int RandomWalksNum = 1;
		
		private ArrayList<IntWritable> al = null;
		
		@Override
		public void compute(Iterable<DoubleWritable> messages)
				throws IOException {
			if(getSuperstep() > MAX_ITERATION_NUM){
				voteToHalt();
				return ;
			}
			
			if(getSuperstep() == 0){
				/* initialization */
				MAX_ITERATION_NUM = this.getConf().getInt("simrank.maxiter", 10);
				RandomWalksNum = this.getConf().getInt("simrank.samplenum", 100);
				
				if(getId().get() == this.getConf().getInt("simrank.src", 10)){
					for(int i = 0; i < RandomWalksNum; i++){
						if(this.getNumEdges() == 0){
							LOG.info("src vertex has no outgoing neighbors!");
							return ;
						}
						this.sendMessage(this.randomGetNeighbor(), new DoubleWritable(1));
					}
				}
				else if(getId().get() == this.getConf().getInt("simrank.dst", 100)){
					if(this.getNumEdges() == 0){
						LOG.info("dst vertex has no outgoing neighbors!");
						return ;
					}
					double prob = 1.0 / getNumEdges();
					for(int i = 0; i < RandomWalksNum; i++){
						this.sendMessage(this.randomGetNeighbor(), new DoubleWritable(-1));
					}
				}
			}
			else{
				int dstCnt = 0;
				int srcCnt = 0;
				for(DoubleWritable msg : messages){
					if(msg.get() > 0.0){
						srcCnt++;
					}
					else{
						dstCnt++;
					}
					if(this.getNumEdges() > 0)
						this.sendMessage(this.randomGetNeighbor(), new DoubleWritable(msg.get()));
				}
				
				double deltaSimRank = 1.0*dstCnt/RandomWalksNum*srcCnt/RandomWalksNum;
				long step = getSuperstep();
				for(int i = 1; i <= step; i++){
					deltaSimRank *= decayFactor;
				}
				
				setValue(new DoubleWritable(this.getValue().get()+deltaSimRank));
				
				aggregate("aggregate.tmppairsimrank", new DoubleWritable(deltaSimRank));
				aggregate("aggregate.pairsimrank", getValue());
			}
			voteToHalt();
		}
		
		
		private IntWritable randomGetNeighbor(){
			if(al == null){
				al = new ArrayList<IntWritable>();
				for(Edge<IntWritable, NullWritable> edge : this.getEdges()){
					al.add(new IntWritable(edge.getTargetVertexId().get()));
				}
				//LOG.info("alSize="+al.size() +" edgeSize="+this.getNumEdges());
				//for(int i = 0; i < al.size(); i++){
					//LOG.info("idx="+i+": vid="+al.get(i).get());
				//}
			}
			int randNum = Math.abs(Random.nextInt());
//			LOG.info("randnum="+randNum+" neighborsize="+getNumEdges());
			return al.get(randNum % this.getNumEdges());
		}

		/** Master compute which uses aggregators. */
		public static class AggregatorsMasterCompute extends
		      DefaultMasterCompute {
		    @Override
		    public void compute() {
				double simrank = ((DoubleWritable)getAggregatedValue("aggregate.pairsimrank")).get();
				double tmpsimrank = ((DoubleWritable)getAggregatedValue("aggregate.tmppairsimrank")).get();
				System.out.println("step= "+getSuperstep()+": simrank="+simrank +" tmpsimrank="+tmpsimrank);
		    }

		    @Override
		    public void initialize() throws InstantiationException,
		        IllegalAccessException {
		      registerAggregator("aggregate.pairsimrank", DoubleSumAggregator.class);
		      registerAggregator("aggregate.tmppairsimrank", DoubleSumAggregator.class);
		    }
		  }
		  
		/** Vertex InputFormat */
		public static class SRWApproximationVertexInputFormat extends
			AdjacencyListTextVertexInputFormat<IntWritable, DoubleWritable, NullWritable> {
				/** Separator for id and value */
				private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

				@Override
				public AdjacencyListTextVertexReader createVertexReader(
						InputSplit split, TaskAttemptContext context) {
					return new OrderedGraphReader();
				}

				public class  OrderedGraphReader extends AdjacencyListTextVertexReader {

					protected String[] preprocessLine(Text line) throws IOException {
						String[] values = SEPARATOR.split(line.toString());
						return values;
					}

					@Override
					protected IntWritable getId(String[] values) throws IOException {
						return decodeId(values[0]);
					}

					@Override
					protected DoubleWritable getValue(String[] values) throws IOException {
						return decodeValue(null);
					}

					@Override
					protected Iterable<Edge<IntWritable, NullWritable>> getEdges(String[] values) throws
					IOException {
						int i = 1;
						List<Edge<IntWritable, NullWritable>> edges = Lists.newLinkedList();
						while (i < values.length) {
							int target = Integer.valueOf(values[i]);
							edges.add(EdgeFactory.create(new IntWritable(target), NullWritable.get()));
							i++;
						}
						return edges;
					}

					@Override
					public IntWritable decodeId(String s) {
						return new IntWritable(Integer.valueOf(s));
					}

					@Override
					public DoubleWritable decodeValue(String s) {
						return new DoubleWritable(0);
					}

					@Override
					public Edge<IntWritable, NullWritable> decodeEdge(String id,
							String value) {
						return null;
					}
				}
			} 

		public static class SRWApproximationVertexOutputFormat extends
			TextVertexOutputFormat<IntWritable, DoubleWritable, NullWritable> {
				@Override
				public TextVertexWriter createVertexWriter(TaskAttemptContext context)
				throws IOException, InterruptedException {
				return new OrderedGraphWriter();
				}
				
				public class OrderedGraphWriter extends TextVertexWriter {
					@Override
					public void writeVertex(
							Vertex<IntWritable, DoubleWritable, NullWritable, ?> vertex)
					throws IOException, InterruptedException {
						getRecordWriter().write(
							new Text(vertex.getId().toString()),
							new Text(vertex.getValue().toString()));
					}
			}
		}
	}