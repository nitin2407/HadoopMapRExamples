package com.cascadingexamples;

import java.util.Properties;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.operation.aggregator.Count;

public class WordCount {
	
	public static void main(String[] args){
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, WordCount.class);
		AppProps.setApplicationName(properties, "Word Count");
		AppProps.addApplicationTag(properties, "Cascading:Word Count");
		
		FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);
		
		Tap src = new Hfs(new TextDelimited(true,"\t"),args[0]); 
		
		Tap dest = new Hfs(new TextDelimited(true,"\t"),args[1]);
		
		Fields token = new Fields("token");
		Fields text = new Fields("text");
		
		RegexSplitGenerator splitter = new RegexSplitGenerator(token,"[ \\[\\]\\(\\),.]");
		
		
		Pipe docPipe = new Each("token",text,splitter,Fields.RESULTS);
		
		Pipe wcPipe = new Pipe("wc",docPipe);
		wcPipe = new GroupBy(wcPipe,token);
		wcPipe = new Every(wcPipe,Fields.ALL,new Count(),Fields.ALL);
		
		FlowDef flowDef = FlowDef.flowDef().setName("wc").addSource(docPipe, src).addTailSink(wcPipe, dest);
		
		Flow wcFlow = flowConnector.connect( flowDef );
	    wcFlow.writeDOT( "dot/wc.dot" );
	    wcFlow.complete();
	}

}
