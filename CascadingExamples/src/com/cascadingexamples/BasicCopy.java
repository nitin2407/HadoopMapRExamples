package com.cascadingexamples;

import java.util.Properties;

import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

public class BasicCopy {
	
	public static void main(String[] args){
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, BasicCopy.class);
		AppProps.setApplicationName(properties, "Basic Copy");
		AppProps.addApplicationTag(properties, "Cascading:First example");
		
		Hadoop2MR1FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);
		
		Tap src = new Hfs(new TextDelimited(true,"\t"),args[0]); 
		
		Tap dest = new Hfs(new TextDelimited(true,"\t"),args[1]);
		
		Pipe copyPipe = new Pipe("copy");
		
		FlowDef flowdef = FlowDef.flowDef().addSource(copyPipe, src).addTailSink(copyPipe, dest).setName("copy");
		
		flowConnector.connect(flowdef).complete();
	}
	
}
