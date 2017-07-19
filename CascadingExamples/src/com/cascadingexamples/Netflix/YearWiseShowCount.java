package com.cascadingexamples.Netflix;

import java.util.Properties;
import cascading.property.AppProps;
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
import com.tresata.cascading.opencsv.*;


public class YearWiseShowCount {
	
	public static void main(String[] args){
		
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, YearWiseShowCount.class);
		AppProps.setApplicationName(properties, "Year wise show count");
		AppProps.addApplicationTag(properties, "Cascading:Netflix");
		
		FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);
		
		Tap src=new Hfs(new OpenCsvScheme(',','"','\\',false),args[0]);
		//Tap src = new Hfs(new TextDelimited(true,","),args[0]);
		Tap dest = new Hfs(new TextDelimited(true,","),args[1]);
		
		Fields year = new Fields("release year");
		Pipe yearPipe = new Pipe("year");
		yearPipe = new GroupBy(yearPipe,year);
		yearPipe = new Every(yearPipe,year,new Count(),Fields.ALL);
		
		FlowDef flowDef = FlowDef.flowDef().setName("show-count").addSource(yearPipe, src).addTailSink(yearPipe, dest);
		
		Flow flow = flowConnector.connect(flowDef);
		flow.complete();
		
		
	}
}
