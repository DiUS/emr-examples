#!/usr/bin/env jruby

require 'java'
Dir.glob(File.join(File.dirname(__FILE__), '../../lib/java/hadoop-1.0.3/*.jar')).each { |jar| require jar }

java_import 'org.apache.hadoop.mapred.SequenceFileInputFormat'
java_import 'org.apache.hadoop.mapred.JobConf'
java_import 'org.apache.hadoop.mapred.Reporter'

conf = JobConf.new
conf.inputFormat = SequenceFileInputFormat
conf.setStrings("mapred.input.dir", "./sources/common-crawl")

input = SequenceFileInputFormat.new
input.getSplits(conf, 1).each do |split|
	reader = input.getRecordReader(split, conf, Reporter.NULL)
	key = reader.createKey
	value = reader.createValue

	while reader.next(key, value)
		puts key.toString + '\t' + value.toString + '\n'
	end	
end
