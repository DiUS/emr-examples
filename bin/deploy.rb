#!/usr/bin/env ruby

$: << File.expand_path("../../lib/ruby", __FILE__)

require 'open3'
require 'dius/aws'

include Dius::Aws

unless ARGV.length == 1
	puts "Usage: deploy BUCKET_NAME"
	exit 1
end

stdin, stdout, stderr = Open3.popen3('gradle jar')
puts stdout.read
puts stderr.read

bucket = s3.buckets[ARGV[0]]

['bootstrap/ami-2.4', 'bootstrap/ami-hadoop-2', 'ruby'].each do |folder|
	files = Dir.glob(File.join(File.dirname(__FILE__), "../src/#{folder}/*"))
	files.each do |file|
	  bucket.objects[folder + "/" + File.basename(file)].write(Pathname.new(file), :acl => :public_read)
	end	
end

jar = File.join(File.dirname(__FILE__), "../build/libs/emr-examples.jar")
bucket.objects["java/emr-examples.jar"].write(Pathname.new(jar), :acl => :public_read)
