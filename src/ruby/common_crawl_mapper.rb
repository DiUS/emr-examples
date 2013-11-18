#!/usr/bin/env ruby

# Hadoop word count example - streaming mapper implementation
#
# We buffer up 25 lines of input before scanning for the target words,
# as it's slightly more efficient than scanning each line individually


patterns = {}
if ARGV.length > 0
	patterns = Hash[ARGV.shift.split(',').map { |word| [ word, Regexp.new(/#{word}/i)] }]
end

line_count = 0
line_count_max = 25
line_text = ''

ARGF.each do |line|
	if line_count < line_count_max
		line_text += line
		line_count += 1
	else
	  patterns.each do |word, pattern|
	    count = line_text.scan(pattern).count
	    if count > 0
	    	puts "#{word}\t#{count}"
	    end
	  end

	  line_count = 0
	  line_text = ''
	end
end
