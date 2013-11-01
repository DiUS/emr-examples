#!/usr/bin/env ruby

words = []
if ARGV.length > 0
	words = ARGV.shift.split ','
end

ARGF.each do |line|
  words.each do |word|
    count = line.scan(/#{word}/i).count
    if count > 0
    	puts "#{word}\t#{count}"
    end
  end
end
