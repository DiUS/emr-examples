#!/usr/bin/env ruby

# Hadoop word count example - streaming reducer implementation

result = {}

ARGF.reduce({}) do |result, line|
	key, value = line.split("\t")
	result[key] ||= 0
	result[key] += value.to_i
	result
end

puts result.map { |key, value| "#{key}\t#{value}"}
