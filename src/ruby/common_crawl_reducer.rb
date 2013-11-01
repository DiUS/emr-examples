#!/usr/bin/env ruby

result = {}

ARGF.reduce({}) do |result, line|
	key, value = tokens = line.split("\t")
	result[key] ||= 0
	result[key] += value.to_i
	result
end

puts result.map { |key, value| "#{key}\t#{value}"}
