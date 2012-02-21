#!/usr/bin/env ruby

require 'rubygems'
require 'bunny'

b = Bunny.new :host => "localhost", :port => 5672, :user => "guest", :pass => "helpful!"
b.start
reindex = b.queue 'reindex', :durable => true
exchange = b.exchange 'publicearth', :type => :direct, :durable => true
reindex.bind exchange, :key => 'indexer'

ARGV.each do |id|
  puts "Posted #{id}"
  reindex.publish "#{id} 1257279827", :persistent => true
end
