#!/bin/ruby
require 'time'
require 'json'


def getinputFileNames(array,interval=600)
r=[]
array.each do |pattern|

	if pattern == "logs"
		r<<"../fakelogs/fake.log" 
	else
		r+=Dir[pattern.gsub(/%./,"*")].select{|i|
			Time.now.to_i-Time.strptime(i,pattern).to_i < interval rescue nil
		}
	end
	
end
r
end

if ARGV.length == 1
	input=File.open(ARGV[0])
else 
	input=STDIN
end

Timestamp=Time.now

config=JSON.load(input,nil,{:symbolize_names => true})

config[:modules].map{|i| i.to_sym}.each do |mod|

definition=config[:definitions][mod]


trigger = Dir[definition[:output].gsub(/%./,"*")].select{|i|
		      Time.now.to_i-Time.strptime(i,definition[:output]).to_i < definition[:interval] rescue nil
         }.count == 0
if trigger 

cmd="./"+definition[:executable]+" "+getinputFileNames(definition[:input]).join(" ")
processio=IO.popen(cmd)
puts mod.to_s+": "+cmd
output=processio.readlines

outfile=File.open(Timestamp.strftime(definition[:output]),"w+")
outfile.puts(output)
outfile.close
puts(output)
puts "-------------------------------"
processio.close
sleep(0.5)
end

end

