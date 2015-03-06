require 'rbbt-util'
require 'rbbt/resource'

$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '../../..', 'lib'))

module CADD
  extend Resource
  self.subdir = 'var/CADD'

  URL = "http://krishna.gs.washington.edu/download/CADD/v1.0/whole_genome_SNVs.tsv.gz"

  CADD.claim CADD.data[File.basename(URL)], :url, URL

  GM_SHARD_FUNCTION = Proc.new do |key|
    key[0..key.index(":")-1]
  end

  CHR_POS = Proc.new do |key|
    raise "Key (position) not String: #{ key }" unless String === key
    if match = key.match(/.*?:(\d+):?/)
      match[1].to_i
    else
      raise "Key (position) not understood: #{ key }"
    end
  end

  def self.database
    @@database ||= begin
                     Persist.persist_tsv("CADD", CADD.data[File.basename(URL)].find, {}, :persist => true,
                                         :file => CADD.scores_packed_shard.find,
                                         :prefix => "CADD", :pattern => %w(f f f f f f f f), :engine => "pki",
                                         :shard_function => GM_SHARD_FUNCTION, :pos_function => CHR_POS) do |sharder|

                       sharder.fields = %w(A C T G).collect{|b| ["raw score", "phread score"].collect{|s| [b,s] * " " } }.flatten
                       sharder.key_field = "Genomic Position"
                       sharder.type = :list

                       #file = CMD.cmd('gunzip', :in => File.open(CADD.data[File.basename(URL)].find), :pipe => true)
                       file = CADD.data[File.basename(URL)]
                       last_chr = nil
                       last_pos = nil
                       last_values = {}
                       TSV.traverse file, :type => :array, :into => sharder, :bar => "CADD" do |line|
                         next if line =~ /#/
                         chr, pos, ref, alt, raw, p = line.split "\t"
                         position = [chr, pos] * ":"
                         if last_pos and last_pos != position
                           result = last_values.values_at(*%w(A C T G)).collect{|v| v.nil? ? [-999,-999] : v }.flatten.collect{|v| v.to_f}
                           last_pos = position
                           last_values = {}
                           last_values[alt] = [raw, p]
                           [position, result]
                         else
                           last_pos = position
                           last_values[alt] = [raw, p]
                           next
                         end
                       end
                                         end
                   end
  end
end

if __FILE__ == $0
 iif CADD.database
end
