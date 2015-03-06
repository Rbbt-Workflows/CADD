require 'rbbt-util'
require 'rbbt/workflow'

require 'rbbt/sources/CADD'

module CADD
  extend Workflow

  class << self
    attr_accessor :organism
  end

  self.organism = "Hsa/jan2013"

  input :mutations, :array, "Genomic Mutation"
  task :annotate => :tsv do |mutations|
    database = CADD.database
    database.unnamed = true
    dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => database.fields, :type => :list, :cast => :to_f
    dumper.init
    TSV.traverse mutations, :into => dumper, :bar => true, :type => :array do |mutation|
      p = database[mutation]
      next if p.nil?
      [mutation, p]
    end
  end
end
