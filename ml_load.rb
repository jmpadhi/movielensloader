#!/usr/bin/env ruby

require "rubygems"
require "mongo"
require "json"
require "yaml"
require "pp"

# ########################################################################## #
# Methods section
# ########################################################################## #
class MovieLenser
  include Mongo

  def initialize
    @client = MongoClient.new('localhost', 27017)
    @db     = @client['movielensdb']
    @ratings = {}
  end

  def read_from_file(file_path)
    fh = File.open(file_path)
    raise "Cannot open file '#{file_path}' for reading" unless fh
    fh.each do |line|
      #puts line
      fields = line.split(/\s+/)
      @ratings[fields[0]] ||= {}
      @ratings[fields[0]][fields[1]] = fields[2]
      #break if @total_count > 1000
    end
    puts @ratings.keys.size
  end

  def iter_and_save
    good_count = 0
    @ratings.keys.sort_by(&:to_i).each do |user_id|
      rating = @ratings[user_id]
      if rating.nil? then
        puts "Missing data for user '#{user_id}'"
      elsif rating.keys.size < 20 then
        puts "Only '#{rating.keys.size}' ratings for user '#{user_id}'"
      else
        good_count += 1
        user_rec = {"_id" => user_id, :ratings => []}
        rating_sum = 0
        rating_count = 0
        rating.keys.sort_by(&:to_i).each do |movie_id|
          rating_count += 1
          rating_sum += rating[movie_id].to_i
          rating_rec = {}
          rating_rec[:user_id] = user_id
          rating_rec[:movie_id] = movie_id 
          rating_rec[:rating] = rating[movie_id]
          @db["ratings"].insert(rating_rec)
          user_rec[:ratings] << {
            :movie_id => movie_id,
            :rating => rating[movie_id]
          }
        end
        user_rec[:num_ratings] = rating_count
        if rating_count > 0 then
          user_rec[:avg_rating] = (rating_sum.to_f/rating_count.to_f)
        end
        @db["users"].insert(user_rec)
      end
    end
    puts "Count : #{@db["ratings"].count}"
  end
end


# ########################################################################## #
# Main section
# ########################################################################## #
raise "Missing file name\nUsage : #{$0} <file name>" unless ARGV[0]

starting_ts = Time.now
movie_lenser = MovieLenser.new
movie_lenser.read_from_file(ARGV[0])
movie_lenser.iter_and_save
finished_ts = Time.now
puts "Elapsed Time : #{finished_ts - starting_ts} seconds"
