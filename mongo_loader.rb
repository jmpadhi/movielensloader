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

  attr_reader :db

  def initialize(file_path, host="localhost", port=27017)
    @client = MongoClient.new(host, port)
    @db     = @client['movielensdb']
    @ratings = {}
    fh = File.open(file_path)
    raise "Cannot open file '#{file_path}' for reading" unless fh
    fh.each do |line|
      #puts line
      fields = line.split(/\s+/)
      @ratings[fields[0]] ||= {}
      @ratings[fields[0]][fields[1]] = fields[2]
      #break if @total_count > 1000
    end
    puts "-- Read ratings for #{@ratings.keys.size} users"
  end

  def clean
    @db["users"].drop
    @db["movies"].drop
    @db["ratings"].drop
    #puts @db.collection_names
  end

  def iter_and_save
    good_count = 0
    @ratings.keys.sort_by(&:to_i).each do |user_id|
      user_ratings = @ratings[user_id]
      if user_ratings.nil? then
        puts "Missing data for user '#{user_id}'"
      elsif user_ratings.keys.size < 20 then
        puts "Only '#{user_ratings.keys.size}' ratings for user '#{user_id}'"
      else
        good_count += 1
        user_rec = {"_id" => user_id, :ratings => []}
        rating_sum = 0
        rating_count = 0
        user_ratings.keys.sort_by(&:to_i).each do |movie_id|
          # #### #
          # Save the movie record
          # #### #
          @db["movies"].update(
            {"_id" => movie_id},
            {"$push" => {"users" => user_id}},
            {:upsert => true}
          )
          # #### #
          # Save the ratings
          # #### #
          rating_count += 1
          rating_sum += user_ratings[movie_id].to_i
          rating_rec = {}
          rating_rec[:user_id] = user_id
          rating_rec[:movie_id] = movie_id 
          rating_rec[:rating] = user_ratings[movie_id]
          @db["ratings"].insert(rating_rec)
          # #### #
          # Save the user record
          # #### #
          user_rec[:ratings] << {
            :movie_id => movie_id,
            :rating => user_ratings[movie_id]
          }
        end
        user_rec[:num_ratings] = rating_count
        if rating_count > 0 then
          user_rec[:avg_rating] = (rating_sum.to_f/rating_count.to_f)
        end
        @db["users"].insert(user_rec)
        puts "#{Time.now} User #{user_id} done, #movies = #{rating_count}"
      end
    end
    puts "Count : #{@db["ratings"].count}"
  end
end


# ########################################################################## #
# Main section
# ########################################################################## #
usg_str = "Usage : #{$0} <u.data file path> <mongodb host> <mongodb port>"
raise "Missing file name\n#{usg_str}" unless ARGV[0]

starting_ts = Time.now
movie_lenser = MovieLenser.new(ARGV[0], ARGV[1], ARGV[2])
movie_lenser.clean
movie_lenser.iter_and_save
finished_ts = Time.now
puts "Elapsed Time : #{finished_ts - starting_ts} seconds"
